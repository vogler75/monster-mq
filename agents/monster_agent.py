"""
MonsterMQ External Agent Base Class

Connects to a MonsterMQ broker via MQTT and runs an LLM-powered agent
with tool calling (ReAct loop). Implements the A2A protocol for agent
discovery and inter-agent communication.

This is the Python equivalent of the internal AgentExecutor (Kotlin/LangChain4j).

Features:
  - Custom LangGraph graphs: override `build_graph()` to replace the default ReAct agent
  - Human-in-the-loop: publishes to sensitive topics pause the graph with `interrupt()` until an
    operator approves or rejects the request over MQTT (or the MonsterMQ dashboard)
  - Durable conversation state: in-memory, SQLite or Postgres LangGraph checkpointers
"""

import json
import logging
import os
import signal
import threading
import time
import uuid
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

import paho.mqtt.client as mqtt
import yaml
from langchain_core.messages import HumanMessage
from langchain_core.tools import BaseTool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.prebuilt import create_react_agent
from langgraph.types import Command, interrupt

logger = logging.getLogger("monster_agent")


def topic_matches(topic_filter: str, topic: str) -> bool:
    """MQTT topic filter matching with + and # wildcards."""
    filter_levels = topic_filter.split("/")
    topic_levels = topic.split("/")
    for i, level in enumerate(filter_levels):
        if level == "#":
            return True
        if i >= len(topic_levels):
            return False
        if level != "+" and level != topic_levels[i]:
            return False
    return len(filter_levels) == len(topic_levels)


def create_checkpointer(memory_config: dict):
    """Create the LangGraph checkpointer that stores conversation state (memory, sqlite or postgres)."""
    kind = (memory_config.get("checkpointer") or "memory").lower()
    if kind == "memory":
        return InMemorySaver()
    if kind == "sqlite":
        import sqlite3
        from langgraph.checkpoint.sqlite import SqliteSaver
        path = memory_config.get("sqlite_path", "agent-state.db")
        saver = SqliteSaver(sqlite3.connect(path, check_same_thread=False))
        saver.setup()
        logger.info(f"Using SQLite checkpointer: {path}")
        return saver
    if kind == "postgres":
        from psycopg import Connection
        from psycopg.rows import dict_row
        from langgraph.checkpoint.postgres import PostgresSaver
        url = memory_config.get("postgres_url") or os.environ.get("AGENT_POSTGRES_URL")
        if not url:
            raise ValueError("memory.postgres_url (or AGENT_POSTGRES_URL) is required for the postgres checkpointer")
        conn = Connection.connect(url, autocommit=True, prepare_threshold=0, row_factory=dict_row)
        saver = PostgresSaver(conn)
        saver.setup()
        logger.info("Using Postgres checkpointer")
        return saver
    raise ValueError(f"Unknown checkpointer: {kind}. Use: memory, sqlite, postgres")


@dataclass
class _Run:
    """One agent invocation, possibly paused by human-in-the-loop interrupts."""
    thread_id: str
    on_done: Callable[[str | None, str | None], None]
    task_id: str | None = None
    # interrupt key -> decision (None while waiting)
    decisions: dict[str, dict | None] = field(default_factory=dict)


def create_llm(provider: str, model: str | None, api_key: str | None, temperature: float):
    """Create a LangChain chat model for the given provider."""
    if not model or not model.strip():
        raise ValueError(f"No model configured for AI provider '{provider}'. A model must be specified.")
    should_set_temperature = temperature is not None and temperature > 0.0
    if provider == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI
        key = api_key or os.environ.get("GEMINI_API_KEY")
        kwargs = {"model": model, "google_api_key": key}
        if should_set_temperature:
            kwargs["temperature"] = temperature
        return ChatGoogleGenerativeAI(**kwargs)
    elif provider == "claude":
        from langchain_anthropic import ChatAnthropic
        key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        kwargs = {"model": model, "anthropic_api_key": key}
        if should_set_temperature:
            kwargs["temperature"] = temperature
        return ChatAnthropic(**kwargs)
    elif provider == "openai":
        from langchain_openai import ChatOpenAI
        key = api_key or os.environ.get("OPENAI_API_KEY")
        kwargs = {"model": model, "openai_api_key": key}
        if should_set_temperature:
            kwargs["temperature"] = temperature
        return ChatOpenAI(**kwargs)
    elif provider == "ollama":
        from langchain_ollama import ChatOllama
        base_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
        kwargs = {"model": model, "base_url": base_url}
        if should_set_temperature:
            kwargs["temperature"] = temperature
        return ChatOllama(**kwargs)
    else:
        raise ValueError(f"Unknown AI provider: {provider}. Use: gemini, claude, openai, ollama")


class MonsterAgent(ABC):
    """
    Base class for MonsterMQ external agents.

    Subclass this and implement `get_tools()` to create your own agent.
    The base class handles MQTT connection, A2A protocol, and the LLM ReAct loop.

    Example:
        class MyAgent(MonsterAgent):
            def get_tools(self) -> list[BaseTool]:
                return [my_tool_1, my_tool_2]

        agent = MyAgent("config.yaml")
        agent.run()
    """

    def __init__(self, config_path: str = "config.yaml"):
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self._mqtt_config = self.config.get("mqtt", {})
        self._agent_config = self.config.get("agent", {})
        self._ai_config = self.config.get("ai", {})
        self._trigger_config = self.config.get("trigger", {})

        self.name = self._agent_config.get("name", "external-agent")
        self.org = self._agent_config.get("org", "default")
        self.site = self._agent_config.get("site", "default")
        self.description = self._agent_config.get("description", "")
        self.version = self._agent_config.get("version", "1.0.0")

        self.input_topics: list[str] = self._trigger_config.get("input_topics", [])
        self.output_topics: list[str] = self._trigger_config.get("output_topics", [])
        self.system_prompt: str = self.config.get("system_prompt", "You are a helpful agent.")
        self.skills: list[dict] = self.config.get("skills", [])

        self._memory_config = self.config.get("memory", {})
        self._hitl_config = self.config.get("hitl", {})
        # Keep one conversation for topic-triggered runs (tasks use their sessionId or a fresh thread)
        self.state_enabled: bool = self._agent_config.get("state_enabled", True)
        self.max_tool_iterations: int = self._ai_config.get("max_tool_iterations", 10)
        self.sensitive_topics: list[str] = self._hitl_config.get("sensitive_topics", [])
        self.approval_timeout_seconds: float = self._hitl_config.get("approval_timeout_seconds", 300)

        self._client_id = f"agent-{self.name}"
        self._mqtt_client: mqtt.Client | None = None
        self._agent_graph = None
        self._memory = create_checkpointer(self._memory_config)
        self._running = False
        self._stop_event = threading.Event()
        # LLM runs are executed one at a time off the MQTT network thread, so the client keeps
        # receiving messages (e.g. approvals) while the agent is working
        self._worker = ThreadPoolExecutor(max_workers=1, thread_name_prefix=f"agent-{self.name}")
        # approval id -> (run, interrupt key, timeout timer)
        self._pending_approvals: dict[str, tuple[_Run, str, threading.Timer]] = {}
        self._approval_lock = threading.Lock()

        # Metrics
        self.messages_processed = 0
        self.llm_calls = 0
        self.errors = 0

    # --- A2A topic helpers (mirrors Kotlin AgentExecutor) ---

    def _a2a_prefix(self) -> str:
        return f"a2a/v1/{self.org}/{self.site}"

    def _a2a_agent_prefix(self) -> str:
        return f"{self._a2a_prefix()}/agents/{self.name}"

    def _a2a_discovery_topic(self) -> str:
        return f"{self._a2a_prefix()}/discovery/{self.name}"

    def _a2a_inbox_topic(self) -> str:
        return f"{self._a2a_agent_prefix()}/inbox"

    def _a2a_status_topic(self, task_id: str) -> str:
        return f"{self._a2a_agent_prefix()}/status/{task_id}"

    def _a2a_health_topic(self) -> str:
        return f"{self._a2a_agent_prefix()}/health"

    def _approval_request_topic(self, approval_id: str) -> str:
        return f"{self._a2a_agent_prefix()}/approval/request/{approval_id}"

    def _approval_response_topic(self, approval_id: str) -> str:
        return f"{self._a2a_agent_prefix()}/approval/response/{approval_id}"

    def _is_inbox_topic(self, topic: str) -> bool:
        inbox = self._a2a_inbox_topic()
        return topic == inbox or topic.startswith(inbox + "/")

    def requires_approval(self, topic: str) -> bool:
        """True if publishing to the topic needs operator approval (hitl.sensitive_topics)."""
        return any(topic_matches(f, topic) for f in self.sensitive_topics)

    # --- Abstract method ---

    @abstractmethod
    def get_tools(self) -> list[BaseTool]:
        """Return the list of LangChain tools available to this agent."""
        ...

    def build_graph(self, llm, tools: list[BaseTool], checkpointer):
        """Build the compiled LangGraph graph that runs the agent.

        Override to use a custom StateGraph. The graph is invoked with {"messages": [HumanMessage]}
        and must return a state with a "messages" list; the last AI message is the answer. Compile it
        with the given checkpointer so conversation memory and human-in-the-loop interrupts work.
        Tools (and nodes) may call `langgraph.types.interrupt()` to wait for an operator decision.
        """
        return create_react_agent(
            model=llm,
            tools=tools,
            prompt=self.system_prompt,
            checkpointer=checkpointer,
        )

    # --- MQTT built-in tools (available to all agents) ---

    def _create_mqtt_tools(self) -> list[BaseTool]:
        """Create built-in MQTT tools that mirror the internal agent's AgentTools."""
        from langchain_core.tools import tool

        agent = self

        @tool
        def publish_message(topic: str, payload: str) -> str:
            """Publish a message to an MQTT topic on the MonsterMQ broker.
            Publishing to sensitive topics waits for approval by an operator.

            Args:
                topic: The MQTT topic to publish to.
                payload: The message payload (string or JSON).
            """
            if agent.requires_approval(topic):
                # Pauses the graph until the operator decides; resumes here with the decision
                decision = interrupt({"action": "publish", "topic": topic, "payload": payload})
                if not (isinstance(decision, dict) and decision.get("approved")):
                    reason = decision.get("reason") if isinstance(decision, dict) else None
                    return f"Publish to {topic} was rejected by the operator" + (f": {reason}" if reason else ".")
            mqtt_client = agent._mqtt_client
            if mqtt_client and mqtt_client.is_connected():
                mqtt_client.publish(topic, payload, qos=1)
                return f"Published to {topic}"
            return "Error: MQTT not connected"

        @tool
        def save_note(key: str, content: str) -> str:
            """Save a persistent note as a retained MQTT message. Notes survive agent restarts.

            Args:
                key: The note key (e.g., 'daily-summary', 'threshold-config').
                content: The note content to save.
            """
            topic = f"{agent._a2a_agent_prefix()}/memory/{key}"
            mqtt_client = agent._mqtt_client
            if mqtt_client and mqtt_client.is_connected():
                mqtt_client.publish(topic, content, qos=1, retain=True)
                return f"Note saved: {key}"
            return "Error: MQTT not connected"

        return [publish_message, save_note]

    # --- MQTT connection ---

    def _on_connect(self, client: mqtt.Client, userdata: Any, flags: Any, reason_code: Any, properties: Any = None):
        if reason_code == 0 or (hasattr(reason_code, 'value') and reason_code.value == 0):
            logger.info(f"Connected to MonsterMQ broker")
            # Subscribe to input topics
            for topic in self.input_topics:
                client.subscribe(topic, qos=1)
                logger.info(f"Subscribed to: {topic}")
            # Subscribe to A2A inbox (broker agents send tasks to inbox/{taskId})
            client.subscribe(self._a2a_inbox_topic(), qos=1)
            client.subscribe(f"{self._a2a_inbox_topic()}/+", qos=1)
            logger.info(f"Subscribed to inbox: {self._a2a_inbox_topic()}[/+]")
            if self.sensitive_topics:
                client.subscribe(self._approval_response_topic("+"), qos=1)
                logger.info(f"Approval required for: {self.sensitive_topics}")
            # Publish agent card and health
            self._publish_agent_card()
            self._publish_health("ready")
        else:
            logger.error(f"Connection failed: {reason_code}")

    def _on_disconnect(self, client: mqtt.Client, userdata: Any, flags: Any = None, reason_code: Any = None, properties: Any = None):
        logger.warning(f"Disconnected from broker (rc={reason_code})")

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage):
        try:
            payload = msg.payload.decode("utf-8")
            topic = msg.topic
            logger.debug(f"Received on {topic}: {payload[:200]}")

            approval_prefix = self._approval_response_topic("")
            if topic.startswith(approval_prefix):
                self._handle_approval_response(topic[len(approval_prefix):], payload)
            elif self._is_inbox_topic(topic):
                self._handle_task_message(topic, payload)
            else:
                self._handle_mqtt_message(topic, payload)

        except Exception as e:
            self.errors += 1
            logger.error(f"Error processing message: {e}", exc_info=True)

    # --- Message handlers ---

    def _handle_mqtt_message(self, topic: str, payload: str):
        """Handle a normal MQTT message (input topic trigger)."""
        user_message = f"[Topic: {topic}] {payload}"
        thread_id = self._client_id if self.state_enabled else f"run:{uuid.uuid4()}"

        def done(response: str | None, error: str | None):
            if response:
                self._publish_response(response)

        self._execute(user_message, thread_id, done)

    def _handle_task_message(self, topic: str, payload: str):
        """Handle an A2A task message (agent-to-agent invocation)."""
        try:
            task = json.loads(payload)
        except json.JSONDecodeError:
            task = None

        if not isinstance(task, dict):
            # Plain text task
            task_id = str(uuid.uuid4())
            self._publish_task_status(task_id, "working")

            def done_plain(response: str | None, error: str | None):
                self._publish_task_status(task_id, "completed" if response else "failed")
                if response:
                    self._publish_response(response)

            self._execute(payload, f"task:{task_id}", done_plain, task_id)
            return

        # Replies to tasks this agent did not send (status without input) are not new tasks
        if "status" in task and "input" not in task:
            logger.debug(f"Ignoring reply payload on {topic}")
            return

        # Task id from the payload, else from inbox/{taskId}, else a new one
        topic_task_id = topic.rsplit("/", 1)[-1] if topic != self._a2a_inbox_topic() else None
        task_id = task.get("taskId") or topic_task_id or str(uuid.uuid4())
        input_value = task.get("input")
        input_text = input_value if isinstance(input_value, str) else (json.dumps(input_value) if input_value is not None else None)
        reply_to = task.get("replyTo", self._a2a_status_topic(task_id))
        skill = task.get("skill")
        caller = task.get("callerAgent", "unknown")
        parent_task_id = task.get("parentTaskId")
        session_id = task.get("sessionId")

        if not input_text:
            logger.warning(f"Task {task_id} missing 'input' field")
            return

        logger.info(f"Received task {task_id} from {caller}")
        self._publish_task_status(task_id, "working", parent_task_id)

        prompt = f"[Task from agent '{caller}', taskId={task_id}"
        if skill:
            prompt += f", skill={skill}"
        prompt += f"]\n{input_text}"

        def done(response: str | None, error: str | None):
            # Publish result to reply topic
            result = {
                "taskId": task_id,
                "status": "completed" if response else "failed",
                "agent": self.name,
            }
            if response:
                result["result"] = response
            else:
                result["error"] = error or "Agent execution failed"
            if self._mqtt_client and self._mqtt_client.is_connected():
                self._mqtt_client.publish(reply_to, json.dumps(result), qos=1)
            self._publish_task_status(task_id, result["status"], parent_task_id)

        # Tasks with the same sessionId share one conversation; others start fresh
        thread_id = f"session:{session_id}" if session_id else f"task:{task_id}"
        self._execute(prompt, thread_id, done, task_id)

    # --- LLM execution ---

    def _execute(self, user_message: str, thread_id: str,
                 on_done: Callable[[str | None, str | None], None], task_id: str | None = None):
        """Queue an agent run; on_done(response, error) is called when it finishes."""
        if not self._agent_graph:
            logger.error("Agent graph not initialized")
            on_done(None, "Agent graph not initialized")
            return
        self.messages_processed += 1
        run = _Run(thread_id=thread_id, on_done=on_done, task_id=task_id)
        logger.info(f"Executing agent with: {user_message[:200]}...")
        self._worker.submit(self._run_graph, run, {"messages": [HumanMessage(content=user_message)]})

    def _graph_config(self, thread_id: str) -> dict:
        return {
            "configurable": {"thread_id": thread_id},
            # Each tool round trip is two graph steps (model + tools)
            "recursion_limit": 2 * self.max_tool_iterations + 1,
        }

    def _run_graph(self, run: _Run, graph_input: Any):
        """Invoke (or resume) the graph on the worker thread."""
        config = self._graph_config(run.thread_id)
        try:
            self.llm_calls += 1
            result = self._agent_graph.invoke(graph_input, config=config)
            interrupts = [i for t in self._agent_graph.get_state(config).tasks for i in (t.interrupts or ())]
            if interrupts:
                self._request_approvals(run, interrupts)
                return
            response = self._extract_response(result)
            if response:
                logger.info(f"Agent response: {response[:200]}...")
            run.on_done(response, None if response else "Agent returned no answer")
        except Exception as e:
            self.errors += 1
            logger.error(f"Agent execution error: {e}", exc_info=True)
            try:
                run.on_done(None, str(e))
            except Exception:
                logger.error("Error in completion handler", exc_info=True)

    @staticmethod
    def _extract_response(result: Any) -> str | None:
        """Return the content of the last AI message."""
        messages = result.get("messages", []) if isinstance(result, dict) else []
        for msg in reversed(messages):
            if getattr(msg, "type", None) == "ai" and msg.content:
                content = msg.content
                if isinstance(content, list):  # content blocks
                    content = "".join(b.get("text", "") if isinstance(b, dict) else str(b) for b in content)
                return content or None
        return None

    # --- Human-in-the-loop approvals ---

    def _request_approvals(self, run: _Run, interrupts: list):
        """Publish an approval request for every pending interrupt of a paused run."""
        run.decisions = {}
        for intr in interrupts:
            key = getattr(intr, "id", None) or getattr(intr, "interrupt_id", None) or str(uuid.uuid4())
            run.decisions[key] = None
            approval_id = str(uuid.uuid4())
            timer = threading.Timer(self.approval_timeout_seconds, self._resolve_approval,
                                    args=(approval_id, False, "approval timed out", "timeout"))
            timer.daemon = True
            with self._approval_lock:
                self._pending_approvals[approval_id] = (run, key, timer)
            request = {
                "approvalId": approval_id,
                "agent": self.name,
                "taskId": run.task_id,
                "threadId": run.thread_id,
                "request": intr.value,
                "responseTopic": self._approval_response_topic(approval_id),
                "timeoutSeconds": self.approval_timeout_seconds,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            # Retained so the request stays visible (e.g. in the dashboard) until it is decided
            if self._mqtt_client:
                self._mqtt_client.publish(self._approval_request_topic(approval_id), json.dumps(request), qos=1, retain=True)
            timer.start()
            logger.info(f"Waiting for approval {approval_id}: {intr.value}")
        if run.task_id:
            self._publish_task_status(run.task_id, "input-required")

    def _handle_approval_response(self, approval_id: str, payload: str):
        """Approval response: {"approved": true|false, "reason": "...", "by": "..."} or plain approve/reject."""
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            data = payload.strip().lower()
        if isinstance(data, dict):
            approved, reason, by = bool(data.get("approved")), data.get("reason"), data.get("by")
        else:
            approved, reason, by = data in ("true", "yes", "approve", "approved", "ok"), None, None
        self._resolve_approval(approval_id, approved, reason, by)

    def _resolve_approval(self, approval_id: str, approved: bool, reason: str | None, by: str | None):
        with self._approval_lock:
            pending = self._pending_approvals.pop(approval_id, None)
        if not pending:
            return
        run, key, timer = pending
        timer.cancel()
        logger.info(f"Approval {approval_id} {'approved' if approved else 'rejected'}" + (f" ({reason})" if reason else ""))
        if self._mqtt_client:
            # Clear the retained request
            self._mqtt_client.publish(self._approval_request_topic(approval_id), b"", qos=1, retain=True)
        run.decisions[key] = {"approved": approved, "reason": reason, "by": by}
        if any(d is None for d in run.decisions.values()):
            return  # other interrupts of this run are still waiting
        decisions = run.decisions
        resume = next(iter(decisions.values())) if len(decisions) == 1 else decisions
        if run.task_id:
            self._publish_task_status(run.task_id, "working")
        self._worker.submit(self._run_graph, run, Command(resume=resume))

    # --- Publishing helpers ---

    def _publish_response(self, response: str):
        """Publish agent response to output topics."""
        if not self._mqtt_client or not self._mqtt_client.is_connected():
            return
        topics = self.output_topics or [f"agents/{self.name}/response"]
        for topic in topics:
            self._mqtt_client.publish(topic, response, qos=1)

    def _publish_agent_card(self):
        """Publish A2A agent card as a retained message for discovery."""
        card = {
            "protocolVersion": "1.0",
            "name": self.name,
            "description": self.description,
            "url": self._a2a_inbox_topic(),
            "preferredTransport": "MQTT",
            "version": self.version,
            "defaultInputModes": ["application/json", "text/plain"],
            "defaultOutputModes": ["application/json", "text/plain"],
            "provider": self._ai_config.get("provider", "gemini"),
            "model": self._ai_config.get("model", ""),
            "triggerType": "MQTT",
            "inputTopics": self.input_topics,
            "outputTopics": self.output_topics,
            "skills": [
                {"id": s["name"], "name": s["name"], "description": s.get("description", "")}
                for s in self.skills
            ],
            "runtime": "python",
            "status": "running",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        self._mqtt_client.publish(
            self._a2a_discovery_topic(),
            json.dumps(card),
            qos=1,
            retain=True,
        )
        logger.info(f"Published agent card to {self._a2a_discovery_topic()}")

    def _publish_health(self, status: str):
        """Publish health status as a retained message."""
        health = {
            "name": self.name,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "messagesProcessed": self.messages_processed,
            "llmCalls": self.llm_calls,
            "errors": self.errors,
        }
        if self._mqtt_client and self._mqtt_client.is_connected():
            self._mqtt_client.publish(
                self._a2a_health_topic(),
                json.dumps(health),
                qos=1,
                retain=True,
            )

    def _publish_task_status(self, task_id: str, status: str, parent_task_id: str | None = None):
        """Publish task status update."""
        msg = {
            "taskId": task_id,
            "status": status,
            "agent": self.name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if parent_task_id:
            msg["parentTaskId"] = parent_task_id
        if self._mqtt_client and self._mqtt_client.is_connected():
            self._mqtt_client.publish(
                self._a2a_status_topic(task_id),
                json.dumps(msg),
                qos=1,
            )

    # --- Lifecycle ---

    def _build_agent(self):
        """Build the LangGraph ReAct agent with tools and LLM."""
        provider = self._ai_config.get("provider", "gemini")
        model = self._ai_config.get("model") or None
        api_key = self._ai_config.get("api_key") or None
        temperature = self._ai_config.get("temperature", 0.7)

        logger.info(f"Creating LLM: provider={provider}, model={model or 'default'}")
        llm = create_llm(provider, model, api_key, temperature)

        # Collect tools: agent-specific + built-in MQTT tools
        tools = self.get_tools() + self._create_mqtt_tools()
        tool_names = [t.name for t in tools]
        logger.info(f"Registered tools: {tool_names}")

        # Create the agent graph (ReAct by default) with checkpointed memory and tool calling
        self._agent_graph = self.build_graph(llm, tools, self._memory)

    def _connect_mqtt(self):
        """Connect to the MonsterMQ broker via MQTT."""
        host = self._mqtt_config.get("host", "localhost")
        port = self._mqtt_config.get("port", 1883)
        username = self._mqtt_config.get("username", "")
        password = self._mqtt_config.get("password", "")

        self._mqtt_client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=self._client_id,
            protocol=mqtt.MQTTv311,
        )
        self._mqtt_client.on_connect = self._on_connect
        self._mqtt_client.on_disconnect = self._on_disconnect
        self._mqtt_client.on_message = self._on_message

        if username:
            self._mqtt_client.username_pw_set(username, password)

        # Set a will message so broker knows if we disconnect unexpectedly
        will_payload = json.dumps({
            "name": self.name,
            "status": "offline",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })
        self._mqtt_client.will_set(self._a2a_health_topic(), will_payload, qos=1, retain=True)

        logger.info(f"Connecting to {host}:{port} as {self._client_id}...")
        self._mqtt_client.connect(host, port, keepalive=60)

    def run(self):
        """Start the agent: connect to MQTT, build the LLM agent, and loop."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        logger.info(f"Starting MonsterMQ agent: {self.name}")

        # Build the LLM agent
        self._build_agent()

        # Connect to MQTT
        self._connect_mqtt()

        # Handle graceful shutdown
        def shutdown(signum, frame):
            logger.info("Shutting down...")
            self._running = False
            self._stop_event.set()

        signal.signal(signal.SIGINT, shutdown)
        signal.signal(signal.SIGTERM, shutdown)

        # Start MQTT loop
        self._running = True
        self._mqtt_client.loop_start()

        logger.info(f"Agent '{self.name}' is running. Press Ctrl+C to stop.")
        logger.info(f"  Input topics:  {self.input_topics}")
        logger.info(f"  Output topics: {self.output_topics}")
        logger.info(f"  A2A inbox:     {self._a2a_inbox_topic()}")

        try:
            self._stop_event.wait()
        finally:
            self._publish_health("stopped")
            time.sleep(0.5)  # allow health message to be sent
            self._worker.shutdown(wait=False, cancel_futures=True)
            with self._approval_lock:
                for _, _, timer in self._pending_approvals.values():
                    timer.cancel()
            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()
            logger.info("Agent stopped.")
