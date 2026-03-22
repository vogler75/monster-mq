"""
MonsterMQ External Agent Base Class

Connects to a MonsterMQ broker via MQTT and runs an LLM-powered agent
with tool calling (ReAct loop). Implements the A2A protocol for agent
discovery and inter-agent communication.

This is the Python equivalent of the internal AgentExecutor (Kotlin/LangChain4j).
"""

import json
import logging
import os
import signal
import threading
import time
import uuid
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

import paho.mqtt.client as mqtt
import yaml
from langchain_core.messages import HumanMessage
from langchain_core.tools import BaseTool
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.prebuilt import create_react_agent

logger = logging.getLogger("monster_agent")


def create_llm(provider: str, model: str | None, api_key: str | None, temperature: float):
    """Create a LangChain chat model for the given provider."""
    if provider == "gemini":
        from langchain_google_genai import ChatGoogleGenerativeAI
        key = api_key or os.environ.get("GEMINI_API_KEY")
        model_name = model or "gemini-2.0-flash"
        return ChatGoogleGenerativeAI(model=model_name, google_api_key=key, temperature=temperature)
    elif provider == "claude":
        from langchain_anthropic import ChatAnthropic
        key = api_key or os.environ.get("ANTHROPIC_API_KEY")
        model_name = model or "claude-sonnet-4-20250514"
        return ChatAnthropic(model=model_name, anthropic_api_key=key, temperature=temperature)
    elif provider == "openai":
        from langchain_openai import ChatOpenAI
        key = api_key or os.environ.get("OPENAI_API_KEY")
        model_name = model or "gpt-4o"
        return ChatOpenAI(model=model_name, openai_api_key=key, temperature=temperature)
    elif provider == "ollama":
        from langchain_ollama import ChatOllama
        base_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
        model_name = model or "llama3"
        return ChatOllama(model=model_name, base_url=base_url, temperature=temperature)
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

        self._client_id = f"agent-{self.name}"
        self._mqtt_client: mqtt.Client | None = None
        self._agent_graph = None
        self._memory = InMemorySaver()
        self._running = False
        self._stop_event = threading.Event()

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

    # --- Abstract method ---

    @abstractmethod
    def get_tools(self) -> list[BaseTool]:
        """Return the list of LangChain tools available to this agent."""
        ...

    # --- MQTT built-in tools (available to all agents) ---

    def _create_mqtt_tools(self) -> list[BaseTool]:
        """Create built-in MQTT tools that mirror the internal agent's AgentTools."""
        from langchain_core.tools import tool

        mqtt_client = self._mqtt_client

        @tool
        def publish_message(topic: str, payload: str) -> str:
            """Publish a message to an MQTT topic on the MonsterMQ broker.

            Args:
                topic: The MQTT topic to publish to.
                payload: The message payload (string or JSON).
            """
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
            topic = f"{self._a2a_agent_prefix()}/memory/{key}"
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
            # Subscribe to A2A inbox
            client.subscribe(self._a2a_inbox_topic(), qos=1)
            logger.info(f"Subscribed to inbox: {self._a2a_inbox_topic()}")
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

            if topic == self._a2a_inbox_topic():
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
        response = self._execute(user_message)
        if response:
            time.sleep(10)
            self._publish_response(response)

    def _handle_task_message(self, topic: str, payload: str):
        """Handle an A2A task message (agent-to-agent invocation)."""
        try:
            task = json.loads(payload)
        except json.JSONDecodeError:
            # Plain text task
            task_id = str(uuid.uuid4())
            self._publish_task_status(task_id, "working")
            response = self._execute(payload)
            status = "completed" if response else "failed"
            self._publish_task_status(task_id, status)
            if response:
                self._publish_response(response)
            return

        task_id = task.get("taskId", str(uuid.uuid4()))
        input_text = task.get("input")
        reply_to = task.get("replyTo", self._a2a_status_topic(task_id))
        skill = task.get("skill")
        caller = task.get("callerAgent", "unknown")

        if not input_text:
            logger.warning(f"Task {task_id} missing 'input' field")
            return

        logger.info(f"Received task {task_id} from {caller}")
        self._publish_task_status(task_id, "working")

        prompt = f"[Task from agent '{caller}', taskId={task_id}"
        if skill:
            prompt += f", skill={skill}"
        prompt += f"]\n{input_text}"

        response = self._execute(prompt)

        # Publish result to reply topic
        result = {
            "taskId": task_id,
            "status": "completed" if response else "failed",
        }
        if response:
            result["result"] = response
        else:
            result["error"] = "Agent execution failed"

        if self._mqtt_client and self._mqtt_client.is_connected():
            self._mqtt_client.publish(reply_to, json.dumps(result), qos=1)

        self._publish_task_status(task_id, result["status"])

    # --- LLM execution ---

    def _execute(self, user_message: str) -> str | None:
        """Run the LLM agent with the given user message."""
        if not self._agent_graph:
            logger.error("Agent graph not initialized")
            return None

        self.messages_processed += 1
        try:
            logger.info(f"Executing agent with: {user_message[:200]}...")
            self.llm_calls += 1
            result = self._agent_graph.invoke(
                {"messages": [HumanMessage(content=user_message)]},
                config={"configurable": {"thread_id": self._client_id}},
            )
            # Extract the last AI message
            messages = result.get("messages", [])
            for msg in reversed(messages):
                if hasattr(msg, "content") and msg.content and msg.type == "ai":
                    response = msg.content
                    logger.info(f"Agent response: {response[:200]}...")
                    return response
            return None
        except Exception as e:
            self.errors += 1
            logger.error(f"Agent execution error: {e}", exc_info=True)
            return None

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

    def _publish_task_status(self, task_id: str, status: str):
        """Publish task status update."""
        msg = {
            "taskId": task_id,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
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
        max_iterations = self._ai_config.get("max_tool_iterations", 10)

        logger.info(f"Creating LLM: provider={provider}, model={model or 'default'}")
        llm = create_llm(provider, model, api_key, temperature)

        # Collect tools: agent-specific + built-in MQTT tools
        tools = self.get_tools() + self._create_mqtt_tools()
        tool_names = [t.name for t in tools]
        logger.info(f"Registered tools: {tool_names}")

        # Create ReAct agent graph with memory (chat history) and tool calling
        self._agent_graph = create_react_agent(
            model=llm,
            tools=tools,
            prompt=self.system_prompt,
            checkpointer=self._memory,
        )

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
            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()
            logger.info("Agent stopped.")
