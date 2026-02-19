#!/usr/bin/env python3
"""
Test script for MonsterMQ GraphQL System Logs Subscription

This script connects to the MonsterMQ GraphQL WebSocket endpoint and subscribes
to system logs with configurable filters.

Requirements:
    pip install websockets

Usage:
    # Monitor all logs
    python test_graphql_system_logs.py
    
    # Monitor only warnings and errors
    python test_graphql_system_logs.py --level WARNING
    
    # Monitor specific logger with regex
    python test_graphql_system_logs.py --logger ".*Session.*"
    
    # Monitor with message filter
    python test_graphql_system_logs.py --message ".*connect.*"
    
    # Combine multiple filters
    python test_graphql_system_logs.py --level SEVERE --logger ".*GraphQL.*"
"""

import asyncio
import json
import sys
import argparse
import websockets
import os
import pytest
from datetime import datetime

# Configuration from environment variables with defaults
GRAPHQL_WS_URL = os.getenv("GRAPHQL_WS_URL", "ws://localhost:4000/graphqlws")

class GraphQLSystemLogsClient:
    """Client for subscribing to MonsterMQ system logs via GraphQL WebSocket"""

    def __init__(self, url=None):
        url = url or GRAPHQL_WS_URL
        self.url = url
        self.websocket = None
        self.subscription_id = "1"
        
    async def connect(self):
        """Establish WebSocket connection"""
        print(f"Connecting to {self.url}...")
        self.websocket = await websockets.connect(
            self.url,
            subprotocols=["graphql-transport-ws"]
        )
        print("✓ Connected to GraphQL WebSocket endpoint")
        
    async def initialize(self):
        """Initialize the GraphQL WebSocket connection"""
        # Send connection_init message (graphql-ws protocol)
        init_message = {
            "type": "connection_init",
            "payload": {}
        }
        await self.websocket.send(json.dumps(init_message))
        
        # Wait for connection_ack
        response = await self.websocket.recv()
        msg = json.loads(response)
        
        if msg.get("type") == "connection_ack":
            print("✓ Connection acknowledged by server")
        else:
            print(f"⚠ Unexpected response: {msg}")
            
    async def subscribe(self, node="+", level="+", logger=None, thread=None,
                       source_class=None, source_method=None, message=None):
        """
        Subscribe to system logs with filters
        
        Args:
            node: Node filter (default "+" for all)
            level: Level filter (INFO, WARNING, SEVERE, or "+" for all)
            logger: Logger name filter (supports regex)
            thread: Thread ID filter (exact match)
            source_class: Source class filter (supports regex)
            source_method: Source method filter (supports regex)
            message: Message content filter (supports regex)
        """
        
        # Build GraphQL subscription query
        query = """
        subscription SystemLogs(
            $node: String,
            $level: String,
            $logger: String,
            $thread: Long,
            $sourceClass: String,
            $sourceMethod: String,
            $message: String
        ) {
            systemLogs(
                node: $node,
                level: $level,
                logger: $logger,
                thread: $thread,
                sourceClass: $sourceClass,
                sourceMethod: $sourceMethod,
                message: $message
            ) {
                timestamp
                level
                logger
                message
                thread
                node
                sourceClass
                sourceMethod
                parameters
                exception {
                    class
                    message
                    stackTrace
                }
            }
        }
        """
        
        # Build variables object (only include non-None values)
        variables = {}
        if node != "+":
            variables["node"] = node
        if level != "+":
            variables["level"] = level
        if logger:
            variables["logger"] = logger
        if thread:
            variables["thread"] = thread
        if source_class:
            variables["sourceClass"] = source_class
        if source_method:
            variables["sourceMethod"] = source_method
        if message:
            variables["message"] = message
            
        # Send subscribe message
        subscribe_message = {
            "id": self.subscription_id,
            "type": "subscribe",
            "payload": {
                "query": query,
                "variables": variables
            }
        }
        
        await self.websocket.send(json.dumps(subscribe_message))
        
        # Print filter info
        print("\n" + "="*80)
        print("📡 Subscription started with filters:")
        print(f"   Node:          {node}")
        print(f"   Level:         {level}")
        if logger:
            print(f"   Logger:        {logger}")
        if thread:
            print(f"   Thread:        {thread}")
        if source_class:
            print(f"   Source Class:  {source_class}")
        if source_method:
            print(f"   Source Method: {source_method}")
        if message:
            print(f"   Message:       {message}")
        print("="*80 + "\n")
        print("Waiting for logs... (Press Ctrl+C to stop)\n")
        
    async def listen(self):
        """Listen for incoming log messages"""
        try:
            async for message in self.websocket:
                msg = json.loads(message)
                
                if msg.get("type") == "next":
                    # Extract log entry from the message
                    log_entry = msg.get("payload", {}).get("data", {}).get("systemLogs")
                    if log_entry:
                        self.print_log_entry(log_entry)
                        
                elif msg.get("type") == "error":
                    print(f"\n❌ Error: {msg.get('payload')}\n")
                    
                elif msg.get("type") == "complete":
                    print("\n✓ Subscription completed\n")
                    break
                    
        except websockets.exceptions.ConnectionClosed:
            print("\n⚠ Connection closed by server\n")
        except KeyboardInterrupt:
            print("\n\n⏹ Stopping subscription...\n")
            await self.unsubscribe()
            
    def print_log_entry(self, log):
        """Pretty print a log entry"""
        # Color codes for different log levels
        colors = {
            "INFO": "\033[32m",      # Green
            "WARNING": "\033[33m",   # Yellow
            "SEVERE": "\033[31m",    # Red
        }
        reset = "\033[0m"
        
        level = log.get("level", "UNKNOWN")
        color = colors.get(level, "")
        
        # Format timestamp
        timestamp = log.get("timestamp", "")
        try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            time_str = dt.strftime("%H:%M:%S.%f")[:-3]
        except:
            time_str = timestamp
            
        # Build log line
        node = log.get("node", "")
        logger = log.get("logger", "")
        message = log.get("message", "")
        thread = log.get("thread", "")
        
        print(f"{color}[{time_str}] [{level:8s}]{reset} [{node}] [{logger}] {message}")
        
        # Print source info if available
        source_class = log.get("sourceClass")
        source_method = log.get("sourceMethod")
        if source_class or source_method:
            print(f"  └─ Source: {source_class or '?'}.{source_method or '?'}()")
            
        # Print parameters if available
        parameters = log.get("parameters")
        if parameters:
            print(f"  └─ Parameters: {parameters}")
            
        # Print exception if available
        exception = log.get("exception")
        if exception:
            exc_class = exception.get("class", "")
            exc_message = exception.get("message", "")
            exc_trace = exception.get("stackTrace", "")
            
            print(f"\n  {color}╔══ Exception: {exc_class}{reset}")
            if exc_message:
                print(f"  {color}║{reset} Message: {exc_message}")
            if exc_trace:
                print(f"  {color}║{reset} Stack Trace:")
                for line in exc_trace.split('\n')[:10]:  # Show first 10 lines
                    if line.strip():
                        print(f"  {color}║{reset}   {line}")
            print(f"  {color}╚{'═'*70}{reset}")
            
        print()  # Empty line between log entries
        
    async def unsubscribe(self):
        """Unsubscribe from system logs"""
        if self.websocket:
            complete_message = {
                "id": self.subscription_id,
                "type": "complete"
            }
            await self.websocket.send(json.dumps(complete_message))
            await self.websocket.close()
            print("✓ Disconnected\n")


async def main():
    """Main function"""
    parser = argparse.ArgumentParser(
        description="Subscribe to MonsterMQ system logs via GraphQL",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Monitor all logs
  %(prog)s
  
  # Monitor only warnings and errors
  %(prog)s --level WARNING
  
  # Monitor specific logger with regex
  %(prog)s --logger ".*Session.*"
  
  # Monitor connection events
  %(prog)s --message ".*connect.*"
  
  # Monitor severe errors from GraphQL components
  %(prog)s --level SEVERE --logger ".*GraphQL.*"
  
  # Monitor specific node
  %(prog)s --node broker-node-1
        """
    )
    
    parser.add_argument(
        "--url",
        default=GRAPHQL_WS_URL,
        help=f"GraphQL WebSocket URL (default: {GRAPHQL_WS_URL})"
    )
    parser.add_argument(
        "--node",
        default="+",
        help="Node filter (default: + for all)"
    )
    parser.add_argument(
        "--level",
        default="+",
        choices=["+", "INFO", "WARNING", "SEVERE"],
        help="Log level filter (default: + for all)"
    )
    parser.add_argument(
        "--logger",
        help="Logger name filter (supports regex)"
    )
    parser.add_argument(
        "--thread",
        type=int,
        help="Thread ID filter (exact match)"
    )
    parser.add_argument(
        "--source-class",
        help="Source class filter (supports regex)"
    )
    parser.add_argument(
        "--source-method",
        help="Source method filter (supports regex)"
    )
    parser.add_argument(
        "--message",
        help="Message content filter (supports regex)"
    )
    
    args = parser.parse_args()
    
    print("\n" + "="*80)
    print("MonsterMQ GraphQL System Logs Subscription Client")
    print("="*80)
    
    client = GraphQLSystemLogsClient(args.url)
    
    try:
        await client.connect()
        await client.initialize()
        await client.subscribe(
            node=args.node,
            level=args.level,
            logger=args.logger,
            thread=args.thread,
            source_class=args.source_class,
            source_method=args.source_method,
            message=args.message
        )
        await client.listen()
        
    except websockets.exceptions.WebSocketException as e:
        print(f"\n❌ WebSocket error: {e}\n")
        print("Make sure the MonsterMQ broker is running with GraphQL enabled.")
        print("Expected endpoint: ws://localhost:4000/graphqlws\n")
        sys.exit(1)
        
    except Exception as e:
        print(f"\n❌ Error: {e}\n")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n⏹ Stopped by user\n")
        sys.exit(0)
