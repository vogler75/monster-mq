#!/usr/bin/env python3
"""
System Monitor Agent - MonsterMQ External Agent Example

An example agent that monitors local system resources (CPU, memory, disk,
network, processes) and answers questions about system health via MQTT.

Usage:
    python system_monitor.py [config.yaml]

Send a request via MQTT:
    mosquitto_pub -t "agents/system-monitor/request" -m "What's the current CPU and memory usage?"

Read the response:
    mosquitto_sub -t "agents/system-monitor/response"
"""

import json
import platform
import sys

import psutil
from langchain_core.tools import tool

from monster_agent import MonsterAgent


# --- Tools: local system monitoring ---

@tool
def get_cpu_usage() -> str:
    """Get current CPU usage percentage and per-core breakdown."""
    overall = psutil.cpu_percent(interval=1)
    per_core = psutil.cpu_percent(interval=0, percpu=True)
    freq = psutil.cpu_freq()
    count = psutil.cpu_count(logical=True)
    physical = psutil.cpu_count(logical=False)

    result = {
        "overall_percent": overall,
        "per_core_percent": per_core,
        "cores_physical": physical,
        "cores_logical": count,
        "frequency_mhz": {
            "current": round(freq.current, 0) if freq else None,
            "max": round(freq.max, 0) if freq and freq.max else None,
        },
    }
    return json.dumps(result, indent=2)


@tool
def get_memory_usage() -> str:
    """Get current RAM and swap memory usage."""
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()

    result = {
        "ram": {
            "total_gb": round(mem.total / (1024**3), 2),
            "used_gb": round(mem.used / (1024**3), 2),
            "available_gb": round(mem.available / (1024**3), 2),
            "percent": mem.percent,
        },
        "swap": {
            "total_gb": round(swap.total / (1024**3), 2),
            "used_gb": round(swap.used / (1024**3), 2),
            "percent": swap.percent,
        },
    }
    return json.dumps(result, indent=2)


@tool
def get_disk_usage() -> str:
    """Get disk space usage for all mounted partitions."""
    partitions = psutil.disk_partitions()
    disks = []
    for part in partitions:
        try:
            usage = psutil.disk_usage(part.mountpoint)
            disks.append({
                "device": part.device,
                "mountpoint": part.mountpoint,
                "filesystem": part.fstype,
                "total_gb": round(usage.total / (1024**3), 2),
                "used_gb": round(usage.used / (1024**3), 2),
                "free_gb": round(usage.free / (1024**3), 2),
                "percent": usage.percent,
            })
        except PermissionError:
            continue
    return json.dumps(disks, indent=2)


@tool
def get_top_processes(sort_by: str = "cpu", limit: int = 10) -> str:
    """Get the top processes sorted by resource usage.

    Args:
        sort_by: Sort by 'cpu' (CPU percentage) or 'memory' (memory percentage).
        limit: Number of processes to return (default: 10).
    """
    procs = []
    for proc in psutil.process_iter(["pid", "name", "cpu_percent", "memory_percent", "status"]):
        try:
            info = proc.info
            procs.append({
                "pid": info["pid"],
                "name": info["name"],
                "cpu_percent": round(info["cpu_percent"] or 0, 1),
                "memory_percent": round(info["memory_percent"] or 0, 1),
                "status": info["status"],
            })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            continue

    key = "cpu_percent" if sort_by == "cpu" else "memory_percent"
    procs.sort(key=lambda p: p[key], reverse=True)
    return json.dumps(procs[:limit], indent=2)


@tool
def get_network_info() -> str:
    """Get network interface addresses and current I/O counters."""
    counters = psutil.net_io_counters()
    addrs = psutil.net_if_addrs()

    interfaces = {}
    for name, addr_list in addrs.items():
        interfaces[name] = [
            {"family": str(a.family), "address": a.address}
            for a in addr_list
            if a.address and not a.address.startswith("fe80")
        ]

    result = {
        "io_counters": {
            "bytes_sent_mb": round(counters.bytes_sent / (1024**2), 1),
            "bytes_recv_mb": round(counters.bytes_recv / (1024**2), 1),
            "packets_sent": counters.packets_sent,
            "packets_recv": counters.packets_recv,
        },
        "interfaces": interfaces,
    }
    return json.dumps(result, indent=2)


@tool
def get_system_info() -> str:
    """Get general system information (OS, hostname, uptime, Python version)."""
    import time
    boot = psutil.boot_time()
    uptime_seconds = int(time.time() - boot)
    days, rem = divmod(uptime_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, _ = divmod(rem, 60)

    result = {
        "hostname": platform.node(),
        "os": f"{platform.system()} {platform.release()}",
        "architecture": platform.machine(),
        "python_version": platform.python_version(),
        "uptime": f"{days}d {hours}h {minutes}m",
        "boot_time": psutil.boot_time(),
    }
    return json.dumps(result, indent=2)


class SystemMonitorAgent(MonsterAgent):
    """
    Example agent that monitors local system resources.

    Demonstrates how to build an external MonsterMQ agent with custom tools.
    The LLM can call any of the system monitoring tools to answer questions.
    """

    def get_tools(self):
        return [
            get_cpu_usage,
            get_memory_usage,
            get_disk_usage,
            get_top_processes,
            get_network_info,
            get_system_info,
        ]


if __name__ == "__main__":
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.yaml"
    agent = SystemMonitorAgent(config_path)
    agent.run()
