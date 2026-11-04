#!/usr/bin/env python3
"""Verificação do Kafka de desenvolvimento através do MCP (stdio).

Ferramenta de apoio ao desenvolvimento: valida o servidor MCP e a configuração
do tópico (1 partição, cleanup.policy=delete, retenção) sem entrar no caminho
medido do protocolo.

Uso:
    python3 tp2/scripts/check_mcp.py [--topic r-events-smoke]

Requer `kafka-mcp-server` no PATH ou em ~/.local/bin, e o Kafka de dev no ar.
"""

from __future__ import annotations

import argparse
import json
import queue
import shutil
import subprocess
import sys
import threading
import time
from pathlib import Path


def find_binary(explicit: str | None) -> str:
    if explicit:
        return explicit
    found = shutil.which("kafka-mcp-server")
    if found:
        return found
    fallback = Path.home() / ".local" / "bin" / "kafka-mcp-server"
    if fallback.exists():
        return str(fallback)
    raise FileNotFoundError(
        "kafka-mcp-server não encontrado; instale o binário ou use --binary",
    )


class McpClient:
    def __init__(self, binary: str) -> None:
        self.proc = subprocess.Popen(
            [binary],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            bufsize=1,
        )
        self.lines: queue.Queue = queue.Queue()
        self.next_id = 1
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self) -> None:
        assert self.proc.stdout is not None
        for line in self.proc.stdout:
            line = line.strip()
            if line:
                self.lines.put(line)

    def _send(self, payload: dict) -> None:
        assert self.proc.stdin is not None
        self.proc.stdin.write(json.dumps(payload) + "\n")
        self.proc.stdin.flush()

    def request(self, method: str, params: dict, timeout: float = 20.0) -> dict:
        request_id = self.next_id
        self.next_id += 1
        self._send({
            "jsonrpc": "2.0", "id": request_id, "method": method, "params": params,
        })
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                line = self.lines.get(timeout=0.2)
            except queue.Empty:
                continue
            message = json.loads(line)
            if message.get("id") == request_id:
                return message
        raise TimeoutError(f"sem resposta para {method}")

    def notify(self, method: str, params: dict) -> None:
        self._send({"jsonrpc": "2.0", "method": method, "params": params})

    def call(self, tool: str, arguments: dict, timeout: float = 30.0) -> dict:
        return self.request(
            "tools/call", {"name": tool, "arguments": arguments}, timeout,
        )

    def close(self) -> None:
        try:
            self.proc.terminate()
            self.proc.wait(timeout=5)
        except Exception:  # noqa: BLE001 - encerramento defensivo
            self.proc.kill()


def text_of(response: dict) -> str:
    blocks = response.get("result", {}).get("content", [])
    return "\n".join(
        block.get("text", "") for block in blocks if block.get("type") == "text"
    )


def main(argv: list | None = None) -> int:
    parser = argparse.ArgumentParser(description="Verificação MCP do Kafka dev")
    parser.add_argument("--binary", default=None)
    parser.add_argument("--topic", default="r-events-smoke")
    args = parser.parse_args(argv)

    client = McpClient(find_binary(args.binary))
    checks = []
    try:
        init = client.request("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "tp2-check-mcp", "version": "0.1"},
        })
        client.notify("notifications/initialized", {})
        tools = client.request("tools/list", {})
        tool_names = [tool["name"] for tool in tools.get("result", {}).get("tools", [])]
        checks.append({
            "check": "initialize",
            "ok": "result" in init,
            "detail": init.get("result", {}).get("serverInfo"),
        })
        checks.append({
            "check": "tools/list",
            "ok": len(tool_names) >= 9,
            "detail": tool_names,
        })

        calls = [
            ("list_brokers", {}),
            ("cluster_overview", {}),
            ("list_topics", {}),
            ("describe_topic", {"topic_name": args.topic}),
            ("describe_configs", {
                "resource_type": "topic",
                "resource_name": args.topic,
                "config_keys": ["retention.ms", "cleanup.policy", "min.insync.replicas"],
            }),
        ]
        for tool, arguments in calls:
            response = client.call(tool, arguments)
            text = text_of(response)
            checks.append({
                "check": tool,
                "ok": not response.get("result", {}).get("isError", False) and bool(text),
                "detail": text[:800],
            })

        produced = client.call("produce_message", {
            "topic": args.topic,
            "key": "R",
            "value": json.dumps({
                "type": "ACQUIRE",
                "resource": "R",
                "process_id": "mcp-check",
                "request_id": f"mcp-check-{int(time.time())}",
            }),
        })
        checks.append({
            "check": "produce_message",
            "ok": "successfully" in text_of(produced),
            "detail": text_of(produced)[:200],
        })

        consumed = client.call("consume_messages", {
            "topics": [args.topic],
            "max_messages": 5,
        })
        checks.append({
            "check": "consume_messages",
            "ok": not consumed.get("result", {}).get("isError", False),
            "detail": text_of(consumed)[:800],
        })
    finally:
        client.close()

    ok = all(check["ok"] for check in checks)
    print(json.dumps({"ok": ok, "checks": checks}, indent=2, ensure_ascii=False))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
