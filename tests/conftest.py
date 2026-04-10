"""Shared fixtures for bus tests."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from bus.db import BusDB
from bus.server import BusServer, create_app


@pytest.fixture
def db(tmp_path):
    return BusDB(str(tmp_path / "test-bus.db"))


@pytest.fixture
def bus(db):
    return BusServer(db, config={"bus_url": "http://localhost:9999"})


@pytest.fixture
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test-bus.db"))
    return TestClient(app)


def install_test_agent(client: TestClient, agent_id: str = "test-agent") -> dict:
    """Helper: install a test agent."""
    return client.post("/v1/install", json={
        "agent_type": "test",
        "id": agent_id,
        "command": "python",
        "args": ["-m", "test.agent"],
        "cwd": "/tmp",
        "config": "/tmp/config.yaml",
    }).json()


def register_test_agent(
    client: TestClient,
    agent_id: str = "test-agent",
    callback: str = "http://localhost:9999",
    skills: list[dict] | None = None,
    collaborations: list[dict] | None = None,
) -> dict:
    """Helper: register a test agent with skills."""
    return client.post("/v1/register", json={
        "id": agent_id,
        "callback": callback,
        "pid": 12345,
        "version": "0.1.0",
        "skills": skills or [
            {"name": "do_thing", "description": "does a thing", "parameters": {"x": {"type": "string"}}},
            {"name": "do_other", "description": "does another thing", "parameters": {}},
        ],
        "collaborations": collaborations or [],
    }).json()
