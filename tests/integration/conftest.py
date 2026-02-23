"""Integration test fixtures.

Centralises the module-reload / app-creation / index-cleanup boilerplate that
was previously copy-pasted into every integration test function.
"""

import asyncio
import os
import sys
import httpx
import pytest_asyncio

# Canonical superset of modules that must be evicted from ``sys.modules``
# before re-importing so that changed environment variables are picked up.
MODULES_TO_RELOAD = [
    "api.router",
    "api.connector_router",
    "api.chat",
    "api.nudges",
    "config.settings",
    "auth_middleware",
    "main",
    "api",
    "services",
    "services.search_service",
    "services.chat_service",
]


def reload_settings(env_overrides: dict[str, str] | None = None):
    """Apply *env_overrides*, evict cached modules, and re-import key symbols.

    Returns a dict with the freshly-imported objects that tests typically need:
    ``create_app``, ``startup_tasks``, ``_ensure_opensearch_index``,
    ``clients``, ``get_index_name``.
    """
    defaults = {
        "DISABLE_STARTUP_INGEST": "true",
        "EMBEDDING_MODEL": "text-embedding-3-small",
        "EMBEDDING_PROVIDER": "openai",
        "GOOGLE_OAUTH_CLIENT_ID": "",
        "GOOGLE_OAUTH_CLIENT_SECRET": "",
    }
    for key, val in defaults.items():
        os.environ.setdefault(key, val)
    if env_overrides:
        os.environ.update(env_overrides)

    for mod in MODULES_TO_RELOAD:
        sys.modules.pop(mod, None)

    from main import create_app, startup_tasks, _ensure_opensearch_index
    from config.settings import clients, get_index_name

    return {
        "create_app": create_app,
        "startup_tasks": startup_tasks,
        "_ensure_opensearch_index": _ensure_opensearch_index,
        "clients": clients,
        "get_index_name": get_index_name,
    }


@pytest_asyncio.fixture
async def fresh_app(request):
    """Function-scoped fixture that creates a clean ASGI app.

    Accepts ``disable_langflow_ingest`` via parametrize; falls back to ``True``.

    Yields ``(app, clients, get_index_name)`` and tears down clients on exit.
    """
    disable_langflow = getattr(request, "param", None)
    if disable_langflow is None:
        disable_langflow = request.node.callobj.__defaults__
        if disable_langflow:
            disable_langflow = disable_langflow[0]
        else:
            disable_langflow = True

    # If test is parametrized with ``disable_langflow_ingest``, grab from there
    if "disable_langflow_ingest" in (request.fixturenames or []):
        disable_langflow = request.getfixturevalue("disable_langflow_ingest")

    env = {
        "DISABLE_INGEST_WITH_LANGFLOW": "true" if disable_langflow else "false",
    }

    ctx = reload_settings(env)
    clients = ctx["clients"]
    create_app = ctx["create_app"]
    startup_tasks = ctx["startup_tasks"]
    ensure_index = ctx["_ensure_opensearch_index"]
    get_index_name = ctx["get_index_name"]

    await clients.initialize()

    app = await create_app()
    await startup_tasks(app.state.services)
    await ensure_index()

    yield app, clients, get_index_name

    try:
        await clients.close()
    except Exception:
        pass


@pytest_asyncio.fixture
async def clean_index(fresh_app):
    """Delete and recreate the OpenSearch index so the test starts empty."""
    app, clients, get_index_name = fresh_app
    index = get_index_name()

    try:
        await clients.opensearch.indices.delete(index=index)
        # Wait for the deletion to propagate
        await asyncio.sleep(1)
    except Exception:
        pass

    from main import _ensure_opensearch_index
    await _ensure_opensearch_index()

    # Confirm the index is empty
    try:
        resp = await clients.opensearch.count(index=index)
        count = resp.get("count", 0)
        if count > 0:
            print(f"[WARN] Index {index} still has {count} docs after cleanup")
    except Exception:
        pass

    yield app, clients, get_index_name


@pytest_asyncio.fixture
async def test_http_client(fresh_app):
    """Provide an ``httpx.AsyncClient`` wired to the ASGI app.

    Waits for the service to be ready before yielding.
    """
    app, _clients, _get_index_name = fresh_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
        yield client
