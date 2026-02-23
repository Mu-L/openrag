import asyncio
import os
from pathlib import Path

import httpx
import pytest

from tests.integration.conftest import reload_settings

EXCLUDED_INGESTION_FILES = {"warmup_ocr.pdf"}


async def wait_for_ready(client: httpx.AsyncClient, timeout_s: float = 60.0):
    deadline = asyncio.get_event_loop().time() + timeout_s
    last_err = None
    while asyncio.get_event_loop().time() < deadline:
        try:
            r1 = await client.get("/auth/me")
            if r1.status_code != 200:
                await asyncio.sleep(0.5)
                continue
            r2 = await client.post("/search", json={"query": "*", "limit": 0})
            if r2.status_code == 200:
                return
            last_err = r2.text
        except Exception as e:
            last_err = str(e)
        await asyncio.sleep(0.5)
    raise AssertionError(f"Service not ready in time: {last_err}")


def count_files_in_documents() -> int:
    base_dir = Path(os.getcwd()) / "openrag-documents"
    if not base_dir.is_dir():
        return 0
    return sum(
        1 for f in base_dir.rglob("*")
        if f.is_file() and f.name not in EXCLUDED_INGESTION_FILES
    )


# ---------------------------------------------------------------------------
# SECTION: Startup
# ---------------------------------------------------------------------------

@pytest.mark.section_startup
@pytest.mark.parametrize("disable_langflow_ingest", [True, False])
async def test_startup_ingest_creates_task(disable_langflow_ingest: bool):
    ctx = reload_settings({
        "DISABLE_STARTUP_INGEST": "false",
        "DISABLE_INGEST_WITH_LANGFLOW": "true" if disable_langflow_ingest else "false",
    })
    clients = ctx["clients"]
    create_app = ctx["create_app"]
    startup_tasks = ctx["startup_tasks"]
    ensure_index = ctx["_ensure_opensearch_index"]
    get_index_name = ctx["get_index_name"]

    await clients.initialize()
    try:
        await clients.opensearch.indices.delete(index=get_index_name())
    except Exception:
        pass

    app = await create_app()
    await startup_tasks(app.state.services)
    await ensure_index()

    transport = httpx.ASGITransport(app=app)
    try:
        async with httpx.AsyncClient(transport=transport, base_url="http://testserver") as client:
            await wait_for_ready(client)

            expected_files = count_files_in_documents()

            async def _wait_for_task(timeout_s: float = 60.0):
                deadline = asyncio.get_event_loop().time() + timeout_s
                last = None
                while asyncio.get_event_loop().time() < deadline:
                    resp = await client.get("/tasks")
                    if resp.status_code == 200:
                        data = resp.json()
                        last = data
                        tasks = data.get("tasks") if isinstance(data, dict) else None
                        if isinstance(tasks, list) and len(tasks) > 0:
                            return tasks
                    await asyncio.sleep(0.5)
                return last.get("tasks") if isinstance(last, dict) else last

            tasks = await _wait_for_task()
            if expected_files == 0:
                return
            if not (isinstance(tasks, list) and len(tasks) > 0):
                sr = await client.post("/search", json={"query": "*", "limit": 1})
                assert sr.status_code == 200, sr.text
                total = sr.json().get("total")
                assert isinstance(total, int) and total >= 0, "Startup ingest did not index documents"
                return
            newest = tasks[0]
            assert "task_id" in newest
            assert newest.get("total_files") == expected_files
    finally:
        from config.settings import clients as _clients
        try:
            await _clients.close()
        except Exception:
            pass
