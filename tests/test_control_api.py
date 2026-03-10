from __future__ import annotations

import json
import threading
import time
from contextlib import contextmanager
from typing import Iterator
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from app import ControlApi, JobStore, create_server


def wait_for_job_completion(
    control_api: ControlApi,
    job_id: str,
    *,
    timeout: float = 3.0,
) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        job = control_api.job_store.get_job(job_id)
        if job and job["status"] in {"succeeded", "failed"}:
            return job
        time.sleep(0.01)
    raise AssertionError(f"Timed out waiting for job {job_id}")


def read_json_response(request: Request | str) -> tuple[int, dict]:
    try:
        with urlopen(request) as response:
            return response.status, json.load(response)
    except HTTPError as error:
        return error.code, json.load(error)


@contextmanager
def running_control_server(control_api: ControlApi) -> Iterator[str]:
    server = create_server("127.0.0.1", 0, control_api=control_api)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}"
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_job_store_prunes_old_completed_jobs() -> None:
    store = JobStore(retain_completed=2)

    job1 = store.create_job("scrape", {})
    store.mark_running(job1["id"])
    store.mark_succeeded(job1["id"], {"ok": 1})

    job2 = store.create_job("sentiment", {})
    store.mark_running(job2["id"])
    store.mark_failed(job2["id"], RuntimeError("boom"), "traceback")

    job3 = store.create_job("ner", {})
    store.mark_running(job3["id"])
    store.mark_succeeded(job3["id"], {"ok": 3})

    assert store.get_job(job1["id"]) is None
    assert [job["id"] for job in store.list_jobs()] == [job3["id"], job2["id"]]


def test_job_store_retains_most_recently_completed_jobs() -> None:
    store = JobStore(retain_completed=2)

    job1 = store.create_job("scrape", {})
    job2 = store.create_job("sentiment", {})
    job3 = store.create_job("ner", {})

    store.mark_running(job1["id"])
    store.mark_running(job2["id"])
    store.mark_running(job3["id"])

    store.mark_succeeded(job2["id"], {"ok": 2})
    store.mark_succeeded(job3["id"], {"ok": 3})
    store.mark_succeeded(job1["id"], {"ok": 1})

    assert store.get_job(job2["id"]) is None
    assert store.get_job(job1["id"])["result"] == {"ok": 1}
    assert store.get_job(job3["id"])["result"] == {"ok": 3}
    assert [job["id"] for job in store.list_jobs()] == [job3["id"], job1["id"]]


def test_control_api_serializes_job_execution_with_runner_lock() -> None:
    control_api = ControlApi(job_store=JobStore(retain_completed=10), job_handlers={})
    first_entered = threading.Event()
    allow_first_to_finish = threading.Event()
    second_entered = threading.Event()
    execution_order: list[str] = []

    def first_job(_: dict) -> dict:
        execution_order.append("first-start")
        first_entered.set()
        allow_first_to_finish.wait(timeout=2)
        execution_order.append("first-end")
        return {"name": "first"}

    def second_job(_: dict) -> dict:
        execution_order.append("second-start")
        second_entered.set()
        execution_order.append("second-end")
        return {"name": "second"}

    first = control_api.submit_job("scrape", {}, first_job)
    assert first_entered.wait(timeout=2)

    second = control_api.submit_job("ner", {}, second_job)
    time.sleep(0.05)
    assert second_entered.is_set() is False

    allow_first_to_finish.set()
    assert second_entered.wait(timeout=2)

    wait_for_job_completion(control_api, first["id"])
    wait_for_job_completion(control_api, second["id"])

    assert execution_order.index("first-end") < execution_order.index("second-start")


def test_control_api_http_lifecycle_and_error_paths() -> None:
    def fake_scrape(payload: dict) -> dict:
        if payload.get("fail"):
            raise RuntimeError("synthetic failure")
        return {"echo": payload.get("value")}

    control_api = ControlApi(
        job_store=JobStore(retain_completed=10),
        job_handlers={"/api/jobs/scrape": ("scrape", fake_scrape)},
    )

    with running_control_server(control_api) as base_url:
        status, invalid_payload = read_json_response(
            Request(
                f"{base_url}/api/jobs/scrape",
                data=b"[]",
                method="POST",
                headers={"Content-Type": "application/json"},
            )
        )
        assert status == 400
        assert "Invalid JSON body" in invalid_payload["error"]

        status, accepted = read_json_response(
            Request(
                f"{base_url}/api/jobs/scrape",
                data=json.dumps({"value": 7}).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
        )
        assert status == 202
        success_job_id = accepted["job"]["id"]

        status, accepted_failure = read_json_response(
            Request(
                f"{base_url}/api/jobs/scrape",
                data=json.dumps({"fail": True}).encode("utf-8"),
                method="POST",
                headers={"Content-Type": "application/json"},
            )
        )
        assert status == 202
        failed_job_id = accepted_failure["job"]["id"]

        success_job = wait_for_job_completion(control_api, success_job_id)
        failed_job = wait_for_job_completion(control_api, failed_job_id)

        assert success_job["result"] == {"echo": 7}
        assert failed_job["error"]["message"] == "synthetic failure"

        status, jobs_payload = read_json_response(f"{base_url}/api/jobs")
        assert status == 200
        assert [job["id"] for job in jobs_payload["jobs"]] == [failed_job_id, success_job_id]

        status, success_payload = read_json_response(f"{base_url}/api/jobs/{success_job_id}")
        assert status == 200
        assert success_payload["job"]["result"] == {"echo": 7}

        status, health_payload = read_json_response(f"{base_url}/api/health")
        assert status == 200
        assert health_payload["jobs"] == {"queued": 0, "running": 0}
