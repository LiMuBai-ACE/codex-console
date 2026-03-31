"""
Run codex-console with an external Codex-Manager sync sidecar.

This script does not patch codex-console source behavior. It:
1. Starts `webui.py` with env-based port defaults (`7686`)
2. Polls the local SQLite database for new/updated accounts
3. Syncs eligible accounts to local Codex-Manager via JSON-RPC
"""

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Dict, Optional


ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT / "data"
STATE_PATH = DATA_DIR / "codex_manager_sync_state.json"

DEFAULT_WEB_PORT = 7686
DEFAULT_POLL_SECONDS = 5
DEFAULT_RPC_URL = "http://127.0.0.1:48760"
DEFAULT_RPC_TOKEN_FILENAME = "codexmanager.rpc-token"


def log(message: str) -> None:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


def resolve_console_env() -> Dict[str, str]:
    env = os.environ.copy()
    port = str(env.get("CODEX_CONSOLE_PORT") or env.get("APP_PORT") or env.get("WEBUI_PORT") or DEFAULT_WEB_PORT)
    env["APP_PORT"] = port
    env["WEBUI_PORT"] = port
    return env


def resolve_database_path(env: Dict[str, str]) -> Path:
    raw = str(env.get("APP_DATABASE_URL") or env.get("DATABASE_URL") or "").strip()
    if not raw:
        app_data_dir = Path(env.get("APP_DATA_DIR") or (ROOT / "data"))
        return app_data_dir / "database.db"

    if raw.startswith("sqlite:///"):
        raw = raw[10:]

    if "://" in raw and not raw.startswith("sqlite:///"):
        raise RuntimeError("external sync script currently supports SQLite only")

    path = Path(raw)
    if not path.is_absolute():
        path = ROOT / path
    return path


def resolve_rpc_url() -> str:
    raw = str(
        os.environ.get("CODEX_MANAGER_RPC_URL")
        or os.environ.get("CODEX_MANAGER_SERVICE_URL")
        or DEFAULT_RPC_URL
    ).strip()
    if not raw:
        raw = DEFAULT_RPC_URL
    if "://" not in raw:
        raw = f"http://{raw}"
    parsed = urllib.parse.urlparse(raw)
    path = (parsed.path or "").rstrip("/")
    if parsed.port == 48761 or path == "/api/rpc":
        raise RuntimeError("use Codex-Manager service RPC address (default 127.0.0.1:48760), not the web UI port 48761")
    if path == "/rpc":
        return raw.rstrip("/")
    if path in {"", "/"}:
        return raw.rstrip("/") + "/rpc"
    return raw.rstrip("/")


def resolve_rpc_token() -> str:
    direct = str(
        os.environ.get("CODEX_MANAGER_RPC_TOKEN")
        or os.environ.get("CODEXMANAGER_RPC_TOKEN")
        or ""
    ).strip()
    if direct:
        return direct

    token_file = str(
        os.environ.get("CODEX_MANAGER_RPC_TOKEN_FILE")
        or os.environ.get("CODEXMANAGER_RPC_TOKEN_FILE")
        or ""
    ).strip()
    if token_file:
        path = Path(token_file).expanduser()
        if path.is_file():
            return path.read_text(encoding="utf-8").strip()

    data_dir = str(
        os.environ.get("CODEX_MANAGER_DATA_DIR")
        or os.environ.get("CODEXMANAGER_DATA_DIR")
        or ""
    ).strip()
    if data_dir:
        path = Path(data_dir).expanduser() / DEFAULT_RPC_TOKEN_FILENAME
        if path.is_file():
            return path.read_text(encoding="utf-8").strip()

    return ""


def load_state() -> Dict[str, str]:
    if not STATE_PATH.is_file():
        return {}
    try:
        payload = json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    fingerprints = payload.get("fingerprints")
    if isinstance(fingerprints, dict):
        return {str(k): str(v) for k, v in fingerprints.items()}
    return {}


def save_state(fingerprints: Dict[str, str]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"fingerprints": fingerprints, "updated_at": int(time.time())}
    STATE_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def account_fingerprint(row: sqlite3.Row) -> str:
    raw = "|".join(
        [
            str(row["email"] or ""),
            str(row["access_token"] or ""),
            str(row["refresh_token"] or ""),
            str(row["id_token"] or ""),
            str(row["account_id"] or ""),
            str(row["workspace_id"] or ""),
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_import_item(row: sqlite3.Row) -> Optional[dict]:
    access_token = str(row["access_token"] or "").strip()
    refresh_token = str(row["refresh_token"] or "").strip()
    id_token = str(row["id_token"] or "").strip()
    if not access_token or not refresh_token or not id_token:
        return None

    account_id = str(row["account_id"] or "").strip()
    workspace_id = str(row["workspace_id"] or "").strip()

    item = {
        "tokens": {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "id_token": id_token,
        },
        "meta": {
            "label": str(row["email"] or "").strip() or f"account-{row['id']}",
        },
    }
    if account_id:
        item["tokens"]["account_id"] = account_id
        item["tokens"]["chatgpt_account_id"] = account_id
        item["meta"]["chatgpt_account_id"] = account_id
    if workspace_id:
        item["meta"]["workspace_id"] = workspace_id
    return item


def rpc_call(rpc_url: str, rpc_token: str, method: str, params: dict) -> dict:
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "id": int(time.time() * 1000),
            "method": method,
            "params": params,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        rpc_url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "X-CodexManager-Rpc-Token": rpc_token,
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=15) as response:
        payload = response.read().decode("utf-8", errors="replace")
    parsed = json.loads(payload)
    if isinstance(parsed, dict) and parsed.get("error"):
        error = parsed["error"]
        if isinstance(error, dict):
            raise RuntimeError(str(error.get("message") or "rpc error"))
        raise RuntimeError(str(error))
    result = parsed.get("result")
    if not isinstance(result, dict):
        return {}
    return result


def sync_account(rpc_url: str, rpc_token: str, row: sqlite3.Row) -> None:
    item = build_import_item(row)
    if item is None:
        raise RuntimeError("missing required tokens: access_token, refresh_token, id_token")

    result = rpc_call(
        rpc_url,
        rpc_token,
        "account/import",
        {"contents": [json.dumps(item, ensure_ascii=False)]},
    )
    failed = int(result.get("failed") or 0)
    if failed > 0:
        errors = result.get("errors") or []
        if isinstance(errors, list) and errors:
            first = errors[0]
            if isinstance(first, dict):
                raise RuntimeError(str(first.get("message") or "Codex-Manager import failed"))
        raise RuntimeError("Codex-Manager import failed")


def sync_loop(stop_event: threading.Event, database_path: Path) -> None:
    enabled = str(os.environ.get("CODEX_MANAGER_SYNC_ENABLED") or "true").strip().lower()
    if enabled not in {"1", "true", "yes", "on"}:
        log("Codex-Manager sync disabled by environment")
        return

    rpc_url = resolve_rpc_url()
    rpc_token = resolve_rpc_token()
    if not rpc_token:
        log("Codex-Manager RPC token missing; sync loop will not start")
        return

    poll_seconds = int(str(os.environ.get("CODEX_MANAGER_POLL_SECONDS") or DEFAULT_POLL_SECONDS).strip() or DEFAULT_POLL_SECONDS)
    fingerprints = load_state()
    log(f"Codex-Manager sync loop started: db={database_path} rpc={rpc_url} poll={poll_seconds}s")

    while not stop_event.is_set():
        try:
            if not database_path.is_file():
                time.sleep(poll_seconds)
                continue

            connection = sqlite3.connect(str(database_path))
            connection.row_factory = sqlite3.Row
            try:
                rows = connection.execute(
                    """
                    SELECT id, email, access_token, refresh_token, id_token, account_id, workspace_id, status
                    FROM accounts
                    ORDER BY id ASC
                    """
                ).fetchall()
            finally:
                connection.close()

            changed = False
            for row in rows:
                status = str(row["status"] or "").strip().lower()
                if status and status not in {"active", "expired", "banned", "failed"}:
                    continue
                item = build_import_item(row)
                if item is None:
                    continue
                key = str(row["id"])
                fingerprint = account_fingerprint(row)
                if fingerprints.get(key) == fingerprint:
                    continue
                try:
                    sync_account(rpc_url, rpc_token, row)
                    fingerprints[key] = fingerprint
                    changed = True
                    log(f"synced account #{row['id']} {row['email']}")
                except Exception as exc:
                    log(f"sync failed for account #{row['id']} {row['email']}: {exc}")

            if changed:
                save_state(fingerprints)
        except urllib.error.HTTPError as exc:
            log(f"Codex-Manager HTTP error: {exc.code}")
        except urllib.error.URLError as exc:
            log(f"Codex-Manager connection error: {exc}")
        except sqlite3.Error as exc:
            log(f"database read error: {exc}")
        except Exception as exc:
            log(f"sync loop error: {exc}")

        stop_event.wait(poll_seconds)


def start_webui(env: Dict[str, str]) -> subprocess.Popen:
    command = [sys.executable, "webui.py"]
    log(f"starting codex-console on port {env['APP_PORT']}")
    return subprocess.Popen(command, cwd=str(ROOT), env=env)


def main() -> int:
    env = resolve_console_env()
    database_path = resolve_database_path(env)

    stop_event = threading.Event()
    thread = threading.Thread(target=sync_loop, args=(stop_event, database_path), daemon=True)
    thread.start()

    child: Optional[subprocess.Popen] = None
    start_web = str(os.environ.get("CODEX_CONSOLE_START_WEBUI") or "true").strip().lower() in {"1", "true", "yes", "on"}

    try:
        if start_web:
            child = start_webui(env)
            return child.wait()

        log("CODEX_CONSOLE_START_WEBUI=false; sync sidecar running without launching webui")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log("shutdown requested")
        return 0
    finally:
        stop_event.set()
        if child and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=5)
            except Exception:
                child.kill()


if __name__ == "__main__":
    raise SystemExit(main())
