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
DEFAULT_CODEX_MANAGER_DB_FILENAME = "codexmanager.db"


def resolve_project_python() -> Optional[Path]:
    if os.name == "nt":
        candidate = ROOT / ".venv" / "Scripts" / "python.exe"
    else:
        candidate = ROOT / ".venv" / "bin" / "python"
    if candidate.is_file():
        return candidate
    return None


def ensure_project_python() -> None:
    project_python = resolve_project_python()
    if project_python is None:
        return

    current_python = Path(sys.executable).resolve()
    target_python = project_python.resolve()
    if current_python == target_python:
        return

    if os.environ.get("CODEX_CONSOLE_BOOTSTRAPPED") == "1":
        return

    env = os.environ.copy()
    env["CODEX_CONSOLE_BOOTSTRAPPED"] = "1"
    os.execve(
        str(target_python),
        [str(target_python), str(Path(__file__).resolve()), *sys.argv[1:]],
        env,
    )


def log(message: str) -> None:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{stamp}] {message}", flush=True)


def resolve_console_env() -> Dict[str, str]:
    env = os.environ.copy()
    port = str(
        env.get("CODEX_CONSOLE_PORT")
        or env.get("APP_PORT")
        or env.get("WEBUI_PORT")
        or DEFAULT_WEB_PORT
    )
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
        raise RuntimeError(
            "use Codex-Manager service RPC address "
            "(default 127.0.0.1:48760), not the web UI port 48761"
        )
    if path == "/rpc":
        return raw.rstrip("/")
    if path in {"", "/"}:
        return raw.rstrip("/") + "/rpc"
    return raw.rstrip("/")


def resolve_rpc_token_path() -> Optional[Path]:
    token_file = str(
        os.environ.get("CODEX_MANAGER_RPC_TOKEN_FILE")
        or os.environ.get("CODEXMANAGER_RPC_TOKEN_FILE")
        or ""
    ).strip()
    if token_file:
        path = Path(token_file).expanduser()
        if path.is_file():
            return path

    data_dir = str(
        os.environ.get("CODEX_MANAGER_DATA_DIR")
        or os.environ.get("CODEXMANAGER_DATA_DIR")
        or ""
    ).strip()
    if data_dir:
        path = Path(data_dir).expanduser() / DEFAULT_RPC_TOKEN_FILENAME
        if path.is_file():
            return path

    appdata = os.environ.get("APPDATA")
    localappdata = os.environ.get("LOCALAPPDATA")
    candidates = []
    if appdata:
        candidates.append(
            Path(appdata) / "com.codexmanager.desktop" / DEFAULT_RPC_TOKEN_FILENAME
        )
    if localappdata:
        candidates.append(
            Path(localappdata) / "com.codexmanager.desktop" / DEFAULT_RPC_TOKEN_FILENAME
        )
    candidates.append(Path.home() / ".codexmanager" / DEFAULT_RPC_TOKEN_FILENAME)

    for path in candidates:
        if path.is_file():
            return path

    return None


def resolve_rpc_token() -> str:
    direct = str(
        os.environ.get("CODEX_MANAGER_RPC_TOKEN")
        or os.environ.get("CODEXMANAGER_RPC_TOKEN")
        or ""
    ).strip()
    if direct:
        return direct

    path = resolve_rpc_token_path()
    if path and path.is_file():
        return path.read_text(encoding="utf-8").strip()

    return ""


def resolve_codex_manager_db_path() -> Optional[Path]:
    data_dir = str(
        os.environ.get("CODEX_MANAGER_DATA_DIR")
        or os.environ.get("CODEXMANAGER_DATA_DIR")
        or ""
    ).strip()
    if data_dir:
        path = Path(data_dir).expanduser() / DEFAULT_CODEX_MANAGER_DB_FILENAME
        if path.is_file():
            return path

    token_path = resolve_rpc_token_path()
    if token_path is not None:
        candidate = token_path.parent / DEFAULT_CODEX_MANAGER_DB_FILENAME
        if candidate.is_file():
            return candidate

    appdata = os.environ.get("APPDATA")
    if appdata:
        candidate = (
            Path(appdata) / "com.codexmanager.desktop" / DEFAULT_CODEX_MANAGER_DB_FILENAME
        )
        if candidate.is_file():
            return candidate

    return None


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
    STATE_PATH.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


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
    if not access_token:
        return None

    account_id = str(row["account_id"] or "").strip()
    workspace_id = str(row["workspace_id"] or "").strip()
    refresh_token = str(row["refresh_token"] or "").strip()
    id_token = str(row["id_token"] or "").strip()

    item = {
        "type": "chatgptAuthTokens",
        "accessToken": access_token,
        "chatgptAccountId": account_id,
        "workspaceId": workspace_id,
    }
    if refresh_token:
        item["refreshToken"] = refresh_token
    if id_token:
        item["idToken"] = id_token
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


def get_manual_account_id(rpc_url: str, rpc_token: str) -> Optional[str]:
    result = rpc_call(rpc_url, rpc_token, "gateway/manualAccount/get", {})
    value = str(result.get("accountId") or "").strip()
    return value or None


def get_current_auth_account(rpc_url: str, rpc_token: str) -> Optional[dict]:
    result = rpc_call(rpc_url, rpc_token, "account/read", {"refreshToken": False})
    account = result.get("account")
    if isinstance(account, dict):
        return account
    return None


def update_account_label(
    rpc_url: str,
    rpc_token: str,
    account_id: str,
    label: str,
) -> None:
    rpc_call(
        rpc_url,
        rpc_token,
        "account/update",
        {
            "accountId": account_id,
            "label": label,
        },
    )


def set_manual_account_id(
    rpc_url: str,
    rpc_token: str,
    account_id: Optional[str],
) -> None:
    if account_id:
        rpc_call(rpc_url, rpc_token, "gateway/manualAccount/set", {"accountId": account_id})
    else:
        rpc_call(rpc_url, rpc_token, "gateway/manualAccount/clear", {})


def snapshot_auth_state(db_path: Optional[Path]) -> Optional[Dict[str, Optional[str]]]:
    if db_path is None or not db_path.is_file():
        return None

    connection = sqlite3.connect(str(db_path))
    try:
        rows = connection.execute(
            """
            SELECT key, value
            FROM app_settings
            WHERE key IN ('auth.current_account_id', 'auth.current_auth_mode')
            """
        ).fetchall()
    finally:
        connection.close()

    mapping = {str(key): str(value) for key, value in rows}
    return {
        "auth.current_account_id": mapping.get("auth.current_account_id"),
        "auth.current_auth_mode": mapping.get("auth.current_auth_mode"),
    }


def restore_auth_state(
    db_path: Optional[Path],
    state: Optional[Dict[str, Optional[str]]],
) -> bool:
    if db_path is None or not db_path.is_file() or state is None:
        return False

    try:
        connection = sqlite3.connect(str(db_path))
    except sqlite3.Error:
        return False

    try:
        try:
            for key in ("auth.current_account_id", "auth.current_auth_mode"):
                value = state.get(key)
                if value is None:
                    connection.execute("DELETE FROM app_settings WHERE key = ?", (key,))
                else:
                    connection.execute(
                        """
                        INSERT INTO app_settings(key, value, updated_at)
                        VALUES (?, ?, strftime('%s','now'))
                        ON CONFLICT(key) DO UPDATE SET
                            value=excluded.value,
                            updated_at=excluded.updated_at
                        """,
                        (key, value),
                    )
            connection.commit()
            return True
        except sqlite3.Error:
            return False
    finally:
        connection.close()


def sync_account(
    rpc_url: str,
    rpc_token: str,
    row: sqlite3.Row,
    codex_manager_db_path: Optional[Path],
) -> None:
    item = build_import_item(row)
    if item is None:
        raise RuntimeError("missing required token: access_token")

    previous_manual_account_id = get_manual_account_id(rpc_url, rpc_token)
    previous_auth_state = snapshot_auth_state(codex_manager_db_path)
    try:
        rpc_call(rpc_url, rpc_token, "account/login/start", item)
        current_account = get_current_auth_account(rpc_url, rpc_token)
        if current_account:
            current_id = str(current_account.get("accountId") or "").strip()
            current_chatgpt_id = str(current_account.get("chatgptAccountId") or "").strip()
            current_workspace_id = str(current_account.get("workspaceId") or "").strip()
            expected_chatgpt_id = str(row["account_id"] or "").strip()
            expected_workspace_id = str(row["workspace_id"] or "").strip()
            email = str(row["email"] or "").strip()
            if (
                current_id
                and email
                and (
                    (expected_chatgpt_id and current_chatgpt_id == expected_chatgpt_id)
                    or (
                        expected_workspace_id
                        and current_workspace_id == expected_workspace_id
                    )
                )
            ):
                update_account_label(rpc_url, rpc_token, current_id, email)
    finally:
        try:
            try:
                set_manual_account_id(rpc_url, rpc_token, previous_manual_account_id)
            except Exception:
                pass
        finally:
            restore_auth_state(codex_manager_db_path, previous_auth_state)


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

    codex_manager_db_path = resolve_codex_manager_db_path()
    poll_seconds = int(
        str(os.environ.get("CODEX_MANAGER_POLL_SECONDS") or DEFAULT_POLL_SECONDS).strip()
        or DEFAULT_POLL_SECONDS
    )
    fingerprints = load_state()

    log(
        f"Codex-Manager sync loop started: db={database_path} rpc={rpc_url} "
        f"manager_db={codex_manager_db_path or 'unknown'} poll={poll_seconds}s"
    )

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
                    sync_account(rpc_url, rpc_token, row, codex_manager_db_path)
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
    ensure_project_python()

    env = resolve_console_env()
    database_path = resolve_database_path(env)

    stop_event = threading.Event()
    thread = threading.Thread(
        target=sync_loop,
        args=(stop_event, database_path),
        daemon=True,
    )
    thread.start()

    child: Optional[subprocess.Popen] = None
    start_web = (
        str(os.environ.get("CODEX_CONSOLE_START_WEBUI") or "true").strip().lower()
        in {"1", "true", "yes", "on"}
    )

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
