"""Run and push portolan catalogs, verifying the remote matches local files.

Vendored identically in fieldmaps/subnational and fieldmaps/international.
"""

import json
import os
import subprocess
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from fnmatch import fnmatch
from logging import getLogger
from pathlib import Path

import obstore as obs
from dotenv import load_dotenv
from obstore.store import S3Store
from tenacity import retry, stop_after_attempt, wait_exponential

logger = getLogger(__name__)

UPLOAD_WORKERS = 4
CHUNK_SIZE = 16 * 1024 * 1024


def run(
    catalog: Path,
    *args: str,
    strict: bool = True,
    command: Sequence[str] = ("portolan",),
) -> dict:
    """Run a portolan command in catalog/ and return its JSON envelope."""
    load_dotenv(catalog.parent / ".env")
    result = subprocess.run(
        [*command, "--format", "json", *args],
        cwd=catalog,
        capture_output=True,
        text=True,
        check=False,
    )
    # Some commands print progress lines before the envelope, even in JSON mode.
    out = result.stdout
    start = 0 if out.startswith("{") else out.rfind("\n{") + 1
    try:
        envelope = json.loads(out[start:])
    except json.JSONDecodeError as e:
        msg = f"portolan {' '.join(args)} exited {result.returncode}: {result.stderr}"
        raise RuntimeError(msg) from e
    if strict and not envelope["success"]:
        msg = f"portolan {' '.join(args)} failed: {envelope['errors']}"
        raise RuntimeError(msg)
    return envelope


def push(
    catalog: Path,
    remote: str | None = None,
    *,
    command: Sequence[str] = ("portolan",),
    exclude: Sequence[str] = (),
) -> None:
    """Run `portolan push`, then upload every file the remote lacks or holds stale."""
    load_dotenv(catalog.parent / ".env")
    remote = remote or os.environ["PORTOLAN_REMOTE"]
    envelope = run(catalog, "push", remote, strict=False, command=command)
    if not envelope["success"]:
        logger.warning("portolan push failed, repairing: %s", envelope["errors"])
    store = S3Store.from_url(remote)
    repair(catalog, store, exclude)
    if stale := _stale(catalog, store, exclude):
        msg = f"remote still differs after repair: {stale[:10]}"
        raise RuntimeError(msg)
    logger.info("push verified")


def repair(catalog: Path, store: S3Store, exclude: Sequence[str] = ()) -> list[str]:
    """Upload stale files, versions.json last so it never points at missing bytes."""
    stale = _stale(catalog, store, exclude)
    assets = [p for p in stale if Path(p).name != "versions.json"]
    versions = [p for p in stale if Path(p).name == "versions.json"]
    for batch in (assets, versions):
        with ThreadPoolExecutor(UPLOAD_WORKERS) as pool:
            list(pool.map(lambda p: _upload(store, catalog, p), batch))
    logger.info("repaired %d files", len(stale))
    return stale


def _stale(catalog: Path, store: S3Store, exclude: Sequence[str] = ()) -> list[str]:
    """List local files missing on the remote, sized differently, or newer locally."""
    remote = {
        m["path"]: (m["size"], m["last_modified"])
        for batch in obs.list(store, chunk_size=1000)
        for m in batch
    }
    stale = []
    for path in sorted(catalog.rglob("*")):
        rel = path.relative_to(catalog)
        if not path.is_file() or any(p.startswith(".") for p in rel.parts):
            continue
        if any(fnmatch(rel.as_posix(), pattern) for pattern in exclude):
            continue
        stat = path.stat()
        size, modified = remote.get(rel.as_posix(), (None, None))
        local_modified = datetime.fromtimestamp(stat.st_mtime, UTC)
        if size != stat.st_size or local_modified > modified:
            stale.append(rel.as_posix())
    return stale


@retry(stop=stop_after_attempt(10), wait=wait_exponential(max=60), reraise=True)
def _upload(store: S3Store, catalog: Path, rel: str) -> None:
    """Upload one file, retrying the dropped connections source.coop returns."""
    obs.put(store, rel, catalog / rel, chunk_size=CHUNK_SIZE, max_concurrency=4)
    logger.info("uploaded %s", rel)
