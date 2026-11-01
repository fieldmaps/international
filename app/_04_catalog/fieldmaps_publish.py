"""Run portolan on a catalog and sync it to its remote with the AWS CLI.

Vendored identically in fieldmaps/subnational, fieldmaps/international and
OCHA-DAP/cod-site.
"""

import json
import os
import subprocess
from collections.abc import Sequence
from logging import getLogger
from pathlib import Path

from dotenv import load_dotenv

logger = getLogger(__name__)

HIDDEN = (".*", "*/.*")


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


def add(
    catalog: Path,
    collection: str,
    *args: str,
    strict: bool = True,
    command: Sequence[str] = ("portolan",),
) -> dict:
    """Run `portolan add`, keeping each parquet's asset key beside its PMTiles."""
    kwargs = {"strict": strict, "command": command}
    envelope = run(catalog, "add", collection, *args, **kwargs)
    target = catalog / collection
    root = target if target.is_dir() else target.parent
    seeded = [_seed_tile_keys(p.parent) for p in sorted(root.rglob("collection.json"))]
    if any(seeded):
        envelope = run(catalog, "add", collection, "--force", *args, **kwargs)
    return envelope


def _seed_tile_keys(collection: Path) -> bool:
    """Key each <stem>.pmtiles as <stem>-tiles; True when collection.json changed."""
    # portolan add can give <stem>.pmtiles the parquet's key, then reuses keys by href.
    path = collection / "collection.json"
    if not path.exists():
        return False
    data = json.loads(path.read_text())
    assets = data.setdefault("assets", {})
    changed = False
    for parquet in sorted(collection.glob("*.parquet")):
        stem, tiles = parquet.stem, f"./{parquet.stem}.pmtiles"
        if not (collection / f"{stem}.pmtiles").exists():
            continue
        if assets.get(stem, {}).get("href") == f"./{parquet.name}" and any(
            k == f"{stem}-tiles" and a.get("href") == tiles for k, a in assets.items()
        ):
            continue
        hrefs = (tiles, f"./{parquet.name}")
        tile_asset = next((a for a in assets.values() if a.get("href") == tiles), {})
        for key in [k for k, a in assets.items() if a.get("href") in hrefs]:
            del assets[key]
        assets[f"{stem}-tiles"] = {**tile_asset, "href": tiles, "roles": ["visual"]}
        assets[stem] = {
            "href": f"./{parquet.name}",
            "type": "application/vnd.apache.parquet",
            "roles": ["data"],
        }
        changed = True
    if changed:
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n")
    return changed


def push(
    catalog: Path,
    remote: str | None = None,
    *,
    exclude: Sequence[str] = (),
) -> None:
    """Sync catalog/ to the remote, versions.json last."""
    load_dotenv(catalog.parent / ".env")
    remote = remote or os.environ["PORTOLAN_REMOTE"]
    excludes = [*HIDDEN, *exclude]
    _sync(catalog, remote, [*excludes, "*versions.json"])
    _sync(catalog, remote, excludes)


def _sync(catalog: Path, remote: str, excludes: Sequence[str]) -> None:
    """Run `aws s3 sync` with the repo-root aws.config, raising if any upload fails."""
    # aws.config retries each dropped multipart part, which source.coop needs.
    env = {**os.environ, "AWS_CONFIG_FILE": str(catalog.parent / "aws.config")}
    args = [arg for pattern in excludes for arg in ("--exclude", pattern)]
    result = subprocess.run(
        ["aws", "s3", "sync", str(catalog), remote, "--no-progress", *args],
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode:
        msg = f"aws s3 sync exited {result.returncode}: {result.stderr}"
        raise RuntimeError(msg)
    for line in result.stdout.splitlines():
        logger.info(line)
