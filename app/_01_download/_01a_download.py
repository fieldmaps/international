"""Download OSM land polygons and reefs, USGS land polygons, and LSIB boundary lines.

Each source is fetched into ``{source}.new/`` and swapped in only once complete.
"""

import json
import re
import sqlite3
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from email.utils import parsedate_to_datetime
from fnmatch import filter as fnfilter
from logging import getLogger
from pathlib import Path
from shutil import rmtree
from xml.etree import ElementTree as ET
from zipfile import ZipFile

from duckdb import DuckDBPyConnection, connect
from httpx2 import Client, Timeout
from tenacity import retry, stop_after_attempt, wait_exponential

from app.config import (
    LAND_OSM_URL,
    LAND_USGS_URL,
    LSIB_META_URL,
    LSIB_NS,
    LSIB_URL,
    OSM_REEFS,
    OVERPASS_URL,
    PARQUET_OPTS,
    download_dir,
)

logger = getLogger(__name__)
_retry = retry(
    stop=stop_after_attempt(6), wait=wait_exponential(multiplier=10), reraise=True
)


def _stream_to_file(dest: Path, url: str) -> None:
    with (
        Client(timeout=Timeout(60, connect=30)) as client,
        client.stream("GET", url) as r,
    ):
        r.raise_for_status()
        with dest.open("wb") as f:
            f.writelines(r.iter_bytes(chunk_size=4 * 1024 * 1024))


def _download_zip(output: Path, url: str, only: str | None = None) -> None:
    tmp = output / ".tmp"
    try:
        _stream_to_file(tmp, url)
        with ZipFile(tmp) as z:
            members = fnfilter(z.namelist(), only) if only else None
            z.extractall(output, members)
    finally:
        tmp.unlink(missing_ok=True)


def _download_file(output: Path, url: str) -> None:
    _stream_to_file(output / Path(url).name, url)


@_retry
def _fetch_lsib(lsib: Path) -> None:
    with Client(timeout=Timeout(60, connect=30)) as client:
        r = client.get(LSIB_META_URL, headers={"Accept": "application/xml"})
        r.raise_for_status()
    el = ET.fromstring(r.text).find(  # noqa: S314
        ".//gmd:CI_Citation/gmd:date/gmd:CI_Date/gmd:date/gco:Date", LSIB_NS
    )
    if el is None or el.text is None:
        msg = "LSIB date not found in metadata response"
        raise ValueError(msg)
    _download_file(lsib, LSIB_URL)
    (lsib / ".date").write_text(el.text)


@_retry
def _fetch_osm(osm: Path) -> None:
    with Client(timeout=Timeout(60, connect=30)) as client:
        r = client.head(LAND_OSM_URL)
        r.raise_for_status()
        last_modified = r.headers.get("last-modified")
    if not last_modified:
        msg = "OSM land date not found in Last-Modified response header"
        raise ValueError(msg)
    date = parsedate_to_datetime(last_modified).date().isoformat()
    _download_zip(osm, LAND_OSM_URL)
    (osm / ".date").write_text(date)


@_retry
def _fetch_reefs(osm: Path) -> None:
    ids = "".join(f"{t}(id:{','.join(map(str, v))});" for t, v in OSM_REEFS.items())
    query = f"[out:json][timeout:60];({ids});out geom;"
    with Client(timeout=Timeout(120, connect=30)) as client:
        r = client.post(OVERPASS_URL, data={"data": query})
        r.raise_for_status()
    found = {(e["type"], e["id"]) for e in r.json()["elements"]}
    expected = {(t, i) for t, v in OSM_REEFS.items() for i in v}
    if found != expected:
        msg = f"Overpass reefs missing: {sorted(expected - found)}"
        raise ValueError(msg)
    (osm / "reefs.json").write_text(r.text)


def _usgs_date(usgs: Path) -> str:
    """Parse the release date from the folder name, e.g. ``..._24Feb2025``."""
    gpkg = next(usgs.rglob("WorldPolygons*.gpkg"))
    match = re.search(r"_(\d{1,2}[A-Za-z]{3}\d{4})$", gpkg.parent.name)
    if match is None:
        msg = f"World Polygons date not found in folder name: {gpkg.parent.name}"
        raise ValueError(msg)
    return datetime.strptime(match[1], "%d%b%Y").date().isoformat()  # noqa: DTZ007


@_retry
def _fetch_usgs(usgs: Path) -> None:
    # The outer zip also ships a Deflate64 hash .txt that Python's zipfile
    # can't decompress; extract only the inner land-polygons zip we need.
    _download_zip(usgs, LAND_USGS_URL, only="*WorldPolygon*.zip")
    inner = next(usgs.rglob("WorldPolygon*.zip"))
    with ZipFile(inner) as z:
        z.extractall(usgs)
    inner.unlink()
    (usgs / ".date").write_text(_usgs_date(usgs))


def _wp_layer(gpkg: Path) -> str:
    """Return the un-generalized World Polygons layer (e.g. ``DoS_WP_11_4``)."""
    with sqlite3.connect(gpkg) as db:
        return next(
            name
            for (name,) in db.execute(
                "SELECT table_name FROM gpkg_contents WHERE data_type='features'"
            )
            if re.fullmatch(r"DoS_WP_\d+_\d+", name)
        )


def _reefs_sql(conn: DuckDBPyConnection, osm: Path) -> str:
    elements = json.loads((osm / "reefs.json").read_text())["elements"]
    rows = [
        (
            e["type"],
            e["id"],
            e["tags"].get("name"),
            [
                "LINESTRING("
                + ", ".join(f"{p['lon']} {p['lat']}" for p in ring["geometry"])
                + ")"
                for ring in ([e] if e["type"] == "way" else e["members"])
            ],
        )
        for e in elements
    ]
    conn.execute(
        "CREATE TABLE reefs (osm_type VARCHAR, osm_id BIGINT, name VARCHAR, "
        "rings VARCHAR[])"
    )
    conn.executemany("INSERT INTO reefs VALUES (?, ?, ?, ?)", rows)
    return """--sql
        SELECT osm_type, osm_id, name, ST_BuildArea(ST_Collect(
            list_transform(rings, lambda x: ST_GeomFromText(x))
        )) AS geom
        FROM reefs ORDER BY osm_type, osm_id
    """


def _sources(conn: DuckDBPyConnection, name: str, d: Path) -> dict[Path, str]:
    """Return the GeoParquet path and SELECT for each file of source ``name``."""
    if name == "lsib":
        return {d / "lsib.parquet": f"SELECT * FROM ST_Read('{d / 'LSIB.gpkg'}')"}
    if name == "osm":
        shp = next(d.rglob("land_polygons.shp"))
        return {
            d / "land.parquet": f"SELECT FID, geom FROM ST_Read('{shp}')",
            d / "reefs.parquet": _reefs_sql(conn, d),
        }
    gpkg = next(d.rglob("WorldPolygons*.gpkg"))
    # Caspian Sea is the only DoS_WP_* feature without a WP_Name.
    return {
        d / "land.parquet": f"""--sql
            SELECT row_number() OVER () AS "FID", geom FROM (
                SELECT UNNEST(ST_Dump(geom)).geom AS geom
                FROM ST_Read('{gpkg}', layer='{_wp_layer(gpkg)}')
                WHERE WP_Name IS NOT NULL
            )
        """
    }


def _convert(name: str, d: Path) -> None:
    """Write each download of source ``name`` as CRS84 GeoParquet with ``geom``."""
    with connect() as conn:
        conn.execute("LOAD spatial")
        conn.execute("SET geometry_always_xy = true")
        conn.execute("SET threads = 5")
        for dest, sql in _sources(conn, name, d).items():
            conn.execute(f"""--sql
                COPY (
                    SELECT * REPLACE (ST_SetCRS(geom, 'OGC:CRS84') AS geom)
                    FROM ({sql})
                ) TO '{dest}' {PARQUET_OPTS}
            """)


def _fetch_osm_all(osm: Path) -> None:
    _fetch_osm(osm)
    _fetch_reefs(osm)


def _refresh(name: str, fetch: Callable[[Path], None]) -> bool:
    """Fetch and convert ``name`` in ``{name}.new/``, then swap it in.

    On failure the last good copy stays; returns whether the source was refreshed.
    """
    live = download_dir / name
    new = download_dir / f"{name}.new"
    old = download_dir / f"{name}.old"
    rmtree(new, ignore_errors=True)
    new.mkdir(parents=True)
    try:
        fetch(new)
        _convert(name, new)
    except Exception:
        rmtree(new, ignore_errors=True)
        if not (live / ".date").exists():
            raise
        date = (live / ".date").read_text()
        logger.warning("%s: download failed, using copy dated %s", name, date)
        return False
    rmtree(old, ignore_errors=True)
    if live.exists():
        live.rename(old)
    new.rename(live)
    rmtree(old, ignore_errors=True)
    return True


def main() -> None:
    """Download required input datasets, keeping the last good copy of any that fail."""
    jobs = {"lsib": _fetch_lsib, "osm": _fetch_osm_all, "usgs": _fetch_usgs}
    with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        futures = {n: pool.submit(_refresh, n, fn) for n, fn in jobs.items()}
    fresh = [n for n, f in futures.items() if f.result()]
    logger.info("download: %d of %d fetched %s", len(fresh), len(jobs), fresh)
