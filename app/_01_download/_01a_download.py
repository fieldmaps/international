"""Download OSM land polygons and reefs, USGS land polygons, and LSIB boundary lines.

Always fetches fresh data. Source publication dates are captured atomically
with each download (OSM Last-Modified header, LSIB metadata XML, World Polygons
folder name) and written to ``tmp/download/{osm,usgs,lsib}/.date``. Each source
is then converted to CRS84 GeoParquet beside its download.
"""

import json
import re
import sqlite3
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
    rmtree(usgs)
    usgs.mkdir()
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


def _convert(osm: Path, usgs: Path, lsib: Path) -> None:
    """Write each download as CRS84 GeoParquet with a ``geom`` column."""
    osm_shp = next(osm.rglob("land_polygons.shp"))
    usgs_gpkg = next(usgs.rglob("WorldPolygons*.gpkg"))
    with connect() as conn:
        conn.execute("LOAD spatial")
        conn.execute("SET geometry_always_xy = true")
        conn.execute("SET threads = 5")
        sources = {
            lsib / "lsib.parquet": f"SELECT * FROM ST_Read('{lsib / 'LSIB.gpkg'}')",
            osm / "land.parquet": f"SELECT FID, geom FROM ST_Read('{osm_shp}')",
            # Caspian Sea is the only DoS_WP_* feature without a WP_Name.
            usgs / "land.parquet": f"""--sql
                SELECT row_number() OVER () AS "FID", geom FROM (
                    SELECT UNNEST(ST_Dump(geom)).geom AS geom
                    FROM ST_Read('{usgs_gpkg}', layer='{_wp_layer(usgs_gpkg)}')
                    WHERE WP_Name IS NOT NULL
                )
            """,
            osm / "reefs.parquet": _reefs_sql(conn, osm),
        }
        for dest, sql in sources.items():
            conn.execute(f"""--sql
                COPY (
                    SELECT * REPLACE (ST_SetCRS(geom, 'OGC:CRS84') AS geom)
                    FROM ({sql})
                ) TO '{dest}' {PARQUET_OPTS}
            """)


def main() -> None:
    """Download required input datasets."""
    osm = download_dir / "osm"
    usgs = download_dir / "usgs"
    lsib = download_dir / "lsib"
    for d in [osm, usgs, lsib]:
        d.mkdir(parents=True, exist_ok=True)

    jobs = [
        (_fetch_lsib, lsib),
        (_fetch_osm, osm),
        (_fetch_reefs, osm),
        (_fetch_usgs, usgs),
    ]
    with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        futures = [pool.submit(fn, arg) for fn, arg in jobs]
    for f in futures:
        f.result()
    _convert(osm, usgs, lsib)
    logger.info("download: %d fetched", len(jobs))
