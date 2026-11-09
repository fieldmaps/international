"""Write catalog GeoParquet for lines, points, and polygons, plus voronoi."""

from logging import getLogger
from pathlib import Path

from duckdb import DuckDBPyConnection

from app.config import (
    CATALOG_DIR,
    EXPORT_LAYERS,
    PARQUET_OPTS,
    catalog_dir,
    output_dir,
)
from app.utils import raise_issues

logger = getLogger(__name__)
# A 1-byte limit closes a row group after every row; ROW_GROUP_SIZE has a floor.
POLYGON_OPTS = (
    "(COMPRESSION ZSTD, COMPRESSION_LEVEL 15, GEOPARQUET_VERSION V2, "
    "ROW_GROUP_SIZE_BYTES 1)"
)


def catalog_parquet(land: str, wld: str, geom: str) -> Path:
    """Return the catalog GeoParquet path for a land, world view, and layer."""
    collection = catalog_dir / CATALOG_DIR[land, wld] / f"adm0-{geom}"
    return collection / f"adm0_{geom}.parquet"


def _copy_parquet(
    conn: DuckDBPyConnection, table: str, file_out: Path, *, has_id: bool, opts: str
) -> None:
    exclude = "(id, geom)" if has_id else "geom"
    file_out.parent.mkdir(parents=True, exist_ok=True)
    file_out.unlink(missing_ok=True)
    conn.execute(f"""--sql
        COPY (
            SELECT * EXCLUDE {exclude}, ST_SetCRS(geom, 'OGC:CRS84') AS geometry
            FROM "{table}"
            ORDER BY ST_Hilbert(geom, ST_Extent(ST_MakeEnvelope(-180, -90, 180, 90)))
        ) TO '{file_out}' {opts}
    """)


def main(conn: DuckDBPyConnection, land: str, wld: str) -> None:
    """Write Hilbert-sorted GeoParquet 2.0 to the catalog, one row group per polygon."""
    issues = []
    for geom, _, layer in EXPORT_LAYERS:
        table = f"{land}_{layer}_{wld}"
        has_id = geom != "lines"
        if geom == "voronoi":
            if wld == "intl":
                voronoi = output_dir / f"adm0/{land}/{wld}/adm0_voronoi.parquet"
                _copy_parquet(conn, table, voronoi, has_id=has_id, opts=PARQUET_OPTS)
            continue
        file_out = catalog_parquet(land, wld, geom)
        opts = POLYGON_OPTS if geom == "polygons" else PARQUET_OPTS
        _copy_parquet(conn, table, file_out, has_id=has_id, opts=opts)
        expected = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchall()[0][0]
        actual = conn.execute(
            f"SELECT COUNT(*) FROM read_parquet('{file_out}')"
        ).fetchall()[0][0]
        if actual != expected:
            issues.append(f"{file_out}: wrote {actual} rows, {table} has {expected}")
    raise_issues(issues, f"{land}_{wld}_parquet")
    logger.info("%s_%s", land, wld)
