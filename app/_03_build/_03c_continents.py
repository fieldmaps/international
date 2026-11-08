"""Identify land masses crossed by boundary lines."""

from logging import getLogger

from duckdb import DuckDBPyConnection

from app.utils import discard, raise_issues

logger = getLogger(__name__)


def _check_null_line_cuts(conn: DuckDBPyConnection, land: str) -> None:
    """Raise if a null-attribute line crosses into a land polygon."""
    seg = f"{land}_null_segments"
    pieces = f"{land}_null_pieces"
    conn.execute(f"""--sql
        CREATE OR REPLACE TEMP TABLE "{seg}" AS
        WITH parts AS (
            SELECT UNNEST(ST_Dump(ST_GeomFromWKB(ST_AsWKB(geom)))).geom AS g
            FROM "{land}_lines_00"
            WHERE CC1 IS NULL
        )
        SELECT ST_MakeLine(ST_PointN(g, i::INT), ST_PointN(g, i::INT + 1)) AS geom
        FROM (SELECT g, UNNEST(range(1, ST_NPoints(g))) AS i FROM parts)
    """)
    fids = conn.execute(f"""--sql
        SELECT DISTINCT a."FID"
        FROM "{land}_land_01" AS a
        JOIN "{seg}" AS s ON ST_Intersects(
            ST_Envelope(s.geom),
            ST_MakeEnvelope(
                ST_XMin(a.geom), ST_YMin(a.geom), ST_XMax(a.geom), ST_YMax(a.geom)
            )
        )
    """).fetchall()
    conn.execute(f'CREATE OR REPLACE TEMP TABLE "{pieces}" (fid BIGINT, geom GEOMETRY)')
    # One polygon per execute keeps GEOS memory bounded.
    for (fid,) in fids:
        conn.execute(
            f"""--sql
            INSERT INTO "{pieces}"
            SELECT $1, UNNEST(
                ST_Dump(ST_Subdivide(ST_GeomFromWKB(ST_AsWKB(geom)), 256))
            ).geom
            FROM "{land}_land_01"
            WHERE "FID" = $1
            """,
            [fid],
        )
    hits = conn.execute(f"""--sql
        SELECT fid, ST_X(pt), ST_Y(pt)
        FROM (
            SELECT p.fid, ST_PointOnSurface(
                ST_Collect(list(ST_Intersection(s.geom, p.geom)))
            ) AS pt
            FROM "{seg}" AS s
            JOIN "{pieces}" AS p ON ST_Intersects(s.geom, p.geom)
            WHERE NOT ST_Touches(s.geom, p.geom)
            GROUP BY p.fid
        )
        ORDER BY fid
    """).fetchall()
    conn.execute(f'DROP TABLE "{seg}"')
    conn.execute(f'DROP TABLE "{pieces}"')
    issues = [
        f"land FID {fid} crossed by null-attribute line near {x:.4f}, {y:.4f}"
        for fid, x, y in hits
    ]
    raise_issues(issues, f"{land}_null_line_cuts")


def main(conn: DuckDBPyConnection, land: str) -> None:
    """Identify land masses crossed by boundary lines."""
    table_in1 = f"{land}_land_00"
    table_in2 = f"{land}_lines_01"
    table_out = f"{land}_land_01"
    rows = conn.execute(f'SELECT ST_AsWKB(geom) FROM "{table_in2}"').fetchall()
    if not rows:
        return
    boundary_wkb = rows[0][0]
    conn.execute(
        f"""--sql
        CREATE OR REPLACE TABLE "{table_out}" AS
        SELECT *
        FROM "{table_in1}" AS a
        WHERE ST_Intersects(a.geom, ST_GeomFromWKB($1))
        """,
        [boundary_wkb],
    )
    _check_null_line_cuts(conn, land)
    discard(conn, table_in2)
    logger.info("%s_continents", land)
