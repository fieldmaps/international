"""Load adm0 lines and points, plus LSIB lines."""

from logging import getLogger

from duckdb import DuckDBPyConnection

from app.config import tmp_dir
from app.utils import (
    copy_from_db,
    discard,
    edge_vertex_issues,
    raise_issues,
    read_input,
)

logger = getLogger(__name__)


def _near_miss_issues(conn: DuckDBPyConnection, lines: str) -> list[str]:
    """Return an issue for each line end within 1e-6 of another line but not on it."""
    rows = conn.execute(f"""--sql
        WITH l AS (SELECT row_number() OVER () AS rid, * FROM "{lines}"),
        e AS (
            SELECT rid, country1, country2,
                UNNEST([ST_StartPoint(g), ST_EndPoint(g)]) AS p
            FROM (
                SELECT rid, country1, country2, UNNEST(ST_Dump(geom)).geom AS g FROM l
            )
        )
        SELECT e.country1 || ' / ' || e.country2, o.country1 || ' / ' || o.country2,
            ST_Distance(e.p, o.geom) AS d
        FROM e JOIN l AS o ON o.rid <> e.rid AND ST_DWithin(e.p, o.geom, 1e-6)
        WHERE ST_Distance(e.p, o.geom) > 0
    """).fetchall()
    return [f"{a} ends {d:.1e} deg from {b} without touching it" for a, b, d in rows]


def main(conn: DuckDBPyConnection, land: str) -> None:
    """Load input lines and points, appending LSIB to lines."""
    prepare_db = tmp_dir / f"{land}__02_prepare.duckdb"
    for table in ["lsib_00", f"{land}_land_00", f"{land}_attributes_points"]:
        copy_from_db(conn, prepare_db, table)
    for layer, file in [
        (f"{land}_lines_00", "adm0_lines.parquet"),
        (f"{land}_points_00", "adm0_points.parquet"),
    ]:
        query = read_input(conn, file)
        conn.execute(f'CREATE OR REPLACE TABLE "{layer}" AS SELECT * FROM {query}')
    lines = f"{land}_lines_00"
    raise_issues(edge_vertex_issues(conn, f'"{lines}"', lines), f"{land}_inputs")
    cols = [r[0] for r in conn.execute(f'DESCRIBE "{land}_lines_00"').fetchall()]
    col_list = ", ".join(f'"{c}"' for c in cols)
    conn.execute(f'INSERT INTO "{land}_lines_00" SELECT {col_list} FROM lsib_00')
    raise_issues(_near_miss_issues(conn, lines), f"{land}_inputs")
    discard(conn, "lsib_00")
    logger.info("%s_inputs", land)
