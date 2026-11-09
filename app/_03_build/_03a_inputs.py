"""Load adm0 lines and points, plus LSIB lines."""

from logging import getLogger

from duckdb import DuckDBPyConnection

from app.config import input_dir, tmp_dir
from app.utils import copy_from_db, discard, edge_vertex_issues, raise_issues

logger = getLogger(__name__)


def main(conn: DuckDBPyConnection, land: str) -> None:
    """Load input lines and points, appending LSIB to lines."""
    prepare_db = tmp_dir / f"{land}__02_prepare.duckdb"
    for table in ["lsib_00", f"{land}_land_00", f"{land}_attributes_points"]:
        copy_from_db(conn, prepare_db, table)
    for layer, file in [
        (f"{land}_lines_00", "adm0_lines.parquet"),
        (f"{land}_points_00", "adm0_points.parquet"),
    ]:
        path = input_dir / file
        conn.execute(
            f"CREATE OR REPLACE TABLE \"{layer}\" AS SELECT * FROM read_parquet('{path}')"
        )
    lines = f"{land}_lines_00"
    raise_issues(edge_vertex_issues(conn, f'"{lines}"', lines), f"{land}_inputs")
    cols = [r[0] for r in conn.execute(f'DESCRIBE "{land}_lines_00"').fetchall()]
    col_list = ", ".join(f'"{c}"' for c in cols)
    conn.execute(f'INSERT INTO "{land}_lines_00" SELECT {col_list} FROM lsib_00')
    discard(conn, "lsib_00")
    logger.info("%s_inputs", land)
