"""Load CSV attribute tables into DuckDB."""

from logging import getLogger

import pandas as pd
from duckdb import DuckDBPyConnection

from app.config import bounds_of, gaul_input_dir, input_dir, land_of
from app.utils import get_source_date

logger = getLogger(__name__)

LINE_KEYS = ["country1", "country2", "rank", "label"]


def _register(conn: DuckDBPyConnection, table: str, df: pd.DataFrame) -> None:
    conn.register("df_temp", df)
    conn.execute(f'CREATE OR REPLACE TABLE "{table}" AS SELECT * FROM df_temp')
    conn.unregister("df_temp")


def _gaul(conn: DuckDBPyConnection, land: str) -> None:
    """Load GAUL unit and line attributes, shaped for the shared catalog steps."""
    land_date = get_source_date(land)
    gaul_date = get_source_date("gaul")
    points = pd.read_csv(gaul_input_dir / "adm0_attributes.csv", dtype={"id": str})
    points["adm0_id"] = points["id"] + "-" + land_date.replace("-", "")
    # Empty world-view mappings let _04b/_04c dissolve each unit onto itself.
    points["a_gaul"] = pd.Series(dtype="string")
    points["p_gaul"] = pd.Series(dtype="string")
    points["label_gaul"] = pd.Series(dtype="Int64")
    lines_path = gaul_input_dir / "adm0_lines.parquet"
    lines = pd.read_parquet(lines_path).drop(columns="geom")
    for df in (points, lines):
        df["wld_date"] = pd.to_datetime(gaul_date).date()
        df["wld_update"] = pd.to_datetime(land_date).date()
        df["wld_land"] = land_of(land)
    points["wld_view"] = pd.Series(dtype="string")
    points["wld_notes"] = pd.Series(dtype="string")
    _register(conn, f"{land}_attributes_points", points)
    _register(conn, f"{land}_attributes_lines", lines)
    logger.info("%s_attributes", land)


def main(conn: DuckDBPyConnection, land: str) -> None:
    """Load attribute tables for the given land source."""
    if bounds_of(land) == "gaul":
        _gaul(conn, land)
        return
    land_date = get_source_date(land)
    lsib_date = get_source_date("lsib")
    for geom in ["points", "lines"]:
        file = input_dir / f"adm0_{geom}.csv"
        df = pd.read_csv(file, keep_default_na=False, na_values=["", "#N/A"])
        if geom == "lines":
            dup = df[df.duplicated(LINE_KEYS, keep=False)]
            if not dup.empty:
                msg = f"{file.name}: duplicate join keys, adm_id {list(dup['adm_id'])}"
                raise ValueError(msg)
            iso3 = df.columns[df.columns.str.startswith("iso3_")]
            df[iso3] = df[iso3].astype("string")
        if geom == "points":
            df["adm0_src"] = df["id"]
            df["adm0_id"] = df["adm0_src"] + "-" + land_date.replace("-", "")
            df["iso_3_grp"] = df["iso_3_grp"].combine_first(df["iso_3"])
            df["region1_cd"] = df["region1_cd"].combine_first(df["status_cd"])
            df["region1_nm"] = df["region1_nm"].combine_first(df["status_nm"])
            df["region2_cd"] = df["region2_cd"].combine_first(df["region1_cd"])
            df["region2_nm"] = df["region2_nm"].combine_first(df["region1_nm"])
            df["region3_cd"] = df["region3_cd"].combine_first(df["region2_cd"])
            df["region3_nm"] = df["region3_nm"].combine_first(df["region2_nm"])
            for col in ["region1_cd", "region2_cd", "region3_cd"]:
                df[col] = df[col].astype("Int64")
        df["wld_date"] = pd.to_datetime(lsib_date).date()
        df["wld_update"] = pd.to_datetime(land_date).date()
        df["wld_land"] = land
        _register(conn, f"{land}_attributes_{geom}", df)
    logger.info("%s_attributes", land)
