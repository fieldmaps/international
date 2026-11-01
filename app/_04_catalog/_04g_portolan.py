"""Build PMTiles with tylertoo and register each collection with portolan."""

from logging import getLogger

import tylertoo
from duckdb import DuckDBPyConnection

from app.config import args, catalog_dir, geoms, tmp_dir, views_of

from . import fieldmaps_publish
from ._04d_parquet import catalog_parquet

logger = getLogger(__name__)


def main(conn: DuckDBPyConnection, land: str) -> None:
    """Write PMTiles beside each catalog parquet, then `portolan add` its collection."""
    for wld in views_of(land):
        for geom in geoms:
            parquet = catalog_parquet(land, wld, geom)
            overview = tmp_dir / f"{land}/{wld}/adm0_{geom}_overview.parquet"
            overview.parent.mkdir(parents=True, exist_ok=True)
            # Every boundary at every zoom: only sub-pixel simplification drops detail.
            tylertoo.overview(
                str(parquet),
                str(overview),
                max_zoom=12,
                density_drop=False,
                line_thinning=0,
                line_visibility=0,
                polygon_thinning=0,
                polygon_visibility=0,
            )
            tylertoo.export_pmtiles(
                str(overview),
                str(parquet.with_suffix(".pmtiles")),
                layer_name=parquet.stem,
                tile_size_limit=None,
            )
            overview.unlink()
            wld_date = conn.execute(
                f"SELECT max(wld_date) FROM read_parquet('{parquet}')"
            ).fetchall()[0][0]
            fieldmaps_publish.run(
                catalog_dir,
                "add",
                str(parquet.parent.relative_to(catalog_dir)),
                "--pmtiles",
                "--datetime",
                wld_date.isoformat(),
            )
            logger.info("%s_%s_%s", land, wld, geom)


def finalize() -> None:
    """Validate the whole catalog, then push it to source.coop if --push is set."""
    fieldmaps_publish.run(catalog_dir, "check", "--data-scope", "local")
    logger.info("check")
    if args.push:
        fieldmaps_publish.push(catalog_dir, exclude=("*/gaul/*",))
