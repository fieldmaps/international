"""Load OSM land, USGS land, and LSIB files into DuckDB."""

import re
import sqlite3
from collections import defaultdict
from logging import getLogger
from pathlib import Path

from duckdb import DuckDBPyConnection

from app.config import download_dir, input_dir
from app.utils import get_source_date

logger = getLogger(__name__)


LSIB_KEYS = ["cc1", "country1", "cc2", "country2", "rank", "label", "status"]
SS_SD = "cc1='SS' AND cc2='SD' AND rank='1'"
CF_SS = "cc1='CF' AND cc2='SS' AND rank='1'"
ILEMI_ADMIN = "cc1='KE' AND cc2='SS' AND rank='2'"
ILEMI_PROV = "((cc1='KE' AND cc2='KE') OR (cc1='SS' AND cc2='SS')) AND rank='2'"
OTHER_LINE = "Other Line of International Separation"
OTHER_CC = {"KE": "SS", "SS": "KE"}
ILEMI_FACES = 3
SCOPUS_RING = "cc1='IL' AND cc2='XW' AND rank='2'"
SCOPUS_DMZ = "label='1949 Armistice Line DMZ'"
SCOPUS_INNER = [
    "cc1='IL' AND cc2='Q4' AND rank='2'",
    "cc1='XW' AND cc2='Q4' AND rank='3' AND label='1949 Armistice Line'",
]
SCOPUS_LABEL = "1949 Armistice Line Mount Scopus"


def _lsib(conn: DuckDBPyConnection, where: str, n: int) -> list[tuple]:
    """Return ``n`` LSIB rows of (*LSIB_KEYS, wkb) matching ``where``."""
    keys = ", ".join(LSIB_KEYS)
    rows = conn.execute(
        f"SELECT {keys}, ST_AsWKB(geom) FROM lsib_00 WHERE {where}"
    ).fetchall()
    if len(rows) != n:
        msg = f"expected {n} LSIB features for {where}, found {len(rows)}"
        raise ValueError(msg)
    return rows


def _faces(conn: DuckDBPyConnection, wkbs: list[bytes]) -> tuple[list, list]:
    """Node ``wkbs``; return its edges and enclosed face boundaries, west to east."""
    edges = [
        r[0]
        for r in conn.execute(
            "SELECT ST_AsWKB(UNNEST(ST_Dump(ST_Union_Agg(ST_GeomFromWKB(g)))).geom) "
            "FROM (SELECT UNNEST($1) AS g)",
            [wkbs],
        ).fetchall()
    ]
    faces = conn.execute(
        """--sql
        SELECT ST_AsWKB(ST_Boundary(f)) FROM (
            SELECT UNNEST(ST_Dump(ST_Polygonize(list(ST_GeomFromWKB(g))))).geom AS f
            FROM (SELECT UNNEST($1) AS g)
        ) ORDER BY ST_X(ST_Centroid(f))
        """,
        [edges],
    ).fetchall()
    return edges, [r[0] for r in faces]


def _shared(conn: DuckDBPyConnection, a: bytes, b: bytes) -> float:
    return conn.execute(
        "SELECT ST_Length(ST_Intersection(ST_GeomFromWKB($1), ST_GeomFromWKB($2)))",
        [a, b],
    ).fetchall()[0][0]


def _along(conn: DuckDBPyConnection, edge: bytes, other: bytes) -> bool:
    length = conn.execute("SELECT ST_Length(ST_GeomFromWKB($1))", [edge]).fetchall()[0][
        0
    ]
    return _shared(conn, edge, other) > 0.5 * length


def _replace(conn: DuckDBPyConnection, where: str, groups: dict) -> None:
    """Replace LSIB features matching ``where`` with merged edges keyed by LSIB_KEYS."""
    if not all(groups.values()):
        msg = f"empty LSIB piece among {list(groups)}"
        raise ValueError(msg)
    conn.execute(f"DELETE FROM lsib_00 WHERE {where}")
    cols = ", ".join(LSIB_KEYS)
    params = ", ".join("?" for _ in LSIB_KEYS)
    conn.executemany(
        f"""--sql
        INSERT INTO lsib_00 ({cols}, geom) VALUES ({params}, ST_Multi(ST_LineMerge(
            ST_Collect(list_transform(?, lambda x: ST_GeomFromWKB(x)))
        )))
        """,
        [(*key, edges) for key, edges in groups.items()],
    )


def _split_kafia_kingi(conn: DuckDBPyConnection) -> None:
    """Split LSIB SS-SD and CF-SS along the face the Sudan claim line closes."""
    claim = conn.execute(f"""--sql
        SELECT ST_AsWKB(geom) FROM read_parquet('{input_dir / "adm0_lines.parquet"}')
        WHERE COUNTRY2 = 'Kafia Kingi' AND LABEL = 'Sudan Claim'
    """).fetchall()[0][0]
    (ss_sd,), (cf_ss,) = _lsib(conn, SS_SD, 1), _lsib(conn, CF_SS, 1)
    edges, faces = _faces(conn, [ss_sd[-1], cf_ss[-1], claim])
    if len(faces) != 1:
        msg = f"expected one Kafia Kingi face, found {len(faces)}"
        raise ValueError(msg)
    groups = {}
    for line, q2 in [
        (ss_sd, ("SD", "SUDAN", "2", "South Sudan Claim", OTHER_LINE)),
        (cf_ss, ("CF", "CENTRAL AFRICAN REP.", "1", None, "International Boundary")),
    ]:
        own = [e for e in edges if _along(conn, e, line[-1])]
        on_face = [e for e in own if _along(conn, e, faces[0])]
        cc1, country1, rank, label, status = q2
        groups[(cc1, country1, "Q2", "Kafia Kingi", rank, label, status)] = on_face
        groups[line[:-1]] = [e for e in own if e not in on_face]
    _replace(conn, f"({SS_SD}) OR ({CF_SS})", groups)


def _split_ilemi(conn: DuckDBPyConnection) -> None:
    """Split the LSIB Ilemi lines into the three faces they enclose, west to east."""
    (admin,) = _lsib(conn, ILEMI_ADMIN, 1)
    provs = _lsib(conn, ILEMI_PROV, 2)
    names = dict([admin[:2], admin[2:4]])
    edges, faces = _faces(conn, [admin[-1], *(p[-1] for p in provs)])
    if len(faces) != ILEMI_FACES:
        msg = f"expected {ILEMI_FACES} Ilemi faces, found {len(faces)}"
        raise ValueError(msg)
    face_cc = []
    for face in faces:
        ccs = [p[0] for p in provs if _shared(conn, face, p[-1]) > 0]
        if len(ccs) != 1:
            msg = f"Ilemi face bordered by provisional lines {ccs}"
            raise ValueError(msg)
        face_cc.append(ccs[0])
    groups = defaultdict(list)
    for edge in edges:
        n = next((i for i, f in enumerate(faces) if _along(conn, edge, f)), None)
        prov = next((p for p in provs if _along(conn, edge, p[-1])), None)
        if n is None:
            if prov:
                msg = "Ilemi provisional edge outside every face"
                raise ValueError(msg)
            groups[admin[:-1]].append(edge)
            continue
        src = prov or admin
        cc1 = face_cc[n] if prov else OTHER_CC[face_cc[n]]
        key = (cc1, names[cc1], "Q2", f"Ilemi Triangle {n + 1}", "2", src[5], src[6])
        groups[key].append(edge)
    _replace(conn, f"({ILEMI_ADMIN}) OR ({ILEMI_PROV})", groups)


def _cut(conn: DuckDBPyConnection, line: bytes, face: bytes, fn: str) -> list:
    """Return the linestrings of ``fn(line, face)`` as WKB."""
    return [
        r[0]
        for r in conn.execute(
            f"""--sql
            SELECT ST_AsWKB(UNNEST(ST_Dump(ST_CollectionExtract(
                {fn}(ST_GeomFromWKB($1), ST_GeomFromWKB($2)), 2))).geom)
            """,
            [line, face],
        ).fetchall()
    ]


def _split_mount_scopus(conn: DuckDBPyConnection) -> None:
    """Relabel the LSIB lines on and inside the Mount Scopus enclave."""
    point = conn.execute(f"""--sql
        SELECT ST_AsWKB(geom) FROM read_parquet('{input_dir / "adm0_points.parquet"}')
        WHERE id = 'ISR_2'
    """).fetchall()[0][0]
    (ring_line,) = _lsib(conn, SCOPUS_RING, 1)
    rings = conn.execute(
        """--sql
        SELECT ST_AsWKB(g) FROM (
            SELECT UNNEST(ST_Dump(ST_GeomFromWKB($1))).geom AS g
        ) WHERE ST_Contains(ST_Envelope(g), ST_GeomFromWKB($2))
        """,
        [ring_line[-1], point],
    ).fetchall()
    if len(rings) != 1:
        msg = f"expected one Mount Scopus ring, found {len(rings)}"
        raise ValueError(msg)
    dmz = [r[-1] for r in _lsib(conn, SCOPUS_DMZ, 2)]
    _, faces = _faces(conn, [rings[0][0], *dmz])
    if len(faces) != 1:
        msg = f"expected one Mount Scopus face, found {len(faces)}"
        raise ValueError(msg)
    face = conn.execute(
        "SELECT ST_AsWKB(ST_MakePolygon(ST_GeomFromWKB($1)))", [faces[0]]
    ).fetchall()[0][0]
    lines = [ring_line] + [_lsib(conn, w, 1)[0] for w in SCOPUS_INNER]
    groups = {}
    for line in lines:
        cc1, country1, cc2, country2, rank, _, status = line[:-1]
        key = (cc1, country1, cc2, country2, rank, SCOPUS_LABEL, status)
        groups[key] = _cut(conn, line[-1], face, "ST_Intersection")
        groups[line[:-1]] = _cut(conn, line[-1], face, "ST_Difference")
    where = " OR ".join(f"({w})" for w in [SCOPUS_RING, *SCOPUS_INNER])
    _replace(conn, where, groups)


def _fix_lsib(conn: DuckDBPyConnection) -> None:
    """Split LSIB lines around South Sudan and Mount Scopus into their pieces."""
    _split_kafia_kingi(conn)
    _split_ilemi(conn)
    _split_mount_scopus(conn)


def _fix_land(conn: DuckDBPyConnection, land: str) -> None:
    """Add wld_update column to land table."""
    land_date = get_source_date(land)
    conn.execute(f"""--sql
        ALTER TABLE "{land}_land_00"
        ADD COLUMN IF NOT EXISTS wld_update DATE DEFAULT '{land_date}'
    """)


def _load_lsib(conn: DuckDBPyConnection) -> None:
    lsib_path = download_dir / "lsib/LSIB.gpkg"
    conn.execute(
        f"CREATE TABLE IF NOT EXISTS lsib_00 AS SELECT * FROM ST_Read('{lsib_path}')"
    )
    _fix_lsib(conn)


def _load_osm(conn: DuckDBPyConnection, table: str) -> None:
    osm_path = next((download_dir / "osm").rglob("land_polygons.shp"))
    conn.execute(
        f'CREATE OR REPLACE TABLE "{table}" AS '
        f"SELECT * EXCLUDE OGC_FID FROM ST_Read('{osm_path}')"
    )


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


def _load_usgs(conn: DuckDBPyConnection, table: str) -> None:
    # Caspian Sea is the only DoS_WP_* feature without a WP_Name; filter it out.
    usgs_path = next((download_dir / "usgs").rglob("WorldPolygons*.gpkg"))
    layer = _wp_layer(usgs_path)
    conn.execute(f"""--sql
        CREATE OR REPLACE TABLE "{table}" AS
        SELECT row_number() OVER () AS "FID", geom
        FROM (
            SELECT UNNEST(ST_Dump(geom)).geom AS geom
            FROM ST_Read('{usgs_path}', layer='{layer}')
            WHERE WP_Name IS NOT NULL
        )
    """)


def main(conn: DuckDBPyConnection, land: str) -> None:
    """Load LSIB plus the given land source into DuckDB."""
    _load_lsib(conn)
    loader = {"osm": _load_osm, "usgs": _load_usgs}[land]
    loader(conn, f"{land}_land_00")
    _fix_land(conn, land)
    logger.info("preprocessing %s", land)
