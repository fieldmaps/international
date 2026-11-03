"""Build final output lines table per world view."""

from logging import getLogger

from duckdb import DuckDBPyConnection

from app.config import GEOM_PRECISION, bounds_of, world_views

logger = getLogger(__name__)


def sides(conn: DuckDBPyConnection, land: str) -> None:
    """Split ``lines_00`` rows into pieces by the voronoi faces on each side."""
    lines_00 = f"{land}_lines_00"
    faces = f"{land}_lines_faces"
    out = f"{land}_lines_sides"
    conn.execute(f"""--sql
        CREATE OR REPLACE TEMP TABLE "{faces}" AS
        WITH v AS (
            SELECT id, ST_GeomFromWKB(ST_AsWKB(geom)) AS geom
            FROM "{land}_voronoi_00"
        )
        SELECT id, geom, ST_Boundary(geom) AS boundary FROM v
    """)
    conn.execute(f"""--sql
        CREATE OR REPLACE TABLE "{out}" (
            country1 VARCHAR, country2 VARCHAR, rank BIGINT, label VARCHAR,
            face_a VARCHAR, face_b VARCHAR, geom GEOMETRY
        )
    """)
    rows = conn.execute(f"""--sql
        SELECT country1, country2, rank, label, ST_AsWKB(geom) FROM "{lines_00}"
    """).fetchall()
    # Per-row WKB param, see _clip. Dangles go to the face they lie in.
    for *vals, wkb in rows:
        conn.execute(
            f"""--sql
            INSERT INTO "{out}"
            WITH line AS (SELECT ST_GeomFromWKB($5) AS g),
            cand AS (
                SELECT f.id, f.geom, ST_CollectionExtract(
                    ST_Intersection(f.boundary, line.g), 2) AS piece
                FROM "{faces}" AS f, line
                WHERE ST_Intersects(f.boundary, line.g)
            ),
            pairs AS (
                SELECT a.id AS face_a, b.id AS face_b, ST_CollectionExtract(
                    ST_Intersection(a.piece, b.piece), 2) AS geom
                FROM cand AS a JOIN cand AS b
                ON a.id < b.id AND ST_Intersects(a.piece, b.piece)
            ),
            rest AS (
                SELECT ST_CollectionExtract(ST_Difference(line.g, COALESCE(
                    (SELECT ST_Union_Agg(geom) FROM pairs WHERE NOT ST_IsEmpty(geom)),
                    'GEOMETRYCOLLECTION EMPTY'::GEOMETRY)), 2) AS g
                FROM line
            ),
            dangles AS (
                SELECT c.id AS face_a, c.id AS face_b, ST_CollectionExtract(
                    ST_Intersection(c.geom, rest.g), 2) AS geom
                FROM cand AS c, rest
                WHERE NOT ST_IsEmpty(rest.g) AND ST_Intersects(c.geom, rest.g)
            )
            SELECT $1, $2, $3, $4, face_a, face_b, ST_LineMerge(geom)
            FROM (SELECT * FROM pairs UNION ALL SELECT * FROM dangles)
            WHERE NOT ST_IsEmpty(geom)
            """,
            [*vals, wkb],
        )
    conn.execute(f'DROP TABLE IF EXISTS "{faces}"')
    logger.info(out)


def _sided(conn: DuckDBPyConnection, land: str, world: str) -> str:
    """Code each side piece with the ISO3 of the world-view unit on each side."""
    out = f"{land}_lines_sided"
    conn.execute(f"""--sql
        CREATE OR REPLACE TABLE "{out}" AS
        WITH unit AS (
            SELECT DISTINCT v.id, p.iso_3
            FROM "{land}_voronoi_00" AS v
            JOIN "{land}_attributes_points" AS p ON p.id = COALESCE(v."a_{world}", v.id)
        ),
        coded AS (
            SELECT s.country1, s.country2, s.rank, s.label,
                   LEAST(a.iso_3, b.iso_3) AS lo,
                   GREATEST(a.iso_3, b.iso_3) AS hi,
                   s.geom
            FROM "{land}_lines_sides" AS s
            JOIN unit AS a ON a.id = s.face_a
            JOIN unit AS b ON b.id = s.face_b
        )
        SELECT country1, country2, rank, label, lo AS iso3_1,
               CASE WHEN lo != hi THEN hi END AS iso3_2, ST_Union_Agg(geom) AS geom
        FROM coded
        GROUP BY ALL
    """)
    return out


def _split(conn: DuckDBPyConnection, sided: str, tmp: str, out: str) -> None:
    """Split clipped lines in ``tmp`` by the coded side pieces in ``sided``."""
    keys = "country1, country2, rank, label"
    on = """t.country1 = s.country1 AND t.country2 = s.country2
        AND t.rank = s.rank AND t.label IS NOT DISTINCT FROM s.label"""
    conn.execute(f"""--sql
        CREATE OR REPLACE TEMP TABLE "{sided}_k" AS
        SELECT *, COUNT(*) OVER (PARTITION BY {keys}) AS k FROM "{sided}"
    """)
    conn.execute(f"""--sql
        CREATE OR REPLACE TABLE "{out}" AS
        SELECT t.country1, t.country2, t.rank, t.label, s.iso3_1, s.iso3_2, t.geom
        FROM "{tmp}" AS t LEFT JOIN "{sided}_k" AS s ON {on}
        WHERE COALESCE(s.k, 1) = 1
    """)
    rows = conn.execute(f"""--sql
        SELECT DISTINCT t.country1, t.country2, t.rank, t.label, ST_AsWKB(t.geom)
        FROM "{tmp}" AS t JOIN "{sided}_k" AS s ON {on}
        WHERE s.k > 1
    """).fetchall()
    # Exact pieces first; coast cuts leave bits off the source segments, which go
    # to the nearest piece.
    for *vals, wkb in rows:
        conn.execute(
            f"""--sql
            INSERT INTO "{out}"
            WITH t AS (SELECT ST_GeomFromWKB($5) AS g),
            p AS (
                SELECT iso3_1, iso3_2, geom FROM "{sided}"
                WHERE country1 = $1 AND country2 = $2 AND rank = $3
                AND label IS NOT DISTINCT FROM $4
            ),
            x AS (
                SELECT iso3_1, iso3_2,
                       ST_CollectionExtract(ST_Intersection(t.g, p.geom), 2) AS g
                FROM p, t
            ),
            r AS (
                SELECT UNNEST(ST_Dump(ST_CollectionExtract(ST_Difference(t.g,
                    COALESCE((SELECT ST_Union_Agg(g) FROM x WHERE NOT ST_IsEmpty(g)),
                    'GEOMETRYCOLLECTION EMPTY'::GEOMETRY)), 2))).geom AS g
                FROM t
            ),
            rn AS (SELECT row_number() OVER () AS rid, g FROM r),
            ra AS (
                SELECT p.iso3_1, p.iso3_2, rn.g
                FROM rn, p
                QUALIFY row_number() OVER (
                    PARTITION BY rn.rid ORDER BY ST_Distance(rn.g, p.geom)
                ) = 1
            )
            SELECT $1, $2, $3, $4, iso3_1, iso3_2, g
            FROM (SELECT * FROM x UNION ALL SELECT * FROM ra)
            WHERE NOT ST_IsEmpty(g)
            """,
            [*vals, wkb],
        )
    conn.execute(f'DROP TABLE IF EXISTS "{sided}_k"')


def _clip(conn: DuckDBPyConnection, land: str, keys: list[str], tmp: str) -> None:
    """Clip each boundary line to each crossed land mass into ``tmp``."""
    lines_00 = f"{land}_lines_00"
    land_01 = f"{land}_land_01"
    key_list = ", ".join(f'"{k}"' for k in keys)
    params = ", ".join(f"${i}" for i in range(1, len(keys) + 1))
    n = len(keys)
    conn.execute(f"""--sql
        CREATE OR REPLACE TABLE "{tmp}" AS
        SELECT {key_list}, geom FROM "{lines_00}" LIMIT 0
    """)

    # Per-pair INSERT: vectorizing ST_Intersection across all lines for a
    # continent-size land polygon balloons GEOS memory (each chunk x thread
    # deserializes the bound polygon WKB). Bbox-prefilter candidates with
    # ST_Envelope (5-point rect) then loop one (line, land) pair per execute,
    # bound as WKB. Same pattern as _03d_intersection.py.
    fids = [
        r[0] for r in conn.execute(f'SELECT DISTINCT "FID" FROM "{land_01}"').fetchall()
    ]
    for fid in fids:
        land_wkb = conn.execute(
            f'SELECT ST_AsWKB(geom) FROM "{land_01}" WHERE "FID" = $1', [fid]
        ).fetchall()[0][0]
        env_wkb = conn.execute(
            f'SELECT ST_AsWKB(ST_Envelope(geom)) FROM "{land_01}" WHERE "FID" = $1',
            [fid],
        ).fetchall()[0][0]
        candidates = conn.execute(
            f"""--sql
            SELECT {key_list}, ST_AsWKB(geom)
            FROM "{lines_00}"
            WHERE ST_Intersects(ST_Envelope(geom), ST_GeomFromWKB($1))
            """,
            [env_wkb],
        ).fetchall()
        for *vals, line_wkb in candidates:
            conn.execute(
                f"""--sql
                INSERT INTO "{tmp}"
                SELECT {params},
                       ST_CollectionExtract(
                           ST_Intersection(
                               ST_GeomFromWKB(${n + 1}),
                               ST_GeomFromWKB(${n + 2})
                           ), 2
                       )
                """,
                [*vals, line_wkb, land_wkb],
            )


def _dissolve_sql(keys: list[str], tmp: str) -> str:
    key_list = ", ".join(f'"{k}"' for k in keys)
    return f"""--sql
        SELECT
            {key_list},
            ST_Multi(
                ST_ReducePrecision(ST_Union_Agg(geom), {GEOM_PRECISION})
            ) AS geom
        FROM "{tmp}"
        WHERE NOT ST_IsEmpty(geom)
        GROUP BY {key_list}
    """


def _gaul(conn: DuckDBPyConnection, land: str, world: str) -> None:
    """Clip GAUL lines to land, dropping world-frame closure segments."""
    attrs = f"{land}_attributes_lines"
    out = f"{land}_lines_02_{world}"
    tmp = f"{land}_lines_clipped"
    _clip(conn, land, ["adm_id"], tmp)
    conn.execute(f"""--sql
        CREATE OR REPLACE TABLE "{out}" AS
        WITH clipped AS ({_dissolve_sql(["adm_id"], tmp)})
        SELECT b.*, '{world}' AS wld_view, a.geom
        FROM clipped AS a
        JOIN "{attrs}" AS b ON a.adm_id = b.adm_id
        WHERE b.kind != 'frame'
    """)
    conn.execute(f'DROP TABLE IF EXISTS "{tmp}"')


def _merge(conn: DuckDBPyConnection, land: str, world: str) -> None:
    """Give lines with ``adm_id_{world}`` set that row's attributes, merged into it."""
    attrs = f"{land}_attributes_lines"
    out = f"{land}_lines_02_{world}"
    pairs = conn.execute(f"""--sql
        SELECT adm_id, "adm_id_{world}"::INTEGER FROM "{out}"
        WHERE "adm_id_{world}" IS NOT NULL
    """).fetchall()
    if not pairs:
        return
    ids = sorted({i for pair in pairs for i in pair})
    conn.execute(
        f"""--sql
        CREATE OR REPLACE TABLE "{out}" AS
        SELECT * FROM "{out}" WHERE NOT COALESCE(list_contains($1, adm_id), false)
        UNION ALL BY NAME
        SELECT
            b.*,
            o.iso3_1,
            o.iso3_2,
            '{world}' AS wld_view,
            ST_Multi(
                ST_ReducePrecision(ST_Union_Agg(o.geom), {GEOM_PRECISION})
            ) AS geom
        FROM "{out}" AS o
        JOIN "{attrs}" AS b
        ON b.adm_id = COALESCE(o."adm_id_{world}"::INTEGER, o.adm_id)
        WHERE list_contains($1, o.adm_id)
        GROUP BY ALL
        """,
        [ids],
    )


def main(conn: DuckDBPyConnection, land: str, world: str) -> None:
    """Clip boundary lines to land and filter by world view."""
    if bounds_of(land) == "gaul":
        _gaul(conn, land, world)
        logger.info("%s_%s", land, world)
        return
    attrs = f"{land}_attributes_lines"
    out = f"{land}_lines_02_{world}"
    tmp = f"{land}_lines_clipped"
    keys = ["country1", "country2", "rank", "label"]
    split = f"{land}_lines_split"
    _clip(conn, land, keys, tmp)
    sided = _sided(conn, land, world)
    _split(conn, sided, tmp, split)
    for table in [sided, tmp]:
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')

    conn.execute(f"""--sql
        CREATE OR REPLACE TABLE "{out}" AS
        WITH clipped AS ({_dissolve_sql([*keys, "iso3_1", "iso3_2"], split)})
        SELECT
            b.*,
            COALESCE(b."iso3_1_{world}", a.iso3_1) AS iso3_1,
            COALESCE(b."iso3_2_{world}", a.iso3_2) AS iso3_2,
            '{world}' AS wld_view,
            a.geom
        FROM clipped AS a
        LEFT JOIN "{attrs}" AS b
        ON a.country1 = b.country1
        AND a.country2 = b.country2
        AND a.rank = b.rank
        AND a.label IS NOT DISTINCT FROM b.label
        WHERE COALESCE("rank_{world}", a.rank) > 0
    """)

    conn.execute(f'DROP TABLE IF EXISTS "{split}"')
    over = f'COALESCE("iso3_1_{world}", "iso3_2_{world}") IS NOT NULL'
    conn.execute(f"""--sql
        CREATE OR REPLACE TABLE "{out}" AS
        SELECT * EXCLUDE (geom),
            ST_Multi(ST_ReducePrecision(ST_Union_Agg(geom), {GEOM_PRECISION})) AS geom
        FROM "{out}" WHERE {over} GROUP BY ALL
        UNION ALL BY NAME
        SELECT * FROM "{out}" WHERE NOT ({over})
    """)
    _merge(conn, land, world)

    # line_type overrides in the CSV; otherwise derived from LSIB status.
    conn.execute(f"""--sql
        UPDATE "{out}"
        SET rank = COALESCE("rank_{world}", rank),
            line_type = COALESCE(line_type, CASE status
                WHEN 'International Boundary' THEN 'international'
                WHEN 'Other Line of International Separation' THEN 'armistice'
                WHEN 'Special Line' THEN 'special'
            END)
    """)

    for wld in world_views:
        for col in [f"rank_{wld}", f"adm_id_{wld}", f"iso3_1_{wld}", f"iso3_2_{wld}"]:
            conn.execute(f'ALTER TABLE "{out}" DROP COLUMN IF EXISTS "{col}"')
    head = "adm_id, cc1, country1, cc2, country2, iso3_1, iso3_2"
    conn.execute(f"""--sql
        CREATE OR REPLACE TABLE "{out}" AS
        SELECT {head}, * EXCLUDE ({head}) FROM "{out}"
    """)

    logger.info("%s_%s", land, world)
