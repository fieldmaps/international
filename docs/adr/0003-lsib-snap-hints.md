# 3. Snap LSIB to land vertices near configured hints

Date: 2026-11-22

## Status

Accepted

## Context

usgs failed the `_04e` coverage check (`ST_CoverageInvalidEdges` at 1e-8°) between Argentina and Uruguay at -58.2438, -34.1748, on an island in the Río de la Plata. Core LSIB and World Polygons both have a vertex where the border leaves the island's coast, but 7.1e-15° apart:

| Source | x | y |
| --- | --- | --- |
| LSIB | -58.243820731358049 | -34.174826087860041 |
| World Polygons | -58.243820731358056 | -34.174826087860041 |

Clipping found two crossing points and left a coastal stretch under 1e-8° between ARG and URY. The mismatch is between two inputs we don't control; our own extension lines aren't involved.

Rejected alternatives:

| Option | Result |
| --- | --- |
| Dissolve land first (ADR 2) | seam remains, it sits inside one land mass |
| Snap and node lines against the largest land mass | over 15 minutes, stopped |
| Scan every LSIB vertex against land worldwide | cost out of proportion to one vertex; the `_04e` coverage check already catches new cases |
| Snap an exact coordinate pair | breaks silently when either source moves the vertex |

## Decision

`LSIB_SNAP` in `app/config.py` lists hint locations per land source. In `_02a`, within `SNAP_WINDOW` (1e-3°) of each hint, LSIB lines are snapped with `ST_Snap` to the land vertices inside the window, at `SNAP_TOL` (1e-9°). The build logs a warning when a hint moves no line.

## Consequences

usgs passes coverage at 1e-8 without rounding: `usgs_polygons_00` has 339 rows, no invalid edges, and the same total area. Snapping happens in each land's own `lsib_00`, so osm is untouched, and lines, polygons and the full-LSIB line output all use the snapped vertex. A new mismatch shows up as a `_04e` coverage failure and gets a new hint. A hint that goes stale after a source update shows up as the warning.
