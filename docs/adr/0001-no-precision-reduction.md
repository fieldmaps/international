# 1. No coordinate precision reduction

Date: 2026-11-22

## Status

Accepted

## Context

Final outputs were rounded once with `ST_ReducePrecision` at 1e-9°. `ST_ReducePrecision` rounds each polygon on its own. On usgs, Equatorial Guinea has a near-duplicate source vertex 1.1e-11° from its neighbour on the Cameroon border. Rounding collapsed it in the GNQ polygon, while the CMR polygon kept its vertex and rounded it to the next grid cell. The shared edge split, and `ST_CoverageInvalidEdges` at a 1e-8° gap width flagged it.

Measured alternatives:

| Option | Result |
| --- | --- |
| `ST_CoverageClean` after rounding, 323 usgs polygons, 78M vertices | over 83 minutes on one core, stopped without finishing, per world view |
| Rounding pair tests (topo-tools) | fixed none of the overlay seams |
| Rounding for size (topo-tools, ZSTD, GeoParquet V2) | no gain in file size or vertex count |

The OpenFileGDB export snaps vertices to its own 1e-9 grid, so rounding adds nothing there either.

## Decision

Keep geometry at source precision through every step and every export format. `_04e_validate` checks the unrounded final tables: `ST_IsValid`, plus `ST_CoverageInvalidEdges` at gap width `XY_TOLERANCE` (1e-8°, Esri's default lat/lon XY tolerance).

## Consequences

Neighbouring polygons share edges exactly as clipped. Seams that come from the inputs themselves are fixed at their source (see ADR 3). Coordinates keep full double precision, at no measured cost in size.
