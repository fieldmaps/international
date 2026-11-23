# 2. Dissolve crossed land with ST_CoverageUnion before clipping

Date: 2026-11-22

## Status

Accepted

## Context

`_03c_continents` takes the land polygons that boundary lines cross, and `_03d_intersection` clips voronoi polygons to them. World Polygons land arrives already split along borders, so one border crossing could be computed in several land pieces, each on its own, and `_03c` took about 29 minutes on usgs.

## Decision

Dissolve the crossed land polygons with `ST_CoverageUnion_Agg`, which treats the input as a coverage and merges shared edges without a general overlay. `_03c` records the crossed ids in `{land}_land_01_fids`, and `_03d` joins land outside that set to its voronoi polygon whole.

Measured on usgs:

| Metric | Value |
| --- | --- |
| Polygons in, out | 380, 40 |
| `ST_CoverageUnion_Agg` time | 28 s |
| `_03c` time | about 29 min before, about 2 min after |
| Output | valid, same total area |

## Consequences

Each border crossing is computed once, so neighbouring countries share it exactly. `_03c` fails if the dissolved result is invalid, because `ST_CoverageUnion` assumes a clean coverage. The dissolve did not fix the ARG/URY seam on its own, because that seam sits inside one land mass (see ADR 3).
