from __future__ import annotations

from math import hypot
from typing import Any


ISO_GEO_PRESETS: dict[str, dict[str, Any]] = {
    "ERCOT": {
        "theme": {"bg": "#081f2c", "glow": "#47c0ff", "accent": "#ff9a3d"},
        "metros": [
            {"id": "m1", "name": "Dallas-Fort Worth", "x": 53, "y": 20, "base_pop": 7.7},
            {"id": "m2", "name": "Houston", "x": 69, "y": 56, "base_pop": 7.2},
            {"id": "m3", "name": "Austin", "x": 51, "y": 52, "base_pop": 2.4},
            {"id": "m4", "name": "San Antonio", "x": 46, "y": 61, "base_pop": 2.6},
            {"id": "m5", "name": "Permian Basin", "x": 21, "y": 41, "base_pop": 0.9},
        ],
        "substations": [
            {"id": "s1", "name": "North Hub 500", "x": 51, "y": 24, "kv": 500},
            {"id": "s2", "name": "Coastal Bend 345", "x": 73, "y": 53, "kv": 345},
            {"id": "s3", "name": "Hill Country 345", "x": 49, "y": 55, "kv": 345},
            {"id": "s4", "name": "South Gate 345", "x": 45, "y": 66, "kv": 345},
            {"id": "s5", "name": "West Export 345", "x": 27, "y": 40, "kv": 345},
            {"id": "s6", "name": "Panhandle Tie 230", "x": 37, "y": 11, "kv": 230},
        ],
        "generation": [
            {"id": "g1", "name": "Panhandle Wind Arc", "x": 32, "y": 8, "technology": "Wind"},
            {"id": "g2", "name": "Permian Solar Belt", "x": 18, "y": 44, "technology": "Solar"},
            {"id": "g3", "name": "Coastal Storage Cluster", "x": 77, "y": 57, "technology": "Storage"},
            {"id": "g4", "name": "South Texas Hybrid", "x": 39, "y": 74, "technology": "Hybrid"},
        ],
        "zones": [
            {"id": "z1", "name": "North Growth Belt", "points": [(35, 7), (64, 8), (71, 24), (44, 33), (31, 20)]},
            {"id": "z2", "name": "I-35 Spine", "points": [(38, 38), (56, 33), (63, 62), (39, 72), (31, 55)]},
            {"id": "z3", "name": "Coastal Load Shelf", "points": [(60, 40), (82, 42), (90, 64), (65, 72), (54, 56)]},
        ],
        "lines": [
            ("s6", "s1"),
            ("s1", "s3"),
            ("s3", "s4"),
            ("s3", "s2"),
            ("s5", "s3"),
            ("s5", "s1"),
            ("s1", "s2"),
        ],
    },
    "CAISO": {
        "theme": {"bg": "#13212b", "glow": "#6ae3a8", "accent": "#ffd166"},
        "metros": [
            {"id": "m1", "name": "Bay Area", "x": 31, "y": 25, "base_pop": 7.8},
            {"id": "m2", "name": "Los Angeles Basin", "x": 47, "y": 69, "base_pop": 12.8},
            {"id": "m3", "name": "San Diego", "x": 51, "y": 84, "base_pop": 3.3},
            {"id": "m4", "name": "Central Valley", "x": 39, "y": 47, "base_pop": 2.7},
            {"id": "m5", "name": "Inland Empire", "x": 56, "y": 63, "base_pop": 4.6},
        ],
        "substations": [
            {"id": "s1", "name": "Path 15 North", "x": 34, "y": 36, "kv": 500},
            {"id": "s2", "name": "Path 15 South", "x": 41, "y": 52, "kv": 500},
            {"id": "s3", "name": "Tehachapi 500", "x": 48, "y": 58, "kv": 500},
            {"id": "s4", "name": "LA Pocket 345", "x": 45, "y": 68, "kv": 345},
            {"id": "s5", "name": "Desert Sun 500", "x": 64, "y": 57, "kv": 500},
            {"id": "s6", "name": "South Border 230", "x": 52, "y": 81, "kv": 230},
        ],
        "generation": [
            {"id": "g1", "name": "Tehachapi Wind", "x": 44, "y": 55, "technology": "Wind"},
            {"id": "g2", "name": "Imperial Solar", "x": 73, "y": 67, "technology": "Solar"},
            {"id": "g3", "name": "Bay Storage Ring", "x": 28, "y": 22, "technology": "Storage"},
            {"id": "g4", "name": "Delta Hydro Flex", "x": 34, "y": 39, "technology": "Hydro"},
        ],
        "zones": [
            {"id": "z1", "name": "Northern Coast Demand", "points": [(21, 14), (38, 15), (43, 28), (30, 38), (19, 26)]},
            {"id": "z2", "name": "Central Transfer Corridor", "points": [(28, 31), (47, 35), (52, 58), (33, 60), (24, 45)]},
            {"id": "z3", "name": "South Basin Load Pocket", "points": [(38, 58), (61, 54), (68, 79), (44, 86), (34, 73)]},
        ],
        "lines": [
            ("s1", "s2"),
            ("s2", "s3"),
            ("s3", "s4"),
            ("s3", "s5"),
            ("s4", "s6"),
            ("s2", "s4"),
            ("s1", "s3"),
        ],
    },
    "PJM": {
        "theme": {"bg": "#1e1c2b", "glow": "#8f9cff", "accent": "#ff7f50"},
        "metros": [
            {"id": "m1", "name": "Chicago Fringe", "x": 13, "y": 22, "base_pop": 2.1},
            {"id": "m2", "name": "Pittsburgh", "x": 41, "y": 31, "base_pop": 2.3},
            {"id": "m3", "name": "Philadelphia", "x": 73, "y": 32, "base_pop": 6.2},
            {"id": "m4", "name": "Baltimore-Washington", "x": 72, "y": 49, "base_pop": 9.8},
            {"id": "m5", "name": "Columbus", "x": 35, "y": 45, "base_pop": 2.2},
        ],
        "substations": [
            {"id": "s1", "name": "ComEd Seam 345", "x": 21, "y": 25, "kv": 345},
            {"id": "s2", "name": "Allegheny 500", "x": 42, "y": 31, "kv": 500},
            {"id": "s3", "name": "Susquehanna 500", "x": 58, "y": 28, "kv": 500},
            {"id": "s4", "name": "Mid-Atlantic 500", "x": 70, "y": 42, "kv": 500},
            {"id": "s5", "name": "Ohio Valley 345", "x": 36, "y": 49, "kv": 345},
            {"id": "s6", "name": "Dominion South 345", "x": 56, "y": 54, "kv": 345},
        ],
        "generation": [
            {"id": "g1", "name": "Appalachian Wind", "x": 52, "y": 58, "technology": "Wind"},
            {"id": "g2", "name": "Mid-Atlantic Solar", "x": 66, "y": 45, "technology": "Solar"},
            {"id": "g3", "name": "PJM Storage Spine", "x": 48, "y": 34, "technology": "Storage"},
            {"id": "g4", "name": "Lake Erie Nuclear", "x": 27, "y": 15, "technology": "Nuclear"},
        ],
        "zones": [
            {"id": "z1", "name": "Great Lakes Interface", "points": [(7, 11), (27, 12), (36, 28), (18, 35), (8, 26)]},
            {"id": "z2", "name": "Allegheny Transfer Arc", "points": [(25, 24), (54, 19), (62, 39), (30, 51), (20, 39)]},
            {"id": "z3", "name": "Atlantic Load Crescent", "points": [(55, 17), (84, 23), (84, 58), (59, 63), (49, 37)]},
        ],
        "lines": [
            ("s1", "s2"),
            ("s2", "s3"),
            ("s3", "s4"),
            ("s2", "s5"),
            ("s5", "s6"),
            ("s6", "s4"),
            ("s3", "s6"),
        ],
    },
    "MISO": {
        "theme": {"bg": "#101e27", "glow": "#7bdff6", "accent": "#f4d35e"},
        "metros": [
            {"id": "m1", "name": "Twin Cities", "x": 39, "y": 13, "base_pop": 3.7},
            {"id": "m2", "name": "Chicago", "x": 55, "y": 43, "base_pop": 9.4},
            {"id": "m3", "name": "St. Louis", "x": 48, "y": 58, "base_pop": 2.8},
            {"id": "m4", "name": "Lower Mississippi", "x": 58, "y": 83, "base_pop": 1.9},
            {"id": "m5", "name": "Upper Plains", "x": 22, "y": 24, "base_pop": 1.4},
        ],
        "substations": [
            {"id": "s1", "name": "Prairie Core 345", "x": 33, "y": 21, "kv": 345},
            {"id": "s2", "name": "Upper Midwest 500", "x": 44, "y": 28, "kv": 500},
            {"id": "s3", "name": "Illinois Hub 500", "x": 53, "y": 44, "kv": 500},
            {"id": "s4", "name": "Mid-South 345", "x": 54, "y": 67, "kv": 345},
            {"id": "s5", "name": "Delta Transfer 345", "x": 58, "y": 83, "kv": 345},
            {"id": "s6", "name": "Dakota Export 230", "x": 18, "y": 14, "kv": 230},
        ],
        "generation": [
            {"id": "g1", "name": "Dakota Wind Belt", "x": 12, "y": 12, "technology": "Wind"},
            {"id": "g2", "name": "Illinois Storage", "x": 56, "y": 46, "technology": "Storage"},
            {"id": "g3", "name": "Mississippi Solar", "x": 66, "y": 81, "technology": "Solar"},
            {"id": "g4", "name": "River Nuclear Fleet", "x": 46, "y": 61, "technology": "Nuclear"},
        ],
        "zones": [
            {"id": "z1", "name": "Northern Wind Export", "points": [(8, 6), (39, 9), (47, 26), (23, 35), (6, 19)]},
            {"id": "z2", "name": "Chicago-St. Louis Core", "points": [(36, 31), (64, 35), (61, 60), (40, 66), (31, 50)]},
            {"id": "z3", "name": "Lower Valley Reserve", "points": [(44, 58), (70, 65), (76, 92), (51, 94), (40, 78)]},
        ],
        "lines": [
            ("s6", "s1"),
            ("s1", "s2"),
            ("s2", "s3"),
            ("s3", "s4"),
            ("s4", "s5"),
            ("s2", "s4"),
            ("s1", "s3"),
        ],
    },
    "SPP": {
        "theme": {"bg": "#17202a", "glow": "#54e1b5", "accent": "#ffb703"},
        "metros": [
            {"id": "m1", "name": "Kansas City", "x": 56, "y": 18, "base_pop": 2.4},
            {"id": "m2", "name": "Oklahoma City", "x": 58, "y": 45, "base_pop": 1.5},
            {"id": "m3", "name": "Tulsa", "x": 67, "y": 35, "base_pop": 1.0},
            {"id": "m4", "name": "Omaha", "x": 49, "y": 8, "base_pop": 0.9},
            {"id": "m5", "name": "West Plains", "x": 22, "y": 30, "base_pop": 0.6},
        ],
        "substations": [
            {"id": "s1", "name": "Nebraska Spine 345", "x": 48, "y": 12, "kv": 345},
            {"id": "s2", "name": "Central Plains 500", "x": 50, "y": 24, "kv": 500},
            {"id": "s3", "name": "Flint Hills 345", "x": 60, "y": 26, "kv": 345},
            {"id": "s4", "name": "Oklahoma Core 345", "x": 57, "y": 43, "kv": 345},
            {"id": "s5", "name": "Arkansas Seam 345", "x": 72, "y": 44, "kv": 345},
            {"id": "s6", "name": "Panhandle Export 345", "x": 23, "y": 35, "kv": 345},
        ],
        "generation": [
            {"id": "g1", "name": "High Plains Wind", "x": 14, "y": 28, "technology": "Wind"},
            {"id": "g2", "name": "Panhandle Solar", "x": 22, "y": 49, "technology": "Solar"},
            {"id": "g3", "name": "Tulsa Storage", "x": 67, "y": 34, "technology": "Storage"},
            {"id": "g4", "name": "Kansas Peaker Flex", "x": 56, "y": 15, "technology": "Gas"},
        ],
        "zones": [
            {"id": "z1", "name": "Wind Export Prairie", "points": [(7, 14), (37, 12), (42, 33), (14, 41), (4, 29)]},
            {"id": "z2", "name": "I-35 Reliability Spine", "points": [(40, 10), (70, 13), (73, 49), (47, 54), (37, 29)]},
            {"id": "z3", "name": "Ozark Load Edge", "points": [(60, 24), (84, 28), (84, 59), (65, 60), (57, 40)]},
        ],
        "lines": [
            ("s1", "s2"),
            ("s2", "s3"),
            ("s3", "s4"),
            ("s4", "s5"),
            ("s2", "s6"),
            ("s6", "s4"),
            ("s3", "s5"),
        ],
    },
    "NYISO": {
        "theme": {"bg": "#0f172a", "glow": "#60a5fa", "accent": "#f59e0b"},
        "metros": [
            {"id": "m1", "name": "Buffalo", "x": 14, "y": 30, "base_pop": 1.1},
            {"id": "m2", "name": "Rochester-Syracuse", "x": 31, "y": 27, "base_pop": 1.7},
            {"id": "m3", "name": "Albany Capital Region", "x": 48, "y": 33, "base_pop": 1.2},
            {"id": "m4", "name": "Hudson Valley", "x": 65, "y": 46, "base_pop": 2.1},
            {"id": "m5", "name": "New York City", "x": 79, "y": 60, "base_pop": 19.3},
        ],
        "substations": [
            {"id": "s1", "name": "West Upstate 345", "x": 19, "y": 30, "kv": 345},
            {"id": "s2", "name": "Moses 500", "x": 38, "y": 29, "kv": 500},
            {"id": "s3", "name": "Capital East 345", "x": 51, "y": 34, "kv": 345},
            {"id": "s4", "name": "Hudson North 500", "x": 63, "y": 46, "kv": 500},
            {"id": "s5", "name": "Downstate Pocket 345", "x": 75, "y": 58, "kv": 345},
            {"id": "s6", "name": "Long Island Tie 230", "x": 88, "y": 63, "kv": 230},
        ],
        "generation": [
            {"id": "g1", "name": "Niagara Hydro", "x": 8, "y": 26, "technology": "Hydro"},
            {"id": "g2", "name": "North Country Wind", "x": 35, "y": 12, "technology": "Wind"},
            {"id": "g3", "name": "Hudson Solar", "x": 61, "y": 43, "technology": "Solar"},
            {"id": "g4", "name": "Downstate Storage", "x": 80, "y": 55, "technology": "Storage"},
        ],
        "zones": [
            {"id": "z1", "name": "Upstate Hydro-Wind Shelf", "points": [(4, 12), (43, 9), (49, 31), (17, 40), (2, 28)]},
            {"id": "z2", "name": "Hudson Transfer Constraint", "points": [(40, 22), (64, 26), (71, 49), (52, 58), (37, 42)]},
            {"id": "z3", "name": "Downstate Demand Pocket", "points": [(63, 42), (92, 48), (95, 69), (75, 76), (58, 59)]},
        ],
        "lines": [
            ("s1", "s2"),
            ("s2", "s3"),
            ("s3", "s4"),
            ("s4", "s5"),
            ("s5", "s6"),
            ("s3", "s5"),
            ("s2", "s4"),
        ],
    },
    "ISO-NE": {
        "theme": {"bg": "#111827", "glow": "#34d399", "accent": "#f97316"},
        "metros": [
            {"id": "m1", "name": "Boston", "x": 67, "y": 33, "base_pop": 4.9},
            {"id": "m2", "name": "Providence", "x": 70, "y": 42, "base_pop": 1.7},
            {"id": "m3", "name": "Hartford", "x": 59, "y": 49, "base_pop": 1.2},
            {"id": "m4", "name": "Maine Coast", "x": 83, "y": 14, "base_pop": 0.9},
            {"id": "m5", "name": "Western New England", "x": 42, "y": 42, "base_pop": 1.4},
        ],
        "substations": [
            {"id": "s1", "name": "North Maine 345", "x": 79, "y": 15, "kv": 345},
            {"id": "s2", "name": "Boston Ring 345", "x": 66, "y": 32, "kv": 345},
            {"id": "s3", "name": "Rhode Corridor 345", "x": 68, "y": 43, "kv": 345},
            {"id": "s4", "name": "Connecticut Core 345", "x": 57, "y": 49, "kv": 345},
            {"id": "s5", "name": "Western Tie 345", "x": 41, "y": 41, "kv": 345},
            {"id": "s6", "name": "Seabrook 345", "x": 72, "y": 24, "kv": 345},
        ],
        "generation": [
            {"id": "g1", "name": "Maine Onshore Wind", "x": 87, "y": 8, "technology": "Wind"},
            {"id": "g2", "name": "Offshore Lease Cluster", "x": 88, "y": 42, "technology": "Offshore Wind"},
            {"id": "g3", "name": "Connecticut Solar", "x": 56, "y": 58, "technology": "Solar"},
            {"id": "g4", "name": "Battery Harbor", "x": 74, "y": 36, "technology": "Storage"},
        ],
        "zones": [
            {"id": "z1", "name": "Northern Export Forest", "points": [(64, 2), (92, 4), (96, 21), (76, 26), (60, 15)]},
            {"id": "z2", "name": "Boston-Providence Load Shelf", "points": [(57, 23), (78, 22), (80, 47), (62, 50), (52, 35)]},
            {"id": "z3", "name": "Southern Flex Pocket", "points": [(36, 33), (63, 34), (67, 63), (41, 69), (30, 49)]},
        ],
        "lines": [
            ("s1", "s6"),
            ("s6", "s2"),
            ("s2", "s3"),
            ("s3", "s4"),
            ("s4", "s5"),
            ("s2", "s4"),
            ("s5", "s2"),
        ],
    },
}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _nearest(point: dict[str, Any], others: list[dict[str, Any]]) -> dict[str, Any]:
    return min(others, key=lambda other: hypot(point["x"] - other["x"], point["y"] - other["y"]))


def _poly(points: list[tuple[int, int]], width: int, height: int) -> list[list[float]]:
    return [[width * x / 100.0, height * y / 100.0] for x, y in points]


def build_grid_map(region: str, hub: str, kpis: dict[str, float], series: dict[str, Any]) -> dict[str, Any]:
    preset = ISO_GEO_PRESETS[region]
    width = 980
    height = 620
    avg_price = kpis.get("avg_price", 30.0)
    congestion = kpis.get("congestion_mean", 2.5)
    negative_share = kpis.get("negative_share", 0.05)
    queue_total = sum(series.get("queue_p50", []))
    queue_peak = max(series.get("queue_p90", [0.0]) or [0.0])
    stress_index = _clamp(0.28 + avg_price / 110.0 + congestion / 20.0 + negative_share * 1.6, 0.2, 1.0)
    queue_index = _clamp(queue_total / 18000.0, 0.2, 1.0)
    renewable_push = _clamp((queue_peak / 5000.0) + negative_share * 0.7, 0.15, 1.0)

    population_hubs: list[dict[str, Any]] = []
    for idx, metro in enumerate(preset["metros"]):
        population = metro["base_pop"] * (0.95 + avg_price / 180.0 + idx * 0.04)
        peak_load = population * (380 + idx * 35) * (1.0 + stress_index * 0.22)
        population_hubs.append(
            {
                "id": metro["id"],
                "name": metro["name"],
                "x": width * metro["x"] / 100.0,
                "y": height * metro["y"] / 100.0,
                "population_m": round(population, 2),
                "peak_load_mw": round(peak_load, 0),
                "load_growth_pct": round((renewable_push * 6.5) + idx * 0.8, 1),
            }
        )

    substations: list[dict[str, Any]] = []
    for idx, station in enumerate(preset["substations"]):
        x = width * station["x"] / 100.0
        y = height * station["y"] / 100.0
        linked_hub = _nearest({"x": x, "y": y}, population_hubs)
        utilization = _clamp(
            0.42
            + stress_index * 0.28
            + linked_hub["population_m"] / 30.0
            + idx * 0.03,
            0.38,
            0.97,
        )
        criticality = _clamp(
            0.34 + queue_index * 0.25 + station["kv"] / 1400.0 + linked_hub["peak_load_mw"] / 18000.0,
            0.3,
            0.98,
        )
        substations.append(
            {
                "id": station["id"],
                "name": station["name"],
                "x": x,
                "y": y,
                "kv": station["kv"],
                "utilization": round(utilization, 3),
                "criticality": round(criticality, 3),
                "linked_hub": linked_hub["name"],
            }
        )

    generation_sites: list[dict[str, Any]] = []
    for idx, site in enumerate(preset["generation"]):
        capacity = (queue_peak * (0.17 + idx * 0.05)) + (350 if idx == 0 else 210)
        generation_sites.append(
            {
                "id": site["id"],
                "name": site["name"],
                "x": width * site["x"] / 100.0,
                "y": height * site["y"] / 100.0,
                "technology": site["technology"],
                "capacity_mw": round(capacity, 0),
                "dispatchability": round(_clamp(0.22 + idx * 0.18 + renewable_push * 0.25, 0.2, 0.95), 2),
            }
        )

    substations_by_id = {station["id"]: station for station in substations}
    power_lines: list[dict[str, Any]] = []
    for idx, (start_id, end_id) in enumerate(preset["lines"]):
        start = substations_by_id[start_id]
        end = substations_by_id[end_id]
        span = hypot(end["x"] - start["x"], end["y"] - start["y"])
        line_utilization = _clamp((start["utilization"] + end["utilization"]) / 2 + idx * 0.015, 0.35, 0.99)
        power_lines.append(
            {
                "id": f"l{idx + 1}",
                "from_id": start_id,
                "to_id": end_id,
                "from_name": start["name"],
                "to_name": end["name"],
                "kv": max(start["kv"], end["kv"]),
                "loading": round(line_utilization, 3),
                "transfer_mw": round(span * (2.4 + line_utilization), 0),
                "loss_pct": round(_clamp(span / 950.0 + negative_share * 4.0, 0.8, 6.5), 2),
            }
        )

    demand_zones: list[dict[str, Any]] = []
    for idx, zone in enumerate(preset["zones"]):
        zone_hub = population_hubs[min(idx, len(population_hubs) - 1)]
        demand_zones.append(
            {
                "id": zone["id"],
                "name": zone["name"],
                "points": _poly(zone["points"], width, height),
                "intensity": round(_clamp(0.35 + zone_hub["population_m"] / 18.0 + stress_index * 0.22, 0.3, 1.0), 3),
                "load_mw": round(zone_hub["peak_load_mw"] * (0.82 + idx * 0.08), 0),
                "electrification_pct": round(9.0 + idx * 3.8 + renewable_push * 7.5, 1),
            }
        )

    corridor_rank = sorted(power_lines, key=lambda line: (-line["loading"], -line["kv"], -line["transfer_mw"]))[:4]
    corridor_cards = [
        {
            "title": f"{line['from_name']} to {line['to_name']}",
            "detail": (
                f"{line['kv']} kV corridor moving about {int(line['transfer_mw'])} MW "
                f"with {line['loading'] * 100:.0f}% loading and {line['loss_pct']:.1f}% losses."
            ),
        }
        for line in corridor_rank
    ]

    insights = [
        (
            f"{max(population_hubs, key=lambda item: item['peak_load_mw'])['name']} is the dominant demand sink, "
            f"leaning on {max(substations, key=lambda item: item['criticality'])['name']} as a critical transformation node."
        ),
        (
            f"{max(generation_sites, key=lambda item: item['capacity_mw'])['name']} provides the strongest renewable injection, "
            f"creating a visible west-to-east transfer pattern against {hub} pricing pressure."
        ),
        (
            f"Grid stress index is {stress_index * 100:.0f}/100 for {region}, with queue-driven expansion pressure "
            f"showing up around {max(demand_zones, key=lambda item: item['electrification_pct'])['name']}."
        ),
    ]

    return {
        "viewport": {"width": width, "height": height},
        "theme": preset["theme"],
        "stress_index": round(stress_index, 3),
        "queue_index": round(queue_index, 3),
        "renewable_push": round(renewable_push, 3),
        "population_hubs": population_hubs,
        "substations": substations,
        "generation_sites": generation_sites,
        "power_lines": power_lines,
        "demand_zones": demand_zones,
        "corridors": corridor_cards,
        "insights": insights,
    }
