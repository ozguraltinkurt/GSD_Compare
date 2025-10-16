# -*- coding: utf-8 -*-
# DV: VHF NAVAID
TYPE_CODE = "DV"
SELECTED_TYPE = ("D", " ")

COLS = [
    ("area_code", (2, 4)),
    ("subsection_code", (6, 6)),
    ("airport_icao", (7, 10)),
    ("icao_code", (11, 12)),
    ("ils_ident", (14, 17)),
    ("navaid_icao_code", (20, 21)),
    ("vor_frequency", (23, 27)),
    ("navaid_class", (28, 32)),
    ("vor_latitude", (33, 41)),
    ("vor_longitude", (42, 51)),
    ("dme_ident", (52, 55)),
    ("dme_latitude", (56, 64)),
    ("dme_longitude", (65, 74)),
    ("station_declination", (75, 79)),
    ("dme_elevation", (80, 84)),
    ("figure_of_merit", (85, 85)),
    ("ils_dme_bias", (86, 87)),
    ("frequency_protection", (88, 90)),
    ("datum_code", (91, 93)),
    ("vor_name", (94, 123)),
    ("primary_1_123", None),
]


def postprocess_row(row):
    # Strip padding from common textual fields for readability.
    for key in ("ils_ident", "vor_name", "airport_icao"):
        if row.get(key):
            row[key] = row[key].strip()
