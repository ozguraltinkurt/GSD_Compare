# -*- coding: utf-8 -*-
# PI: Localizer / Glide Slope (GS olmayabilir; normal)
TYPE_CODE = "PI"
SELECTED_TYPE = ("P","I")

COLS = [
    ("record_type", (1,1)),
    ("area_code", (2,4)),
    ("sec", (5,5)),
    ("icao", (7,10)),
    ("icao_code", (11,12)),
    ("sub", (13,13)),
    ("localizer_identifier", (14,17)),
    ("ils_category", (18,18)),
    ("localizer_frequency", (23,27)),
    ("runway_identifier", (28,32)),
    ("localizer_latitude", (33,41)),
    ("localizer_longitude", (42,51)),
    ("localizer_bearing", (52,55)),
    ("glide_slope_latitude", (56,64)),
    ("glide_slope_longitude", (65,74)),
    ("localizer_position", (75,78)),
    ("localizer_position_reference", (79,79)),
    ("glide_slope_position", (80,83)),
    ("localizer_width", (84,87)),
    ("glide_slope_angle", (88,90)),
    ("station_declination", (91,95)),
    ("glide_slope_height_lthr", (96,97)),
    ("glide_slope_elevation", (98,102)),
    ("supporting_facility_id", (103,106)),
    ("supporting_facility_icao", (107,108)),
    ("supporting_facility_section", (109,109)),
    ("supporting_facility_subsection", (110,110)),
    ("primary_1_123", None),
]

def postprocess_row(row):
    # ident’ı 4 haneye sabitle; kategori 86 tek karakter
    if row.get("localizer_identifier"):
        row["localizer_identifier"] = row["localizer_identifier"][:4].strip()
