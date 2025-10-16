# -*- coding: utf-8 -*-
# PV: Airport Communications
TYPE_CODE = "PV"
SELECTED_TYPE = ("P","V")

COLS = [
    ("area_code", (2,4)),
    ("blank_6", (6,6)),
    ("icao", (7,10)),
    ("icao_code", (11,12)),
    ("communications_type", (14,16)),
    ("communications_frequency", (17,23)),
    ("guard_transmit", (24,24)),
    ("frequency_units", (25,25)),
    ("cont_no_column_26", (26,26)),
    ("service_indicator", (27,29)),
    ("radar_service", (30,30)),
    ("modulation", (31,31)),
    ("signal_emission", (32,32)),
    ("latitude", (33,41)),
    ("longitude", (42,51)),
    ("magnetic_variation", (52,56)),
    ("facility_elevation", (57,61)),
    ("h24_indicator", (62,62)),
    ("sectorization", (63,68)),
    ("altitude_description", (69,69)),
    ("communication_altitude_1", (70,74)),
    ("communication_altitude_2", (75,79)),
    ("sector_facility", (80,83)),
    ("sector_facility_icao", (84,85)),
    ("sector_facility_section", (86,86)),
    ("sector_facility_subsection", (87,87)),
    ("distance_description", (88,88)),
    ("communications_distance", (89,90)),
    ("remote_facility", (91,94)),
    ("remote_facility_icao", (95,96)),
    ("remote_facility_section", (97,97)),
    ("remote_facility_subsection", (98,98)),
    ("call_sign", (99,123)),
    ("primary_1_123", None),
]

def postprocess_row(row):
    if row.get("communications_type"):
        row["communications_type"] = row["communications_type"].strip()
    if row.get("communications_frequency"):
        row["communications_frequency"] = row["communications_frequency"].strip()
    if row.get("call_sign"):
        row["call_sign"] = row["call_sign"].strip()
