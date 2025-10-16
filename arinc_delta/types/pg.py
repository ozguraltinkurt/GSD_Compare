# -*- coding: utf-8 -*-
# PG: Runway
TYPE_CODE = "PG"
SELECTED_TYPE = ("P","G")

COLS = [
    ("area_code",(2,4)),
    ("icao",(7,10)), ("runway_id",(14,18)),
    ("rwy_length_ft",(23,27)), ("rwy_mag_brg_tenths",(28,31)),
    ("lat_raw",(33,41)), ("lon_raw",(42,51)),
    ("rwy_grad_pct100",(52,56)),
    ("lthr_elev_ft",(67,71)), ("dthr_ft",(72,75)),
    ("tch_raw",(76,77)), ("rwy_width_ft",(78,80)),
    ("loc_mls_gls_ident",(82,85)),
    ("primary_1_123", None),
]

def postprocess_row(row):
    # PG için ekstra düzeltme gerek yok; alan adları ham substring.
    # İstersen burada bearing/gradient’ı sayısallaştırıp yazabilirsin.
    if row.get("loc_mls_gls_ident"):
        row["loc_mls_gls_ident"] = row["loc_mls_gls_ident"][:4].strip()
