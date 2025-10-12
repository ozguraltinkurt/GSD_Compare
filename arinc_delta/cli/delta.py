# -*- coding: utf-8 -*-
import argparse, os
from typing import Set, Tuple, Dict, Any, List, Optional

from arinc_delta.core.common import (
    parse_filter_list, read_lines, bucket_by_tcode, combine_groups,
    find_airports_to_discard, write_type_csvs, slice_, type_tuple,
    write_csv
)

# tip modülleri
from arinc_delta.types import pg, pi, pv, vhf

REGION_ALIAS_MAP = {
    "EU": {"EUR", "EEU"},
    "ME": {"MES"},
}


def write_vhf_extras(out_dir: str,
                     type_code: str,
                     base_hdr,
                     mod_hdr,
                     current_rows,
                     added_rows,
                     removed_rows,
                     mod_rows,
                     context):
    if type_code != vhf.TYPE_CODE:
        return None

    region_requested = bool(context.get("region_requested"))

    def is_ils_dme(row):
        raw = row.get("primary_1_123", "") or ""
        return len(raw) >= 29 and raw[28].upper() == "I"

    def is_vor(row):
        return (row.get("navaid_class", "") or "").upper().startswith("V")

    # Remove default DV files; custom outputs will be written below.
    for prefix in ("current", "added", "removed", "modified"):
        default_path = os.path.join(out_dir, f"{prefix}_{type_code}.csv")
        if os.path.exists(default_path):
            os.remove(default_path)

    def write_rows(prefix: str, suffix: str, rows, header):
        write_csv(os.path.join(out_dir, f"{prefix}_{suffix}.csv"), rows, header)

    def rename_ident_fields(rows, old_key, new_key):
        renamed = []
        for r in rows:
            new_row = dict(r)
            if old_key in new_row:
                new_row[new_key] = new_row.pop(old_key)
            if new_row.get("changed_fields"):
                parts = [new_key if x == old_key else x for x in new_row["changed_fields"].split(",") if x]
                new_row["changed_fields"] = ",".join(parts)
            renamed.append(new_row)
        return renamed

    def rename_header(header, old_key, new_key):
        return [new_key if h == old_key else h for h in header]

    ils_suffix = f"{type_code.lower()}_ils_dme"
    ils_current = [r for r in current_rows if is_ils_dme(r)]
    ils_added = [r for r in added_rows if is_ils_dme(r)]
    ils_removed = [r for r in removed_rows if is_ils_dme(r)]
    ils_modified = [r for r in mod_rows if is_ils_dme(r)]

    write_rows("current", ils_suffix, ils_current, base_hdr)
    write_rows("added", ils_suffix, ils_added, base_hdr)
    write_rows("removed", ils_suffix, ils_removed, base_hdr)
    write_rows("modified", ils_suffix, ils_modified, mod_hdr)

    vor_suffix = f"{type_code.lower()}_vor"
    vor_files = [os.path.join(out_dir, f"{prefix}_{vor_suffix}.csv")
                 for prefix in ("current", "added", "removed", "modified")]

    if region_requested:
        vor_current = rename_ident_fields([r for r in current_rows if is_vor(r)], "ils_ident", "vor_ident")
        vor_added = rename_ident_fields([r for r in added_rows if is_vor(r)], "ils_ident", "vor_ident")
        vor_removed = rename_ident_fields([r for r in removed_rows if is_vor(r)], "ils_ident", "vor_ident")
        vor_modified = rename_ident_fields([r for r in mod_rows if is_vor(r)], "ils_ident", "vor_ident")

        vor_base_hdr = rename_header(base_hdr, "ils_ident", "vor_ident")
        vor_mod_hdr = rename_header(mod_hdr, "ils_ident", "vor_ident")

        write_rows("current", vor_suffix, vor_current, vor_base_hdr)
        write_rows("added", vor_suffix, vor_added, vor_base_hdr)
        write_rows("removed", vor_suffix, vor_removed, vor_base_hdr)
        write_rows("modified", vor_suffix, vor_modified, vor_mod_hdr)
    else:
        for path in vor_files:
            if os.path.exists(path):
                os.remove(path)

    return len(ils_current), len(ils_added), len(ils_removed), len(ils_modified)


REGISTRY = {
    pg.TYPE_CODE: (pg.SELECTED_TYPE, pg.COLS, pg.postprocess_row, None),
    pi.TYPE_CODE: (pi.SELECTED_TYPE, pi.COLS, pi.postprocess_row, None),
    pv.TYPE_CODE: (pv.SELECTED_TYPE, pv.COLS, pv.postprocess_row, None),
    vhf.TYPE_CODE: (vhf.SELECTED_TYPE, vhf.COLS, vhf.postprocess_row, write_vhf_extras),
}

def main():
    ap = argparse.ArgumentParser(description="ARINC 424-17 delta — PG/PI/PV/DV (type-specific combine & CSV)")
    ap.add_argument("old"); ap.add_argument("new")
    ap.add_argument("--out", default="delta_pg_pi_pv_out")
    ap.add_argument("--airport", default=None, help="ICAO filter (e.g., LTAC or LTAC,LTFM)")
    ap.add_argument("--area",    default=None, help="Area Code filter col(2–4) (e.g., EUU,TR1)")
    ap.add_argument("--region",  default="EUR,EEU,MES", help="Region presets: comma-separated list of area codes or aliases (e.g., EUR,EEU,MES or EU). Use empty string for all.")
    ap.add_argument("--types",   default="PG,PI,PV,DV", help="Comma: subset of PG,PI,PV,DV")
    args = ap.parse_args()

    req_types = [t.strip().upper() for t in args.types.split(",") if t.strip()]
    for t in req_types:
        if t not in REGISTRY:
            raise SystemExit(f"Unknown type '{t}'. Allowed: {', '.join(REGISTRY.keys())}")

    selected_types: Set[Tuple[str,str]] = {REGISTRY[t][0] for t in req_types}
    icao_f = parse_filter_list(args.airport)

    explicit_area = parse_filter_list(args.area)
    region_codes = parse_filter_list(args.region)
    region_area: Optional[Set[str]] = None
    if region_codes:
        region_area = set()
        for code in region_codes:
            mapped = REGION_ALIAS_MAP.get(code)
            if mapped:
                region_area.update(mapped)
            else:
                region_area.add(code)
    area_f = explicit_area if explicit_area is not None else region_area
    execution_context = {
        "region_requested": bool(region_codes),
        "airport_requested": bool(icao_f),
    }

    # read & filter
    old_lines = read_lines(args.old, selected_types, icao_f, area_f)
    new_lines = read_lines(args.new, selected_types, icao_f, area_f)

    # discard ICAOs with no primary but has continuations
    discard_old = find_airports_to_discard(old_lines)
    discard_new = find_airports_to_discard(new_lines)
    discard_icaos = sorted(discard_old | discard_new)

    os.makedirs(args.out, exist_ok=True)
    if discard_icaos:
        with open(os.path.join(args.out, "discarded_airports.txt"), "w", encoding="utf-8") as fw:
            fw.write("\n".join(discard_icaos))
        old_lines = [l for l in old_lines if slice_(l,7,10).strip().upper() not in discard_icaos]
        new_lines = [l for l in new_lines if slice_(l,7,10).strip().upper() not in discard_icaos]
        print(f"Discarded ICAOs: {', '.join(discard_icaos)}")
    else:
        print("No airports discarded (primary present for all with continuations).")

    # bucket by type code ("PG","PI","PV","DV")
    old_b = bucket_by_tcode(old_lines)
    new_b = bucket_by_tcode(new_lines)

    # per-type combine & delta
    import csv
    summary_rows: List[Dict[str,Any]] = []
    for t in req_types:
        _selected_type, cols, post, extra = REGISTRY[t]
        og = combine_groups(old_b.get(t, []))
        ng = combine_groups(new_b.get(t, []))
        c,a,r,m = write_type_csvs(args.out, t, cols, post, og, ng,
                                  extra_handler=extra, context=execution_context)
        summary_rows.append({"type": t, "current": c, "added": a, "removed": r, "modified": m})
        print(f"{t}: current={c} added={a} removed={r} modified={m}")

    with open(os.path.join(args.out, "summary.csv"), "w", newline="", encoding="utf-8") as fw:
        w = csv.DictWriter(fw, fieldnames=["type","current","added","removed","modified"])
        w.writeheader()
        for row in summary_rows:
            w.writerow(row)

    print(f"Done. Outputs in: {args.out}")

if __name__ == "__main__":
    main()
