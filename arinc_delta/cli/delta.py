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

REGION_AREA_MAP = {
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
                     mod_rows):
    if type_code != vhf.TYPE_CODE:
        return

    def filter_vor(rows):
        return [r for r in rows if r.get("navaid_class", "").startswith("V")]

    vor_current = filter_vor(current_rows)
    if vor_current:
        write_csv(os.path.join(out_dir, f"current_{type_code}_vor.csv"), vor_current, base_hdr)

    vor_added = filter_vor(added_rows)
    if vor_added:
        write_csv(os.path.join(out_dir, f"added_{type_code}_vor.csv"), vor_added, base_hdr)

    vor_removed = filter_vor(removed_rows)
    if vor_removed:
        write_csv(os.path.join(out_dir, f"removed_{type_code}_vor.csv"), vor_removed, base_hdr)

    vor_modified = filter_vor(mod_rows)
    if vor_modified:
        write_csv(os.path.join(out_dir, f"modified_{type_code}_vor.csv"), vor_modified, mod_hdr)


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
    ap.add_argument("--region",  default="EU,ME", help="Region presets: combinations of EU,ME (comma-separated). Use empty string for all.")
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
            if code not in REGION_AREA_MAP:
                raise SystemExit(f"Unknown region '{code}'. Allowed: {', '.join(sorted(REGION_AREA_MAP.keys()))}")
            region_area.update(REGION_AREA_MAP[code])
    area_f = explicit_area if explicit_area is not None else region_area

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
        c,a,r,m = write_type_csvs(args.out, t, cols, post, og, ng, extra_handler=extra)
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
