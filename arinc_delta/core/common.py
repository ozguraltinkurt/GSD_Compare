# -*- coding: utf-8 -*-
import os, re, csv
from typing import Dict, Any, Tuple, List, Optional, Set
from collections import OrderedDict, defaultdict

ARINC_WIDTH = 132
HDR_RE = re.compile(r'^\s*(HDR|EOF)\d', re.IGNORECASE)

TCODE_ALIASES = {
    "D ": "DV",
}

CONT_NO_COLUMN_BY_TCODE = {
    "PG": 22,
    "PI": 22,
    "PV": 26,
    "DV": 22,
}

APPLICATION_TYPE_COLUMN_BY_TCODE = {
    "PG": 23,
    "PI": 23,
    "PV": 27,
    "DV": 23,
}

APPLICATION_TYPE_LABELS = {
    "A": "Notes or formatted data continuation",
    "C": "Call sign or controlling agency continuation",
    "E": "Primary record extension",
    "L": "VHF navaid limitation continuation",
    "N": "Sector narrative continuation",
    "T": "Time of operations continuation (formatted data)",
    "U": "Time of operations continuation (narrative data)",
    "V": "Time of operations continuation (alternate narrative)",
    "P": "Flight planning application continuation",
    "Q": "Flight planning primary data continuation",
    "S": "Simulation application continuation",
}

# ---------- low-level helpers ----------
def pad132(s: str) -> str:
    s = s.rstrip("\r\n")
    if len(s) < ARINC_WIDTH:
        s = s + " " * (ARINC_WIDTH - len(s))
    return s[:ARINC_WIDTH]

def looks_record(line: str) -> bool:
    return len(line.rstrip("\r\n")) >= 70

def is_header(line: str) -> bool:
    return bool(HDR_RE.match(line.strip()))

def slice_(line: str, a: int, b: int) -> str:
    return line[a-1:b]

def cont_no(line: str, column: int = 22) -> str:
    # Primary: bosluk veya '0' veya '1'
    c = slice_(line, column, column)
    return "" if c in ("", "0", "1", " ") else c

def cont_application_type(line: str, tcode: str) -> str:
    col = APPLICATION_TYPE_COLUMN_BY_TCODE.get(tcode)
    if col is None:
        return ""
    return slice_(line, col, col).strip().upper()

def application_type_label(code: str) -> str:
    if not code:
        return ""
    return APPLICATION_TYPE_LABELS.get(code.upper(), "Unknown")

def type_tuple(line: str) -> Tuple[str,str]:
    return (slice_(line,5,5), slice_(line,13,13))

def tcode(line: str) -> str:
    s, u = type_tuple(line)
    raw = f"{s}{u}"
    return TCODE_ALIASES.get(raw, raw)

def payload_1_123(line: str) -> str:
    return line[:123]  # FRN/Cycle hariç

def parse_filter_list(arg: Optional[str]) -> Optional[Set[str]]:
    if not arg: return None
    vals = [x.strip().upper() for x in arg.split(",") if x.strip()]
    return set(vals) if vals else None

def line_passes_filters(line: str, icao_set: Optional[Set[str]], area_set: Optional[Set[str]]) -> bool:
    if icao_set is None and area_set is None: return True
    icao = slice_(line,7,10).strip().upper()
    area = slice_(line,2,4).strip().upper()
    ok_icao = True if icao_set is None else (icao in icao_set)
    ok_area = True if area_set is None else (area in area_set)
    return ok_icao and ok_area

# ---------- grouping ----------
def idroot_for_line(line: str) -> Tuple[str, str]:
    """TIP (PG/PI/PV) + cols 1..21 — primary & continuation aynı anahtar."""
    return (tcode(line), slice_(line, 1, 21))

def combine_groups(lines: List[str]) -> Dict[Tuple[str,str], Dict[str,Any]]:
    """Her entity: {'type':(sec,sub), 'primary':line|None, 'cont':{cno->{'line':str,'appl':str}}, 'sample':line}"""
    groups: Dict[Tuple[str,str], Dict[str,Any]] = {}
    for ln in lines:
        key = idroot_for_line(ln)
        tcode = key[0]
        cont_col = CONT_NO_COLUMN_BY_TCODE.get(tcode, 22)
        g = groups.setdefault(key, {"type": type_tuple(ln), "primary": None,
                                    "cont": OrderedDict(), "sample": ln})
        c = cont_no(ln, cont_col)
        if c == "":
            if g["primary"] is None:
                g["primary"] = ln
            continue
        appl = cont_application_type(ln, tcode)
        g["cont"][c] = {"line": ln, "appl": appl}
    return groups

def find_airports_to_discard(lines: List[str]) -> Set[str]:
    """ICAO'yu discard et: (PG/PI/PV içinde) primary==0 ve continuation>0 ise."""
    groups = combine_groups(lines)
    primary_count_by_icao = defaultdict(int)
    cont_count_by_icao    = defaultdict(int)
    for g in groups.values():
        ref = g["primary"] or g["sample"]
        icao = slice_(ref, 7, 10).strip().upper()
        if not icao: continue
        if g["primary"] is not None:
            primary_count_by_icao[icao] += 1
        if g["cont"]:
            cont_count_by_icao[icao]    += 1
    to_discard = set()
    for icao in set(list(primary_count_by_icao.keys()) + list(cont_count_by_icao.keys())):
        if primary_count_by_icao.get(icao, 0) == 0 and cont_count_by_icao.get(icao, 0) > 0:
            to_discard.add(icao)
    return to_discard

# ---------- IO ----------
def read_lines(path: str,
               selected_types: Set[Tuple[str,str]],
               icao_set: Optional[Set[str]],
               area_set: Optional[Set[str]]) -> List[str]:
    out=[]
    with open(path, "r", encoding="latin1", errors="ignore") as f:
        for raw in f:
            if is_header(raw) or not looks_record(raw): continue
            ln = pad132(raw)
            if type_tuple(ln) not in selected_types: continue
            if not line_passes_filters(ln, icao_set, area_set): continue
            out.append(ln)
    return out

def bucket_by_tcode(lines: List[str]) -> Dict[str, List[str]]:
    b: Dict[str, List[str]] = {}
    for ln in lines:
        b.setdefault(tcode(ln), []).append(ln)
    return b

# ---------- CSV & delta ----------
def write_csv(path: str, rows: List[Dict[str,Any]], header: List[str]):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def canonical_payload(grp: Dict[str,Any]) -> str:
    parts=[]
    if grp["primary"] is not None:
        parts.append(payload_1_123(grp["primary"]))
    for cno in sorted(grp["cont"].keys(), key=lambda x: (len(x), x)):
        entry = grp["cont"][cno]
        appl = entry.get("appl", "") or "_"
        parts.append(f"[C{cno}:{appl}]" + payload_1_123(entry["line"]))
    return "|".join(parts)

def compute_delta(old_g, new_g):
    ok, nk = set(old_g.keys()), set(new_g.keys())
    added   = sorted(nk - ok)
    removed = sorted(ok - nk)
    modified= [k for k in sorted(ok & nk) if canonical_payload(old_g[k]) != canonical_payload(new_g[k])]
    return added, removed, modified

def build_header_for_type(cols: List[Tuple[str, Optional[Tuple[int,int]]]],
                         cont_numbers: Set[str]) -> List[str]:
    hdr = [name for (name, _) in cols]
    for cno in sorted(cont_numbers, key=lambda x: (len(x), x)):
        prefix = f"cont#{cno}"
        hdr.append(f"{prefix}_appl_code")
        hdr.append(f"{prefix}_appl_label")
        hdr.append(f"{prefix}_1_123")
    return hdr

def parse_primary_fields(line: str, cols: List[Tuple[str, Optional[Tuple[int,int]]]]) -> Dict[str,Any]:
    d: Dict[str,Any] = {}
    for name, rng in cols:
        d[name] = payload_1_123(line) if rng is None else slice_(line, rng[0], rng[1]).strip()
    return d

def build_row_from_group(cols: List[Tuple[str, Optional[Tuple[int,int]]]],
                         grp: Dict[str,Any],
                         header: List[str],
                         postprocess_row=None) -> Dict[str,Any]:
    row: Dict[str,Any] = {}
    ref = grp["primary"]
    if not ref and grp["cont"]:
        first_cont = sorted(grp["cont"].keys(), key=lambda x: (len(x), x))[0]
        ref = grp["cont"][first_cont]["line"]
    if ref:
        row.update(parse_primary_fields(ref, cols))
        if grp["primary"] is not None:
            row["primary_1_123"] = payload_1_123(grp["primary"])
    else:
        for k in header:
            row[k] = ""
    for cno in sorted(grp["cont"].keys(), key=lambda x: (len(x), x)):
        entry = grp["cont"][cno]
        prefix = f"cont#{cno}"
        code_key = f"{prefix}_appl_code"
        label_key = f"{prefix}_appl_label"
        payload_key = f"{prefix}_1_123"
        if code_key in header:
            row[code_key] = entry.get("appl", "")
        if label_key in header:
            row[label_key] = application_type_label(entry.get("appl", ""))
        if payload_key in header:
            row[payload_key] = payload_1_123(entry["line"])
    if postprocess_row:
        postprocess_row(row)
    for k in header:
        row.setdefault(k, "")
    return row

def write_type_csvs(out_dir: str,
                    type_code: str,
                    cols: List[Tuple[str, Optional[Tuple[int,int]]]],
                    postprocess_row,
                    old_groups,
                    new_groups,
                    extra_handler=None):
    conts=set()
    for g in list(old_groups.values()) + list(new_groups.values()):
        conts.update(g["cont"].keys())
    base_hdr = build_header_for_type(cols, conts)
    mod_hdr  = base_hdr + ["changed_field_count","changed_fields"]

    current_rows = [build_row_from_group(cols, g, base_hdr, postprocess_row) for g in new_groups.values()]
    write_csv(os.path.join(out_dir, f"current_{type_code}.csv"), current_rows, base_hdr)

    added, removed, modified = compute_delta(old_groups, new_groups)
    added_rows = [build_row_from_group(cols, new_groups[k], base_hdr, postprocess_row) for k in added]
    removed_rows = [build_row_from_group(cols, old_groups[k], base_hdr, postprocess_row) for k in removed]
    write_csv(os.path.join(out_dir, f"added_{type_code}.csv"), added_rows, base_hdr)
    write_csv(os.path.join(out_dir, f"removed_{type_code}.csv"), removed_rows, base_hdr)

    mod_rows=[]
    for k in modified:
        nr = build_row_from_group(cols, new_groups[k], base_hdr, postprocess_row)
        orow = build_row_from_group(cols, old_groups[k], base_hdr, postprocess_row)
        changed = [c for c in base_hdr if nr.get(c) != orow.get(c)]
        row = dict(nr)
        row["changed_field_count"] = len(changed)
        row["changed_fields"]      = ",".join(changed)
        mod_rows.append(row)
    write_csv(os.path.join(out_dir, f"modified_{type_code}.csv"), mod_rows, mod_hdr)

    if extra_handler:
        extra_handler(out_dir, type_code, base_hdr, mod_hdr,
                      current_rows, added_rows, removed_rows, mod_rows)

    return len(current_rows), len(added), len(removed), len(mod_rows)
