# -*- coding: utf-8 -*-
import csv
import os
import threading
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from arinc_delta.cli.delta import REGISTRY, REGION_ALIAS_MAP
from arinc_delta.core.common import (
    parse_filter_list,
    read_lines,
    bucket_by_tcode,
    combine_groups,
    find_airports_to_discard,
    write_type_csvs,
    slice_,
    build_header_for_type,
    build_row_from_group,
    compute_delta,
)


TYPE_TITLES: Dict[str, str] = {
    "PG": "Runway",
    "PI": "Localizer Glide Slope",
    "PV": "Communication",
    "DV": "VHF Navaid",
}

STATE_TITLES: Dict[str, str] = {
    "added": "Added",
    "removed": "Removed",
    "modified": "Updated",
}

TYPE_SUMMARY_FIELDS: Dict[str, List[str]] = {
    "PG": ["icao", "runway_id"],
    "PI": ["icao", "runway_identifier", "localizer_identifier"],
    "PV": ["icao", "communications_type", "communications_frequency"],
    "DV": ["airport_icao", "ils_ident", "vor_frequency"],
}

TYPE_ORDER: Tuple[str, ...] = ("PG", "PI", "PV", "DV")
STATE_ORDER: Tuple[str, ...] = ("added", "removed", "modified")

APP_BACKGROUND = "#f5f7fb"
SURFACE_BACKGROUND = "#ffffff"
TEXT_COLOR = "#1f2a44"
ACCENT_COLOR = "#4b7bcc"
ACCENT_ACTIVE_COLOR = "#3b67ad"
TAB_BACKGROUND = "#e2ebfb"
TAB_SELECTED_BACKGROUND = "#cbd9f6"
TABLE_HEADER_BACKGROUND = "#e6edfa"
TABLE_HEADER_TEXT = TEXT_COLOR
TABLE_CELL_BACKGROUND = "#ffffff"
TABLE_CELL_TEXT = TEXT_COLOR
TABLE_CHANGED_BACKGROUND = "#fff3c4"
TABLE_CHANGED_TEXT = TEXT_COLOR
MAX_GUI_ROWS = 200

def collect_delta_rows(type_code: str,
                       cols,
                       postprocess,
                       old_groups,
                       new_groups) -> Dict[str, Any]:
    cont_numbers: Set[str] = set()
    sim_cont_numbers: Set[str] = set()

    for grp in list(old_groups.values()) + list(new_groups.values()):
        for cno, entry in grp["cont"].items():
            cont_numbers.add(cno)
            if (entry.get("appl", "") or "").upper() == "S":
                sim_cont_numbers.add(cno)

    header = build_header_for_type(cols, cont_numbers, sim_cont_numbers)

    def include_in_gui(field: str) -> bool:
        if field.endswith("_raw"):
            return False
        if field == "primary_1_123":
            return False
        if field.endswith("_1_123"):
            return False
        return True

    visible_header = tuple(field for field in header if include_in_gui(field))
    if not visible_header:
        visible_header = tuple(header)

    added_keys, removed_keys, modified_keys = compute_delta(old_groups, new_groups)

    def build_row(groups, key):
        return build_row_from_group(cols, groups[key], header, postprocess)

    def project(row: Dict[str, Any]) -> Dict[str, Any]:
        return {field: row.get(field, "") for field in visible_header}

    added_rows = [project(build_row(new_groups, key)) for key in added_keys]
    removed_rows = [project(build_row(old_groups, key)) for key in removed_keys]

    modified_rows = []
    for key in modified_keys:
        new_row = build_row(new_groups, key)
        old_row = build_row(old_groups, key)
        changed = [col for col in header if new_row.get(col) != old_row.get(col)]
        visible_changed = [col for col in changed if col in visible_header]
        modified_rows.append({
            "key": key,
            "new": project(new_row),
            "changed_fields": visible_changed,
        })

    return {
        "header": visible_header,
        "added": added_rows,
        "removed": removed_rows,
        "modified": modified_rows,
    }


def format_record_label(type_code: str, row: Dict[str, Any]) -> str:
    fields = TYPE_SUMMARY_FIELDS.get(type_code, [])
    parts: List[str] = []
    for field in fields:
        value = (row.get(field) or "").strip()
        if value:
            parts.append(value)
    if parts:
        return " - ".join(parts)
    payload = (row.get("primary_1_123") or "").strip()
    if payload:
        return payload
    return "Record"


def execute_delta(old_path: Optional[str],
                  new_path: Optional[str],
                  out_dir: str,
                  airport_filter: str,
                  region_filter: str,
                  selected_types: List[str],
                  log: Callable[[str], None]) -> Dict[str, Any]:
    if not old_path and not new_path:
        raise ValueError("En az bir veri dosyasi secmelisin.")

    req_types = selected_types or list(REGISTRY.keys())
    for tcode in req_types:
        if tcode not in REGISTRY:
            raise ValueError(f"Bilinmeyen tip '{tcode}'. Gecerli secenekler: {', '.join(REGISTRY.keys())}")

    log(f"Islem basladi. Cikti klasoru: {out_dir}")
    os.makedirs(out_dir, exist_ok=True)

    if not old_path:
        log("Eski dosya secilmedi; bos veriyle karsilastirilacak.")
    if not new_path:
        log("Yeni dosya secilmedi; bos veriyle karsilastirilacak.")

    selected_type_roots = {REGISTRY[tcode][0] for tcode in req_types}
    icao_filter = parse_filter_list(airport_filter)
    region_codes = parse_filter_list(region_filter)

    region_area = None
    if region_codes:
        region_area = set()
        for code in region_codes:
            mapped = REGION_ALIAS_MAP.get(code)
            if mapped:
                region_area.update(mapped)
            else:
                region_area.add(code)

    def read_or_empty(path: Optional[str]):
        if not path:
            return []
        return read_lines(path, selected_type_roots, icao_filter, region_area)

    old_lines = read_or_empty(old_path)
    new_lines = read_or_empty(new_path)

    discard_old = find_airports_to_discard(old_lines)
    discard_new = find_airports_to_discard(new_lines)
    discard_icaos = sorted(discard_old | discard_new)

    if discard_icaos:
        discard_path = os.path.join(out_dir, "discarded_airports.txt")
        with open(discard_path, "w", encoding="utf-8") as handle:
            handle.write("\n".join(discard_icaos))
        old_lines = [line for line in old_lines if slice_(line, 7, 10).strip().upper() not in discard_icaos]
        new_lines = [line for line in new_lines if slice_(line, 7, 10).strip().upper() not in discard_icaos]
        log(f"Atlanan ICAO sayisi: {len(discard_icaos)} (liste: discarded_airports.txt)")
    else:
        log("Tum ICAO kayitlari birincil satir iceriyor; atlanan yok.")

    old_buckets = bucket_by_tcode(old_lines)
    new_buckets = bucket_by_tcode(new_lines)

    region_requested = bool(region_codes)
    context = {
        "region_requested": region_requested,
        "airport_requested": bool(icao_filter),
    }

    summary_rows = []
    detail_by_type: Dict[str, Dict[str, Any]] = {}

    for tcode in req_types:
        _, cols, post, extra = REGISTRY[tcode]
        old_groups = combine_groups(old_buckets.get(tcode, []))
        new_groups = combine_groups(new_buckets.get(tcode, []))
        counts = write_type_csvs(
            out_dir,
            tcode,
            cols,
            post,
            old_groups,
            new_groups,
            extra_handler=extra,
            context=context,
        )
        current_count, added_count, removed_count, modified_count = counts
        summary_rows.append({
            "type": tcode,
            "current": current_count,
            "added": added_count,
            "removed": removed_count,
            "modified": modified_count,
        })
        detail_by_type[tcode] = collect_delta_rows(tcode, cols, post, old_groups, new_groups)
        log(f"{tcode}: mevcut={current_count} eklenen={added_count} silinen={removed_count} degisen={modified_count}")

    summary_path = os.path.join(out_dir, "summary.csv")
    with open(summary_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["type", "current", "added", "removed", "modified"])
        writer.writeheader()
        for row in summary_rows:
            writer.writerow(row)
    log("Islem tamamlandi. Ozet dosyasi: summary.csv")

    return {
        "types": detail_by_type,
        "summary": summary_rows,
        "out_dir": out_dir,
    }


class CellTable(ttk.Frame):
    def __init__(self,
                 parent: ttk.Frame,
                 headers: Tuple[str, ...],
                 rows: List[Dict[str, Any]],
                 *,
                 header_style: str = "TableHeader.TLabel",
                 cell_style: str = "TableCell.TLabel",
                 changed_style: str = "TableCellChanged.TLabel",
                 on_row_double_click: Optional[Callable[[Dict[str, Any], Set[str]], None]] = None):
        super().__init__(parent)

        self.canvas = tk.Canvas(self,
                                highlightthickness=0,
                                background=APP_BACKGROUND,
                                borderwidth=0)
        self.v_scroll = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.h_scroll = ttk.Scrollbar(self, orient="horizontal", command=self.canvas.xview)

        self.canvas.configure(yscrollcommand=self.v_scroll.set, xscrollcommand=self.h_scroll.set)
        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.v_scroll.grid(row=0, column=1, sticky="ns")
        self.h_scroll.grid(row=1, column=0, sticky="ew")
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.inner = ttk.Frame(self.canvas, style="TableInner.TFrame")
        self.inner.columnconfigure(tuple(range(len(headers))), weight=1)
        self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.inner.bind(
            "<Configure>",
            lambda event: self.canvas.configure(scrollregion=self.canvas.bbox("all"))
        )

        border_padx = (0, 0)
        border_pady = (0, 0)

        for col_index, heading in enumerate(headers):
            lbl = ttk.Label(self.inner, text=heading, style=header_style, anchor="w")
            lbl.grid(row=0, column=col_index, sticky="nsew", padx=border_padx, pady=border_pady)
            self.inner.columnconfigure(col_index, weight=1, minsize=140)

        for row_index, entry in enumerate(rows, start=1):
            values: Dict[str, Any] = entry.get("values", {})
            changed: Set[str] = set(entry.get("changed", set()))
            row_payload = dict(values)
            row_changed = set(changed)

            def bind_handler(widget: ttk.Label, payload=row_payload, changed_fields=row_changed):
                if on_row_double_click:
                    widget.bind(
                        "<Double-Button-1>",
                        lambda _event, data=dict(payload), fields=set(changed_fields): on_row_double_click(data, fields)
                    )

            for col_index, heading in enumerate(headers):
                value = values.get(heading, "")
                style = changed_style if heading in changed else cell_style
                lbl = ttk.Label(self.inner, text=value, style=style, anchor="w")
                lbl.grid(row=row_index, column=col_index, sticky="nsew", padx=border_padx, pady=border_pady)
                bind_handler(lbl)

        # ensure minimum height when no data
        if not rows:
            self.inner.grid_rowconfigure(1, minsize=32)


class DeltaApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("ARINC Delta Viewer")
        self.root.geometry("1120x760")
        self._configure_style()

        self.old_path_var = tk.StringVar()
        self.new_path_var = tk.StringVar()
        self.out_dir_var = tk.StringVar(value=self._default_out_dir())
        self.airport_var = tk.StringVar()
        self.region_var = tk.StringVar(value="EUR,EEU,MES")

        self.type_vars = {code: tk.BooleanVar(value=True) for code in REGISTRY.keys()}

        self.delta_results: Optional[Dict[str, Any]] = None

        self._build_ui()

    def _configure_style(self) -> None:
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        style.configure("TFrame", background=APP_BACKGROUND)
        style.configure("TLabel", background=APP_BACKGROUND, foreground=TEXT_COLOR)
        style.configure("TNotebook", background=APP_BACKGROUND, borderwidth=0)
        style.configure("TNotebook.Tab",
                        background=TAB_BACKGROUND,
                        foreground=TEXT_COLOR,
                        padding=(10, 6))
        style.map("TNotebook.Tab",
                  background=[("selected", TAB_SELECTED_BACKGROUND)],
                  foreground=[("selected", TEXT_COLOR)])

        style.configure("TButton",
                        background=ACCENT_COLOR,
                        foreground="#ffffff",
                        padding=(10, 6))
        style.map("TButton",
                  background=[("active", ACCENT_ACTIVE_COLOR)],
                  foreground=[("disabled", "#d6dbe5")])
        style.configure("Accent.TButton",
                        background=ACCENT_COLOR,
                        foreground="#ffffff",
                        padding=(10, 6))
        style.map("Accent.TButton",
                  background=[("active", ACCENT_ACTIVE_COLOR)],
                  foreground=[("disabled", "#d6dbe5")])

        style.configure("TCheckbutton", background=APP_BACKGROUND, foreground=TEXT_COLOR)
        style.configure("TEntry", fieldbackground=SURFACE_BACKGROUND, foreground=TEXT_COLOR)

        style.configure("Treeview",
                        background=TABLE_CELL_BACKGROUND,
                        fieldbackground=TABLE_CELL_BACKGROUND,
                        foreground=TABLE_CELL_TEXT,
                        rowheight=24,
                        bordercolor=APP_BACKGROUND)
        style.configure("Treeview.Heading",
                        background=TABLE_HEADER_BACKGROUND,
                        foreground=TABLE_HEADER_TEXT,
                        padding=(6, 4),
                        borderwidth=1,
                        relief="solid")

        style.configure("TableInner.TFrame", background=SURFACE_BACKGROUND)
        style.configure("TableHeader.TLabel",
                        background=TABLE_HEADER_BACKGROUND,
                        foreground=TABLE_HEADER_TEXT,
                        padding=(8, 5),
                        relief="solid",
                        borderwidth=1,
                        font=("Segoe UI", 10, "bold"))
        style.configure("TableCell.TLabel",
                        background=TABLE_CELL_BACKGROUND,
                        foreground=TABLE_CELL_TEXT,
                        padding=(6, 4),
                        relief="solid",
                        borderwidth=1,
                        font=("Segoe UI", 10))
        style.configure("TableCellChanged.TLabel",
                        background=TABLE_CHANGED_BACKGROUND,
                        foreground=TABLE_CHANGED_TEXT,
                        padding=(6, 4),
                        relief="solid",
                        borderwidth=1,
                        font=("Segoe UI", 10))

        self.root.configure(background=APP_BACKGROUND)

    def _build_ui(self) -> None:
        main = ttk.Frame(self.root, padding=12)
        main.pack(fill=tk.BOTH, expand=True)

        for col in range(3):
            main.columnconfigure(col, weight=1 if col else 0)

        row = 0
        row = self._build_file_selector(main, row, "Eski veri dosyasi", self.old_path_var, self._browse_old)
        row = self._build_file_selector(main, row, "Yeni veri dosyasi", self.new_path_var, self._browse_new)
        row = self._build_dir_selector(main, row, "Cikti klasoru", self.out_dir_var, self._browse_out)

        ttk.Label(main, text="Airport filtresi (virgul ile, opsiyonel)").grid(row=row, column=0, sticky="w")
        ttk.Entry(main, textvariable=self.airport_var).grid(row=row, column=1, columnspan=2, sticky="ew", pady=2)
        row += 1

        ttk.Label(main, text="Region filtresi (virgul ile, ornek EUR,EEU,MES)").grid(row=row, column=0, sticky="w")
        ttk.Entry(main, textvariable=self.region_var).grid(row=row, column=1, columnspan=2, sticky="ew", pady=2)
        row += 1

        ttk.Label(main, text="Karsilastirilacak tipler").grid(row=row, column=0, sticky="nw")
        type_frame = ttk.Frame(main)
        type_frame.grid(row=row, column=1, columnspan=2, sticky="w")
        for idx, (code, var) in enumerate(sorted(self.type_vars.items())):
            ttk.Checkbutton(type_frame, text=f"{code} ({TYPE_TITLES.get(code, code)})", variable=var).grid(
                row=0, column=idx, padx=4, pady=2, sticky="w"
            )
        row += 1

        self.run_button = ttk.Button(main,
                                     text="Karsilastir ve Goster",
                                     command=self._start_run,
                                     style="Accent.TButton")
        self.run_button.grid(row=row, column=0, columnspan=3, pady=(10, 6), sticky="ew")
        row += 1

        ttk.Label(main, text="Islem gunlugu").grid(row=row, column=0, sticky="w")
        row += 1

        log_frame = ttk.Frame(main)
        log_frame.grid(row=row, column=0, columnspan=3, sticky="nsew")
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        main.rowconfigure(row, weight=1)

        self.log_text = tk.Text(log_frame,
                                height=8,
                                state="disabled",
                                wrap="word",
                                background=SURFACE_BACKGROUND,
                                foreground=TEXT_COLOR,
                                insertbackground=TEXT_COLOR,
                                borderwidth=1,
                                relief="solid",
                                highlightthickness=0)
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)
        row += 1

        ttk.Label(main, text="Fark sonuclari").grid(row=row, column=0, sticky="w", pady=(12, 0))
        row += 1

        self.result_area = ttk.Frame(main)
        self.result_area.grid(row=row, column=0, columnspan=3, sticky="nsew")
        self.result_area.columnconfigure(0, weight=1)
        self.result_area.rowconfigure(0, weight=1)
        main.rowconfigure(row, weight=3)

        self.result_notebook = ttk.Notebook(self.result_area)
        self.result_placeholder = ttk.Label(self.result_area, text="Henuz karsilastirma yapilmadi.", anchor="center")
        self.result_placeholder.grid(row=0, column=0, sticky="nsew")

    def _default_out_dir(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"delta_output_{timestamp}"

    def _build_file_selector(self, container, row, label, variable, callback):
        ttk.Label(container, text=label).grid(row=row, column=0, sticky="w")
        ttk.Entry(container, textvariable=variable).grid(row=row, column=1, columnspan=2, sticky="ew", pady=2)
        ttk.Button(container, text="Gozat", command=callback).grid(row=row, column=2, sticky="ew", padx=4)
        return row + 1

    def _build_dir_selector(self, container, row, label, variable, callback):
        ttk.Label(container, text=label).grid(row=row, column=0, sticky="w")
        ttk.Entry(container, textvariable=variable).grid(row=row, column=1, columnspan=2, sticky="ew", pady=2)
        ttk.Button(container, text="Sec", command=callback).grid(row=row, column=2, sticky="ew", padx=4)
        return row + 1

    def _browse_old(self):
        path = filedialog.askopenfilename(title="Eski veri dosyasi")
        if path:
            self.old_path_var.set(path)

    def _browse_new(self):
        path = filedialog.askopenfilename(title="Yeni veri dosyasi")
        if path:
            self.new_path_var.set(path)

    def _browse_out(self):
        path = filedialog.askdirectory(title="Cikti klasoru sec")
        if path:
            self.out_dir_var.set(path)

    def _queue_log(self, message: str):
        self.root.after(0, self._append_log, message)

    def _append_log(self, message: str):
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def _selected_types(self) -> List[str]:
        return [code for code, var in self.type_vars.items() if var.get()]

    def _start_run(self):
        old_path = self.old_path_var.get().strip()
        new_path = self.new_path_var.get().strip()
        out_dir = self.out_dir_var.get().strip() or self._default_out_dir()
        airport_filter = self.airport_var.get().strip()
        region_filter = self.region_var.get().strip()
        selected_types = self._selected_types()

        if not old_path and not new_path:
            messagebox.showerror("Hata", "En az bir veri dosyasi secmelisin.")
            return

        self.run_button.config(state="disabled")
        self._queue_log("=== Yeni islem ===")

        worker = threading.Thread(
            target=self._run_in_thread,
            args=(old_path or None,
                  new_path or None,
                  out_dir,
                  airport_filter,
                  region_filter,
                  selected_types),
            daemon=True,
        )
        worker.start()

    def _run_in_thread(self,
                       old_path: Optional[str],
                       new_path: Optional[str],
                       out_dir: str,
                       airport_filter: str,
                       region_filter: str,
                       selected_types: List[str]):
        try:
            result = execute_delta(
                old_path,
                new_path,
                out_dir,
                airport_filter,
                region_filter,
                selected_types,
                self._queue_log,
            )
            self._queue_log("Tamamlandi.")
            self.root.after(0, lambda: self._handle_results(result))
        except Exception as exc:
            self._queue_log(f"Hata: {exc}")
            self.root.after(0, lambda: messagebox.showerror("Hata", str(exc)))
        finally:
            self.root.after(0, lambda: self.run_button.config(state="normal"))

    def _handle_results(self, result: Dict[str, Any]) -> None:
        self.delta_results = result
        self._display_results()
        messagebox.showinfo("Bilgi", "Karsilastirma tamamlandi.")

    def _clear_result_notebook(self) -> None:
        for tab_id in self.result_notebook.tabs():
            self.result_notebook.forget(tab_id)

    def _show_placeholder(self) -> None:
        self.result_notebook.grid_forget()
        self.result_placeholder.grid(row=0, column=0, sticky="nsew")

    def _display_results(self) -> None:
        self._clear_result_notebook()
        if not self.delta_results:
            self._show_placeholder()
            return

        type_data = self.delta_results.get("types", {})
        if not type_data:
            self._show_placeholder()
            return

        self.result_placeholder.grid_forget()
        self.result_notebook.grid(row=0, column=0, sticky="nsew")

        summary_lookup = {row["type"]: row for row in self.delta_results.get("summary", [])}

        ordered_types = [t for t in TYPE_ORDER if t in type_data]
        for tcode in type_data.keys():
            if tcode not in ordered_types:
                ordered_types.append(tcode)

        for tcode in ordered_types:
            data = type_data[tcode]
            frame = ttk.Frame(self.result_notebook, padding=10, style="TableInner.TFrame")
            self.result_notebook.add(frame, text=f"{TYPE_TITLES.get(tcode, tcode)} ({tcode})")
            frame.columnconfigure(0, weight=1)
            frame.rowconfigure(1, weight=1)

            counts = summary_lookup.get(tcode)
            if counts:
                info = f"Added: {counts['added']} | Removed: {counts['removed']} | Updated: {counts['modified']}"
                ttk.Label(frame, text=info).grid(row=0, column=0, sticky="w", pady=(0, 8))

            state_notebook = ttk.Notebook(frame)
            state_notebook.grid(row=1, column=0, sticky="nsew")

            for state in STATE_ORDER:
                if state == "modified":
                    total_modified = counts["modified"] if counts else len(data.get("modified", []))
                    self._create_modified_tab(state_notebook, tcode, data, total_modified)
                else:
                    total_state = counts[state] if counts else len(data.get(state, []))
                    self._create_simple_tab(state_notebook, tcode, state, data, total_state)

        if self.result_notebook.tabs():
            self.result_notebook.select(self.result_notebook.tabs()[0])

    def _create_simple_tab(self,
                           notebook: ttk.Notebook,
                           type_code: str,
                           state: str,
                           data: Dict[str, Any],
                           total_count: int) -> None:
        tab = ttk.Frame(notebook, padding=8, style="TableInner.TFrame")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)
        notebook.add(tab, text=STATE_TITLES[state])

        header = tuple(data.get("header", [])) or ("primary_1_123",)
        state_rows = data.get(state, [])
        display_rows = state_rows[:MAX_GUI_ROWS]
        truncated = total_count > len(display_rows)

        rows = [{
            "values": entry,
            "changed": set(),
        } for entry in display_rows]

        table = CellTable(
            tab,
            header,
            rows,
            on_row_double_click=lambda values, changed, hdr=header: self._open_row_detail(hdr, values, changed)
        )
        table.grid(row=0, column=0, sticky="nsew")

        if truncated:
            note = ttk.Label(
                tab,
                text=f"Toplam {total_count} kayit var. Ilk {MAX_GUI_ROWS} satiri gösteriyorum. Tam listeyi CSV dosyasindan inceleyebilirsin.",
                wraplength=640,
            )
            note.grid(row=1, column=0, sticky="w", pady=(6, 0))

    def _create_modified_tab(self,
                             notebook: ttk.Notebook,
                             type_code: str,
                             data: Dict[str, Any],
                             total_count: int) -> None:
        tab = ttk.Frame(notebook, padding=8, style="TableInner.TFrame")
        tab.columnconfigure(0, weight=1)
        tab.rowconfigure(0, weight=1)
        notebook.add(tab, text=STATE_TITLES["modified"])

        header = tuple(data.get("header", [])) or ("primary_1_123",)
        modified_entries = data.get("modified", [])
        display_entries = modified_entries[:MAX_GUI_ROWS]
        truncated = total_count > len(display_entries)

        rows = [{
            "values": entry.get("new", {}),
            "changed": set(entry.get("changed_fields", [])),
        } for entry in display_entries]

        table = CellTable(
            tab,
            header,
            rows,
            on_row_double_click=lambda values, changed, hdr=header: self._open_row_detail(hdr, values, changed)
        )
        table.grid(row=0, column=0, sticky="nsew")

        if truncated:
            note = ttk.Label(
                tab,
                text=f"Toplam {total_count} kayit var. Ilk {MAX_GUI_ROWS} satiri gösteriyorum. Tam listeyi CSV dosyasindan inceleyebilirsin.",
                wraplength=640,
            )
            note.grid(row=1, column=0, sticky="w", pady=(6, 0))

    def _open_row_detail(self,
                         header: Tuple[str, ...],
                         values: Dict[str, Any],
                         changed_fields: Set[str]) -> None:
        detail = tk.Toplevel(self.root)
        detail.title("Kayit Detayi")
        detail.configure(background=APP_BACKGROUND)
        detail.minsize(520, 400)
        detail.columnconfigure(0, weight=1)
        detail.rowconfigure(0, weight=1)

        container = ttk.Frame(detail, padding=12)
        container.grid(row=0, column=0, sticky="nsew")
        container.columnconfigure(0, weight=1)
        container.rowconfigure(0, weight=1)

        canvas = tk.Canvas(container,
                           background=APP_BACKGROUND,
                           highlightthickness=0,
                           borderwidth=0)
        canvas.grid(row=0, column=0, sticky="nsew")
        v_scroll = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
        v_scroll.grid(row=0, column=1, sticky="ns")
        canvas.configure(yscrollcommand=v_scroll.set)

        form = ttk.Frame(canvas, padding=(0, 4), style="TableInner.TFrame")
        canvas.create_window((0, 0), window=form, anchor="nw")
        form.columnconfigure(1, weight=1)

        form.bind(
            "<Configure>",
            lambda _event: canvas.configure(scrollregion=canvas.bbox("all"))
        )

        for index, field in enumerate(header):
            label_text = field + (" *" if field in changed_fields else "")
            ttk.Label(form, text=label_text, style="TableCell.TLabel").grid(
                row=index, column=0, sticky="nw", padx=(0, 8), pady=(0, 6)
            )
            raw_value = values.get(field, "")
            value_str = "" if raw_value is None else str(raw_value)
            base_height = value_str.count("\n") + 1
            approx_height = max(base_height, (len(value_str) // 90) + 1)
            height = min(8, max(1, approx_height))

            text_widget = tk.Text(form,
                                  height=height,
                                  wrap="word",
                                  background=TABLE_CHANGED_BACKGROUND if field in changed_fields else SURFACE_BACKGROUND,
                                  foreground=TEXT_COLOR,
                                  relief="solid",
                                  borderwidth=1,
                                  highlightthickness=0)
            text_widget.insert("1.0", value_str)
            text_widget.configure(state="disabled")
            text_widget.grid(row=index, column=1, sticky="ew", pady=(0, 6))

        close_button = ttk.Button(container, text="Kapat", command=detail.destroy)
        close_button.grid(row=1, column=0, columnspan=2, sticky="e", pady=(8, 0))


def main():
    root = tk.Tk()
    DeltaApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
