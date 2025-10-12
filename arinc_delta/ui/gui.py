# -*- coding: utf-8 -*-
import csv
import os
import threading
from datetime import datetime
from typing import Callable, List, Optional

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
)


def execute_delta(old_path: Optional[str],
                  new_path: Optional[str],
                  out_dir: str,
                  airport_filter: str,
                  region_filter: str,
                  selected_types: List[str],
                  log: Callable[[str], None]) -> None:
    if not old_path and not new_path:
        raise ValueError("En az bir veri dosyası seçmelisin.")

    req_types = selected_types or list(REGISTRY.keys())
    for t in req_types:
        if t not in REGISTRY:
            raise ValueError(f"Bilinmeyen tip '{t}'. Geçerli seçenekler: {', '.join(REGISTRY.keys())}")

    log(f"Çalışma başlatıldı. Çıktı klasörü: {out_dir}")
    os.makedirs(out_dir, exist_ok=True)

    if not old_path:
        log("Eski dosya seçilmedi; boş veriyle karşılaştırılacak.")
    if not new_path:
        log("Yeni dosya seçilmedi; boş veriyle karşılaştırılacak.")

    selected_type_roots = {REGISTRY[t][0] for t in req_types}
    icao_f = parse_filter_list(airport_filter)
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
        return read_lines(path, selected_type_roots, icao_f, region_area)

    old_lines = read_or_empty(old_path)
    new_lines = read_or_empty(new_path)

    discard_old = find_airports_to_discard(old_lines)
    discard_new = find_airports_to_discard(new_lines)
    discard_icaos = sorted(discard_old | discard_new)

    if discard_icaos:
        discard_path = os.path.join(out_dir, "discarded_airports.txt")
        with open(discard_path, "w", encoding="utf-8") as fw:
            fw.write("\n".join(discard_icaos))
        old_lines = [l for l in old_lines if slice_(l, 7, 10).strip().upper() not in discard_icaos]
        new_lines = [l for l in new_lines if slice_(l, 7, 10).strip().upper() not in discard_icaos]
        log(f"Atlanan ICAO sayısı: {len(discard_icaos)} (liste: discarded_airports.txt)")
    else:
        log("Tüm ICAO kayıtları birincil satır içeriyor; atlanan yok.")

    old_buckets = bucket_by_tcode(old_lines)
    new_buckets = bucket_by_tcode(new_lines)

    region_requested = bool(region_codes)
    context = {
        "region_requested": region_requested,
        "airport_requested": bool(icao_f),
    }

    summary_rows = []
    for t in req_types:
        _, cols, post, extra = REGISTRY[t]
        old_groups = combine_groups(old_buckets.get(t, []))
        new_groups = combine_groups(new_buckets.get(t, []))
        counts = write_type_csvs(
            out_dir,
            t,
            cols,
            post,
            old_groups,
            new_groups,
            extra_handler=extra,
            context=context,
        )
        current_count, added_count, removed_count, modified_count = counts
        summary_rows.append({
            "type": t,
            "current": current_count,
            "added": added_count,
            "removed": removed_count,
            "modified": modified_count,
        })
        log(f"{t}: mevcut={current_count} eklenen={added_count} silinen={removed_count} değişen={modified_count}")

    summary_path = os.path.join(out_dir, "summary.csv")
    with open(summary_path, "w", newline="", encoding="utf-8") as fw:
        writer = csv.DictWriter(fw, fieldnames=["type", "current", "added", "removed", "modified"])
        writer.writeheader()
        for row in summary_rows:
            writer.writerow(row)
    log("İşlem tamamlandı. Özet dosyası: summary.csv")


class DeltaApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("ARINC Delta Karşılaştırma Arayüzü")
        self.root.geometry("720x520")

        main = ttk.Frame(root, padding=12)
        main.pack(fill=tk.BOTH, expand=True)

        self.old_path_var = tk.StringVar()
        self.new_path_var = tk.StringVar()
        self.out_dir_var = tk.StringVar(value=self._default_out_dir())
        self.airport_var = tk.StringVar()
        self.region_var = tk.StringVar(value="EUR,EEU,MES")

        self.type_vars = {t: tk.BooleanVar(value=True) for t in REGISTRY.keys()}

        row = 0
        row = self._build_file_selector(main, row, "Eski verisi", self.old_path_var, self._browse_old)
        row = self._build_file_selector(main, row, "Yeni verisi", self.new_path_var, self._browse_new)
        row = self._build_dir_selector(main, row, "Çıktı klasörü", self.out_dir_var, self._browse_out)

        ttk.Label(main, text="Airport filtresi (virgülle, opsiyonel)").grid(row=row, column=0, sticky="w")
        ttk.Entry(main, textvariable=self.airport_var).grid(row=row, column=1, columnspan=2, sticky="ew", pady=2)
        row += 1

        ttk.Label(main, text="Region filtresi (virgülle, örn. EUR,EEU,MES)").grid(row=row, column=0, sticky="w")
        ttk.Entry(main, textvariable=self.region_var).grid(row=row, column=1, columnspan=2, sticky="ew", pady=2)
        row += 1

        ttk.Label(main, text="Tipler").grid(row=row, column=0, sticky="nw")
        type_frame = ttk.Frame(main)
        type_frame.grid(row=row, column=1, columnspan=2, sticky="w")
        for idx, (code, var) in enumerate(self.type_vars.items()):
            ttk.Checkbutton(type_frame, text=code, variable=var).grid(row=0, column=idx, padx=4, pady=2, sticky="w")
        row += 1

        self.run_button = ttk.Button(main, text="Karşılaştır", command=self._start_run)
        self.run_button.grid(row=row, column=0, columnspan=3, pady=(8, 6), sticky="ew")
        row += 1

        ttk.Label(main, text="Çıktılar").grid(row=row, column=0, sticky="w")
        row += 1

        self.log_text = tk.Text(main, height=14, state="disabled", wrap="word")
        self.log_text.grid(row=row, column=0, columnspan=3, sticky="nsew")
        row += 1

        scrollbar = ttk.Scrollbar(main, orient="vertical", command=self.log_text.yview)
        scrollbar.grid(row=row-1, column=3, sticky="ns")
        self.log_text.configure(yscrollcommand=scrollbar.set)

        main.columnconfigure(1, weight=1)
        main.columnconfigure(2, weight=1)
        main.rowconfigure(row-1, weight=1)

    def _default_out_dir(self) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"delta_output_{timestamp}"

    def _build_file_selector(self, container, row, label, variable, callback):
        ttk.Label(container, text=label).grid(row=row, column=0, sticky="w")
        ttk.Entry(container, textvariable=variable).grid(row=row, column=1, sticky="ew", pady=2)
        ttk.Button(container, text="Gözat", command=callback).grid(row=row, column=2, sticky="ew", padx=4)
        return row + 1

    def _build_dir_selector(self, container, row, label, variable, callback):
        ttk.Label(container, text=label).grid(row=row, column=0, sticky="w")
        ttk.Entry(container, textvariable=variable).grid(row=row, column=1, sticky="ew", pady=2)
        ttk.Button(container, text="Seç", command=callback).grid(row=row, column=2, sticky="ew", padx=4)
        return row + 1

    def _browse_old(self):
        path = filedialog.askopenfilename(title="Eski veri dosyası")
        if path:
            self.old_path_var.set(path)

    def _browse_new(self):
        path = filedialog.askopenfilename(title="Yeni veri dosyası")
        if path:
            self.new_path_var.set(path)

    def _browse_out(self):
        path = filedialog.askdirectory(title="Çıktı klasörü seç")
        if path:
            self.out_dir_var.set(path)

    def _queue_log(self, message: str):
        self.root.after(0, self._append_log, message)

    def _append_log(self, message: str):
        self.log_text.configure(state="normal")
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)
        self.log_text.configure(state="disabled")

    def _start_run(self):
        old_path = self.old_path_var.get().strip()
        new_path = self.new_path_var.get().strip()
        out_dir = self.out_dir_var.get().strip() or self._default_out_dir()
        airport_filter = self.airport_var.get().strip()
        region_filter = self.region_var.get().strip()
        selected_types = [code for code, var in self.type_vars.items() if var.get()]

        if not old_path and not new_path:
            messagebox.showerror("Hata", "En az bir veri dosyası seçmelisin.")
            return

        self.run_button.config(state="disabled")
        self._queue_log("=== Yeni çalışma ===")

        thread = threading.Thread(
            target=self._run_in_thread,
            args=(old_path or None,
                  new_path or None,
                  out_dir,
                  airport_filter,
                  region_filter,
                  selected_types),
            daemon=True,
        )
        thread.start()

    def _run_in_thread(self,
                       old_path: Optional[str],
                       new_path: Optional[str],
                       out_dir: str,
                       airport_filter: str,
                       region_filter: str,
                       selected_types: List[str]):
        try:
            execute_delta(
                old_path,
                new_path,
                out_dir,
                airport_filter,
                region_filter,
                selected_types,
                self._queue_log,
            )
            self._queue_log("Tamamlandı.")
            self.root.after(0, lambda: messagebox.showinfo("Bilgi", "Karşılaştırma tamamlandı."))
        except Exception as exc:
            self._queue_log(f"Hata: {exc}")
            self.root.after(0, lambda: messagebox.showerror("Hata", str(exc)))
        finally:
            self.root.after(0, lambda: self.run_button.config(state="normal"))


def main():
    root = tk.Tk()
    app = DeltaApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()
