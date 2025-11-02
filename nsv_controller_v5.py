# NSV – Controller v5
# Change in this version:
# • While exporting, create a date–time stamped folder (e.g., NSV_2025-08-19_11-42-30)
#   and save all exported files into that folder. Export also pauses data collection.
# • This file follows the trimmed v4 scaffold you shared. (Video-export hooks can be added
#   later if you bring back the recording writers.)

import os
import time
import math
import random
import queue
import threading
import customtkinter as ctk
from tkinter import ttk, filedialog, messagebox
import numpy as np
import cv2

try:
    import pandas as pd
except Exception:
    pd = None

# ---- Simulated signals (same as your v4 stub) ----

def sim_speed_kmph(t):
    return max(0.0, 40 + 10*math.sin(t/10) + random.uniform(-3, 3))

def sim_gps():
    return (
        17.385 + random.uniform(-1e-4, 1e-4),
        78.486 + random.uniform(-1e-4, 1e-4),
        505 + random.uniform(-1, 1),
    )

def sim_tof_values():
    return [800 + random.gauss(0, 5) for _ in range(5)]

class NSVApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("NSV – Controller v5")
        self.geometry("1450x900")

        self.ui_queue = queue.Queue()
        self.running = False
        self.paused_event = threading.Event()

        self.columns = (
            "Bin end (m)",
            "Speed (km/h)",
            "Latitude",
            "Longitude",
            "Altitude (m)",
            "TOF1 (mm)",
            "TOF2 (mm)",
            "TOF3 (mm)",
            "TOF4 (mm)",
            "TOF5 (mm)",
        )

        self.build_left()
        self.build_right()

        # fake data loop
        self.after(100, self.fake_data)

    # ---------------- UI build ----------------
    def build_left(self):
        left = ctk.CTkFrame(self)
        left.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)

        # Camera placeholders (always visible in this trimmed version)
        cam_bar = ctk.CTkFrame(left)
        cam_bar.pack(fill="x")
        self.cam_labels = []
        for i in range(3):
            lbl = ctk.CTkLabel(cam_bar, text=f"Camera {i}")
            lbl.pack(side="left", expand=True, fill="both", padx=5, pady=5)
            self.cam_labels.append(lbl)

        # Data table
        self.tree = ttk.Treeview(left, columns=self.columns, show="headings", height=12)
        for col in self.columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=120, anchor="center")
        self.tree.pack(fill="both", expand=True, pady=10)

    def build_right(self):
        side = ctk.CTkFrame(self, width=320)
        side.grid(row=0, column=1, sticky="ns")

        self.speed_var = ctk.StringVar(value="0 km/h")
        self.dist_var = ctk.StringVar(value="0 m")
        self.lat_var = ctk.StringVar(value="--")
        self.lon_var = ctk.StringVar(value="--")
        self.alt_var = ctk.StringVar(value="--")

        ctk.CTkLabel(side, text="Survey Data", font=ctk.CTkFont(size=18, weight="bold")).pack(pady=6)
        ctk.CTkLabel(side, textvariable=self.speed_var).pack()
        ctk.CTkLabel(side, textvariable=self.dist_var).pack()
        ctk.CTkLabel(side, textvariable=self.lat_var).pack()
        ctk.CTkLabel(side, textvariable=self.lon_var).pack()
        ctk.CTkLabel(side, textvariable=self.alt_var).pack()

        btns = ctk.CTkFrame(side)
        btns.pack(pady=10, fill="x")
        ctk.CTkButton(btns, text="Start", command=self.on_start).pack(side="left", expand=True, fill="x", padx=2)
        ctk.CTkButton(btns, text="Pause", command=self.on_pause_toggle).pack(side="left", expand=True, fill="x", padx=2)
        ctk.CTkButton(btns, text="Reset", command=self.on_reset).pack(side="left", expand=True, fill="x", padx=2)
        ctk.CTkButton(btns, text="Stop", command=self.on_stop).pack(side="left", expand=True, fill="x", padx=2)
        ctk.CTkButton(side, text="Export", command=self.on_export).pack(pady=5, fill="x")

        # Sidebar performance table
        perf_frame = ctk.CTkFrame(side)
        perf_frame.pack(pady=10, fill="x")
        ctk.CTkLabel(perf_frame, text="Performance Indices", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, columnspan=4)
        cols = ("Left Wheel Path", "Right Wheel Path", "Average")
        self.perf_table = ttk.Treeview(perf_frame, columns=cols, show="tree headings", height=4)
        self.perf_table.heading("#0", text="Metric")
        self.perf_table.column("#0", width=120, anchor="w")
        for col in cols:
            self.perf_table.heading(col, text=col)
            self.perf_table.column(col, width=90, anchor="center")
        self.perf_table.grid(row=1, column=0, columnspan=4)
        for row in ("Rut (mm)", "IRI (m/km)", "SMTD (mm)", "RQI"):
            self.perf_table.insert("", "end", text=row, values=("--", "--", "--"))

    # ---------------- Controls ----------------
    def on_start(self):
        self.running = True

    def on_pause_toggle(self):
        if self.paused_event.is_set():
            self.paused_event.clear()
        else:
            self.paused_event.set()

    def on_reset(self):
        for iid in self.tree.get_children():
            self.tree.delete(iid)

    def on_stop(self):
        self.on_export()
        self.running = False

    # ---- NEW: Export into date-time stamped folder ----
    def on_export(self):
        # Pause data collection while exporting
        self.paused_event.set()

        # Gather rows
        rows = [self.tree.item(iid, "values") for iid in self.tree.get_children()]
        if not rows:
            messagebox.showinfo("Export", "No data to export.")
            return

        # Ask for a parent directory, then create a timestamped subfolder
        base_dir = filedialog.askdirectory(title="Choose export location")
        if not base_dir:
            return
        stamp = time.strftime("NSV_%Y-%m-%d_%H-%M-%S")
        out_dir = os.path.join(base_dir, stamp)
        os.makedirs(out_dir, exist_ok=True)

        # Save data file inside that folder
        data_path = os.path.join(out_dir, "nsv_data.xlsx" if pd else "nsv_data.csv")
        try:
            if pd:
                pd.DataFrame(rows, columns=self.columns).to_excel(data_path, index=False)
            else:
                with open(data_path, "w", encoding="utf-8") as f:
                    f.write(",".join(self.columns) + "\n")
                    for r in rows:
                        f.write(",".join(map(str, r)) + "\n")
            messagebox.showinfo("Export", f"Saved to:\n{out_dir}")
        except Exception as e:
            messagebox.showerror("Export failed", str(e))

    # ---------------- Fake data loop ----------------
    def fake_data(self):
        if self.running and not self.paused_event.is_set():
            t = time.time()
            spd = sim_speed_kmph(t)
            lat, lon, alt = sim_gps()
            tof = sim_tof_values()
            values = [f"{t % 100:.1f}", f"{spd:.1f}", f"{lat:.6f}", f"{lon:.6f}", f"{alt:.1f}"] + [f"{v:.1f}" for v in tof]
            self.tree.insert("", "end", values=values)
            self.speed_var.set(f"{spd:.1f} km/h")
            self.lat_var.set(f"Lat: {lat:.6f}")
            self.lon_var.set(f"Lon: {lon:.6f}")
            self.alt_var.set(f"Alt: {alt:.1f} m")
        self.after(500, self.fake_data)

if __name__ == "__main__":
    NSVApp().mainloop()
