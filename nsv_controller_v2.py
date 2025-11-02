# NSV – Basic Controller (v2)
# Changes vs v1:
# 1) Readings start on "Start" and stop on "Stop" (sensors + cameras)
# 2) "Export" button to save current table to Excel
# 3) Sensor selection module (below camera views) to enable/disable any of the 5 TOF sensors

import time
import math
import threading
import queue
import random
import cv2
import numpy as np
import customtkinter as ctk
from tkinter import ttk, messagebox, filedialog

# Optional: pip install pandas openpyxl for Excel export
try:
    import pandas as pd
except Exception:  # keep UI working if pandas isn't installed yet
    pd = None

BIN_METERS = 10.0           # distance interval for averaging
TOF_HZ = 100                # sensor sample rate
CAM_INDEXES = [0, 1, 2]     # change per your machine

# -----------------------------
# Utility: Test card for cameras
# -----------------------------
def make_testcard(w=400, h=225, text="NO CAMERA"):
    img = np.zeros((h, w, 3), dtype=np.uint8)
    bars = [
        (255, 255, 255), (255, 255, 0), (0, 255, 255),
        (0, 255, 0),     (255, 0, 255), (255, 0, 0), (0, 0, 255)
    ]
    bw = w // len(bars)
    for i, c in enumerate(bars):
        img[:, i*bw:(i+1)*bw] = c
    cv2.putText(img, text, (10, h//2), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (0,0,0), 3, cv2.LINE_AA)
    cv2.putText(img, text, (10, h//2), cv2.FONT_HERSHEY_SIMPLEX, 0.9, (255,255,255), 1, cv2.LINE_AA)
    return img

# -----------------------------
# Camera worker (start/stop-able)
# -----------------------------
class CameraWorker(threading.Thread):
    def __init__(self, cam_index, out_queue, width=400, height=225):
        super().__init__(daemon=True)
        self.cam_index = cam_index
        self.out_queue = out_queue
        self.width = width
        self.height = height
        self.stop_event = threading.Event()
        self.cap = None

    def run(self):
        try:
            self.cap = cv2.VideoCapture(self.cam_index, cv2.CAP_DSHOW)
            if self.cap is None or not self.cap.isOpened():
                while not self.stop_event.is_set():
                    frame = make_testcard(self.width, self.height, f"CAM {self.cam_index} NOT FOUND")
                    self.out_queue.put((self.cam_index, frame))
                    time.sleep(0.1)
                return
            self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
            self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)
            while not self.stop_event.is_set():
                ok, frame = self.cap.read()
                if not ok:
                    frame = make_testcard(self.width, self.height, f"CAM {self.cam_index} ERROR")
                else:
                    frame = cv2.resize(frame, (self.width, self.height))
                self.out_queue.put((self.cam_index, frame))
                time.sleep(0.03)  # ~33 fps
        finally:
            if self.cap is not None:
                self.cap.release()

    def stop(self):
        self.stop_event.set()

# -----------------------------
# Simulated DMI / GPS / TOF
# Replace these functions with your real hardware reads
# -----------------------------
def sim_speed_kmph(t):
    base = 40 + 12*math.sin(2*math.pi*t/20.0) + 3*random.uniform(-1,1)
    return max(0.0, base)

def sim_gps():
    lat = 17.385 + random.uniform(-1e-4, 1e-4)
    lon = 78.486 + random.uniform(-1e-4, 1e-4)
    alt = 505 + random.uniform(-1.0, 1.0)
    return lat, lon, alt

def sim_tof_values():
    base = 800  # mm (80 cm)
    return [base + random.gauss(0, 5) for _ in range(5)]

# -----------------------------
# Sensor worker: samples TOF @100Hz, integrates distance, bins every 10 m
# -----------------------------
class SensorWorker(threading.Thread):
    def __init__(self, ui_queue, enabled_mask_getter):
        super().__init__(daemon=True)
        self.ui_queue = ui_queue
        self.enabled_mask_getter = enabled_mask_getter
        self.stop_event = threading.Event()
        self.distance_m = 0.0
        self.last_t = time.time()
        self.next_bin_edge = BIN_METERS
        self.bin_buffers = [[] for _ in range(5)]

    def run(self):
        while not self.stop_event.is_set():
            t = time.time()
            speed_kmph = sim_speed_kmph(t)
            speed_mps = speed_kmph / 3.6
            dt = t - self.last_t
            self.last_t = t
            self.distance_m += speed_mps * dt
            lat, lon, alt = sim_gps()
            tof_raw = sim_tof_values()

            enabled = self.enabled_mask_getter()  # list[bool] len 5
            tof = [v if enabled[i] else float('nan') for i, v in enumerate(tof_raw)]
            for i, v in enumerate(tof):
                self.bin_buffers[i].append(v)

            self.ui_queue.put({
                "type": "telemetry",
                "speed_kmph": speed_kmph,
                "distance_m": self.distance_m,
                "lat": lat, "lon": lon, "alt": alt,
                "tof": tof,
            })

            if self.distance_m >= self.next_bin_edge:
                avg = [float(np.nanmean(buf)) if len(buf) else float("nan") for buf in self.bin_buffers]
                self.bin_buffers = [[] for _ in range(5)]
                self.ui_queue.put({
                    "type": "bin",
                    "bin_end_m": self.next_bin_edge,
                    "avg_tof_mm": avg
                })
                self.next_bin_edge += BIN_METERS

            time.sleep(max(0.0, 1.0/TOF_HZ - (time.time()-t)))

    def stop(self):
        self.stop_event.set()

# -----------------------------
# UI
# -----------------------------
class NSVApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")
        self.title("NSV – Basic Controller v2")
        self.geometry("1320x820")

        self.ui_queue = queue.Queue()
        self.cam_queue = queue.Queue()

        # runtime state
        self.cams = []
        self.sensor_worker = None
        self.running = False

        # sensor enable mask (thread-safe)
        self._mask_lock = threading.Lock()
        self.sensor_enabled = [True, True, True, True, True]

        # layout
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=0)
        self.grid_rowconfigure(0, weight=1)

        self.build_left()
        self.build_right()

        # timers for queues
        self.after(30, self.poll_cam_frames)
        self.after(60, self.poll_ui_queue)

        self.protocol("WM_DELETE_WINDOW", self.on_close)

    # -------- Left area --------
    def build_left(self):
        left = ctk.CTkFrame(self, corner_radius=12)
        left.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        left.grid_columnconfigure((0,1,2), weight=1)
        left.grid_rowconfigure(2, weight=1)

        # Camera strip
        cam_bar = ctk.CTkFrame(left)
        cam_bar.grid(row=0, column=0, columnspan=3, sticky="ew", padx=10, pady=(10,5))
        cam_bar.grid_columnconfigure((0,1,2), weight=1)
        self.cam_labels = []
        for i in range(3):
            lbl = ctk.CTkLabel(cam_bar, text=f"Camera {i}", compound="top")
            lbl.grid(row=0, column=i, padx=6, pady=6, sticky="ew")
            self.cam_labels.append(lbl)

        # NEW: Sensor selection module (transverse frame)
        sel = ctk.CTkFrame(left)
        sel.grid(row=1, column=0, columnspan=3, sticky="ew", padx=10, pady=(0,10))
        sel.grid_columnconfigure((0,1,2,3,4,5,6), weight=1)
        ctk.CTkLabel(sel, text="Select Active Sensors on Frame:", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, padx=8, pady=6, sticky="w")

        # Simple canvas to hint positions (L to R)
        canvas = ctk.CTkCanvas(sel, width=780, height=60, bg="#f4f4f4", highlightthickness=0)
        canvas.grid(row=1, column=0, columnspan=7, padx=8, pady=(0,8), sticky="ew")
        w, h = 760, 40
        x0, y0 = 10, 10
        canvas.create_rectangle(x0, y0, x0+w, y0+h, fill="#ffd24d", outline="#999")  # frame bar
        positions = [0.1, 0.3, 0.5, 0.7, 0.9]
        self.sensor_checks = []
        for idx, px in enumerate(positions):
            cx = x0 + int(px*w)
            canvas.create_line(cx, y0, cx, y0+h, fill="#333")
            var = ctk.BooleanVar(value=True)
            chk = ctk.CTkCheckBox(sel, text=f"S{idx+1}", variable=var, command=self._on_sensor_toggle)
            chk.grid(row=2, column=idx+1, padx=4, pady=(0,8))
            self.sensor_checks.append(var)

        # Data table
        table_frame = ctk.CTkFrame(left)
        table_frame.grid(row=2, column=0, columnspan=3, sticky="nsew", padx=10, pady=(5,10))
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)

        columns = ("Bin end (m)", "TOF1 (mm)", "TOF2 (mm)", "TOF3 (mm)", "TOF4 (mm)", "TOF5 (mm)")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=12)
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=140, anchor="center")
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=vsb.set)
        vsb.grid(row=0, column=1, sticky="ns")

    # -------- Right sidebar --------
    def build_right(self):
        side = ctk.CTkFrame(self, width=300, corner_radius=12)
        side.grid(row=0, column=1, sticky="ns", padx=(0,10), pady=10)
        side.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(side, text="Survey Data", font=ctk.CTkFont(size=18, weight="bold")).grid(row=0, column=0, padx=10, pady=(12,6), sticky="w")

        self.speed_var = ctk.StringVar(value="0.0 km/h")
        self.dist_var  = ctk.StringVar(value="0.0 m")
        self.lat_var   = ctk.StringVar(value="--")
        self.lon_var   = ctk.StringVar(value="--")
        self.alt_var   = ctk.StringVar(value="--")

        self.speed_bar = ctk.CTkProgressBar(side, height=18)
        self.speed_bar.set(0)
        self.speed_bar.grid(row=1, column=0, padx=12, pady=(8,4), sticky="ew")
        ctk.CTkLabel(side, textvariable=self.speed_var, font=ctk.CTkFont(size=14)).grid(row=2, column=0, padx=12, pady=(0,12), sticky="w")

        ctk.CTkLabel(side, text="Distance", font=ctk.CTkFont(weight="bold")).grid(row=3, column=0, padx=12, pady=(4,0), sticky="w")
        ctk.CTkLabel(side, textvariable=self.dist_var).grid(row=4, column=0, padx=12, pady=(0,10), sticky="w")

        ctk.CTkLabel(side, text="GPS", font=ctk.CTkFont(weight="bold")).grid(row=5, column=0, padx=12, pady=(4,0), sticky="w")
        ctk.CTkLabel(side, textvariable=self.lat_var).grid(row=6, column=0, padx=12, pady=(0,0), sticky="w")
        ctk.CTkLabel(side, textvariable=self.lon_var).grid(row=7, column=0, padx=12, pady=(0,0), sticky="w")
        ctk.CTkLabel(side, textvariable=self.alt_var).grid(row=8, column=0, padx=12, pady=(0,10), sticky="w")

        btn_row = ctk.CTkFrame(side)
        btn_row.grid(row=9, column=0, padx=12, pady=10, sticky="ew")
        btn_row.grid_columnconfigure((0,1,2), weight=1)
        self.start_btn = ctk.CTkButton(btn_row, text="Start", command=self.on_start)
        self.start_btn.grid(row=0, column=0, padx=6, pady=6, sticky="ew")
        self.stop_btn = ctk.CTkButton(btn_row, text="Stop", command=self.on_stop)
        self.stop_btn.grid(row=0, column=1, padx=6, pady=6, sticky="ew")
        self.export_btn = ctk.CTkButton(btn_row, text="Export", command=self.on_export)
        self.export_btn.grid(row=0, column=2, padx=6, pady=6, sticky="ew")

        self.status_lbl = ctk.CTkLabel(side, text="Status: idle")
        self.status_lbl.grid(row=10, column=0, padx=12, pady=(0,12), sticky="w")

        ctk.CTkLabel(side, text="Averages every 10 m\n(TOF @100 Hz)", justify="left").grid(row=11, column=0, padx=12, pady=(0,12), sticky="w")

    # -------- Thread/queue polling --------
    def poll_cam_frames(self):
        try:
            from PIL import Image, ImageTk
            while True:
                cam_idx, frame = self.cam_queue.get_nowait()
                rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                imgtk = ImageTk.PhotoImage(image=Image.fromarray(rgb))
                lbl = self.cam_labels[min(cam_idx, len(self.cam_labels)-1)]
                lbl.configure(image=imgtk)
                lbl.image = imgtk
        except queue.Empty:
            pass
        self.after(30, self.poll_cam_frames)

    def poll_ui_queue(self):
        try:
            while True:
                msg = self.ui_queue.get_nowait()
                if msg["type"] == "telemetry":
                    spd = msg["speed_kmph"]
                    self.speed_var.set(f"{spd:0.1f} km/h")
                    self.speed_bar.set(min(1.0, spd/120.0))
                    self.dist_var.set(f"{msg['distance_m']:,.1f} m")
                    self.lat_var.set(f"Lat: {msg['lat']:.6f}")
                    self.lon_var.set(f"Lon: {msg['lon']:.6f}")
                    self.alt_var.set(f"Alt: {msg['alt']:.1f} m")
                elif msg["type"] == "bin":
                    end_m = msg["bin_end_m"]
                    av = msg["avg_tof_mm"]
                    values = [f"{end_m:,.1f}"] + [f"{v:0.1f}" if not math.isnan(v) else "--" for v in av]
                    self.tree.insert("", "end", values=values)
                    self.tree.see(self.tree.get_children()[-1])
        except queue.Empty:
            pass
        self.after(60, self.poll_ui_queue)

    # -------- Sensor enable mask --------
    def _on_sensor_toggle(self):
        with self._mask_lock:
            self.sensor_enabled = [var.get() for var in self.sensor_checks]

    def _get_enabled_mask(self):
        with self._mask_lock:
            return list(self.sensor_enabled)

    # -------- Controls --------
    def on_start(self):
        if self.running:
            return
        self.running = True
        self.status_lbl.configure(text="Status: running")
        # Start cameras
        if not self.cams:
            self.cams = [CameraWorker(i, self.cam_queue) for i in CAM_INDEXES]
            for c in self.cams:
                c.start()
        # Start sensors
        self.sensor_worker = SensorWorker(self.ui_queue, self._get_enabled_mask)
        self.sensor_worker.start()

    def on_stop(self):
        if not self.running:
            return
        self.status_lbl.configure(text="Status: stopped")
        # Stop sensors
        if self.sensor_worker:
            self.sensor_worker.stop()
            self.sensor_worker = None
        # Stop cameras
        for c in self.cams:
            c.stop()
        self.cams = []
        self.running = False

    def on_export(self):
        # Gather table rows
        cols = ["Bin end (m)", "TOF1 (mm)", "TOF2 (mm)", "TOF3 (mm)", "TOF4 (mm)", "TOF5 (mm)"]
        rows = []
        for iid in self.tree.get_children():
            rows.append(self.tree.item(iid, 'values'))
        if not rows:
            messagebox.showinfo("Export", "No data to export.")
            return

        # Choose file
        fpath = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook", ".xlsx"), ("CSV", ".csv")],
            initialfile="nsv_data.xlsx",
            title="Export data")
        if not fpath:
            return

        try:
            if fpath.lower().endswith('.csv'):
                # CSV export without pandas
                with open(fpath, 'w', encoding='utf-8') as f:
                    f.write(','.join(cols) + "\n")
                    for r in rows:
                        f.write(','.join(map(str, r)) + "\n")
            else:
                # Excel needs pandas+openpyxl
                if pd is None:
                    messagebox.showerror("Export", "Install pandas and openpyxl for Excel export, or export as CSV.")
                    return
                df = pd.DataFrame(rows, columns=cols)
                df.to_excel(fpath, index=False)
            messagebox.showinfo("Export", f"Saved to:\n{fpath}")
        except Exception as e:
            messagebox.showerror("Export failed", str(e))

    def on_close(self):
        try:
            if self.sensor_worker:
                self.sensor_worker.stop()
            for c in self.cams:
                c.stop()
        finally:
            self.destroy()

if __name__ == "__main__":
    app = NSVApp()
    app.mainloop()
