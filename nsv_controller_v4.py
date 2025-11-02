# NSV – Controller v4
# Changes vs v3:
# 1) Export pauses data collection (sensors are paused; camera preview continues)
# 2) Cameras preview from app launch (no need to press Start). Camera selection applies immediately
# 3) Sidebar metrics table (Rut, IRI, SMTD, RQI) with columns: Left Wheel Path, Right Wheel Path, Average
#    Rut is approximated each 10 m bin using 5-beam TOF; IRI/SMTD/RQI placeholders ('--') for future models

import os
import time
import math
import shutil
import tempfile
import threading
import queue
import random
import cv2
import numpy as np
import customtkinter as ctk
from tkinter import ttk, messagebox, filedialog

try:
    import pandas as pd
except Exception:
    pd = None

BIN_METERS = 10.0
TOF_HZ = 100
CAM_INDEXES = [0, 1, 2]
CAM_RES = (400, 225)
CAM_FPS = 30

# -----------------------------
# Utilities
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
# Camera worker (preview always on; writing when writer provided)
# -----------------------------
class CameraWorker(threading.Thread):
    def __init__(self, cam_index, out_queue, get_writer, width=CAM_RES[0], height=CAM_RES[1]):
        super().__init__(daemon=True)
        self.cam_index = cam_index
        self.out_queue = out_queue
        self.get_writer = get_writer  # callable -> VideoWriter or None
        self.width = width
        self.height = height
        self.stop_event = threading.Event()
        self.cap = None

    def run(self):
        try:
            self.cap = cv2.VideoCapture(self.cam_index, cv2.CAP_DSHOW)
            if not (self.cap and self.cap.isOpened()):
                while not self.stop_event.is_set():
                    frame = make_testcard(self.width, self.height, f"CAM {self.cam_index} NOT FOUND")
                    self.out_queue.put((self.cam_index, frame))
                    time.sleep(1.0 / CAM_FPS)
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
                writer = self.get_writer(self.cam_index)
                if writer is not None:
                    writer.write(frame)
                time.sleep(max(0.0, 1.0 / CAM_FPS))
        finally:
            if self.cap is not None:
                self.cap.release()

    def stop(self):
        self.stop_event.set()

# -----------------------------
# Simulated sensors (replace with real hardware reads)
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
    base = 800
    return [base + random.gauss(0, 5) for _ in range(5)]

# -----------------------------
# Sensor worker with pause + reset
# -----------------------------
class SensorWorker(threading.Thread):
    def __init__(self, ui_queue, paused_event, enabled_mask_getter):
        super().__init__(daemon=True)
        self.ui_queue = ui_queue
        self.paused_event = paused_event
        self.enabled_mask_getter = enabled_mask_getter
        self.stop_event = threading.Event()
        self.reset()

    def reset(self):
        self.distance_m = 0.0
        self.last_t = time.time()
        self.next_bin_edge = BIN_METERS
        self.bin_buffers = [[] for _ in range(5)]
        self.last_speed = 0.0
        self.last_gps = (None, None, None)

    def run(self):
        while not self.stop_event.is_set():
            t = time.time()
            if self.paused_event.is_set():
                self.last_t = t
                time.sleep(0.01)
                continue

            speed_kmph = sim_speed_kmph(t)
            speed_mps = speed_kmph / 3.6
            dt = t - self.last_t
            self.last_t = t
            self.distance_m += speed_mps * dt
            lat, lon, alt = sim_gps()
            tof_raw = sim_tof_values()
            enabled = self.enabled_mask_getter()
            tof = [v if enabled[i] else float('nan') for i, v in enumerate(tof_raw)]
            for i, v in enumerate(tof):
                self.bin_buffers[i].append(v)

            self.last_speed = speed_kmph
            self.last_gps = (lat, lon, alt)

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
                    "avg_tof_mm": avg,
                    "speed_kmph": self.last_speed,
                    "lat": lat,
                    "lon": lon,
                    "alt": alt,
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
        self.title("NSV – Controller v4")
        self.geometry("1500x920")

        self.ui_queue = queue.Queue()
        self.cam_queue = queue.Queue()

        # runtime state
        self.cameras = {}  # cam_idx -> CameraWorker
        self.sensor_worker = None
        self.running = False
        self.paused_event = threading.Event()  # set => paused (sensors only)

        # Writers and temp paths
        self.video_writers = {}
        self.video_temp_paths = {}

        # masks & locks
        self._mask_lock = threading.Lock()
        self.sensor_enabled = [True]*5
        self.camera_enabled = [True]*len(CAM_INDEXES)

        # Layout
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=0)
        self.grid_rowconfigure(0, weight=1)

        self.build_left()
        self.build_right()

        # Start camera previews immediately using selected cams
        self._on_camera_toggle()
        for enabled, cam_idx in zip(self.camera_enabled, CAM_INDEXES):
            if enabled:
                self._start_camera(cam_idx)

        self.after(30, self.poll_cam_frames)
        self.after(60, self.poll_ui_queue)
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    # -------- Left area --------
    def build_left(self):
        left = ctk.CTkFrame(self, corner_radius=12)
        left.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        left.grid_columnconfigure((0,1,2), weight=1)
        left.grid_rowconfigure(3, weight=1)

        # Camera strip
        cam_bar = ctk.CTkFrame(left)
        cam_bar.grid(row=0, column=0, columnspan=3, sticky="ew", padx=10, pady=(10,5))
        cam_bar.grid_columnconfigure((0,1,2), weight=1)
        self.cam_labels = []
        for i in range(3):
            lbl = ctk.CTkLabel(cam_bar, text=f"Camera {i}", compound="top")
            lbl.grid(row=0, column=i, padx=6, pady=6, sticky="ew")
            self.cam_labels.append(lbl)

        # Camera selection row
        cam_sel = ctk.CTkFrame(left)
        cam_sel.grid(row=1, column=0, columnspan=3, sticky="ew", padx=10, pady=(0,8))
        cam_sel.grid_columnconfigure((0,1,2,3), weight=1)
        ctk.CTkLabel(cam_sel, text="Select Cameras:", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, padx=8, pady=6, sticky="w")
        self.cam_checks = []
        for idx, cam in enumerate(CAM_INDEXES):
            var = ctk.BooleanVar(value=True)
            chk = ctk.CTkCheckBox(cam_sel, text=f"Cam {cam}", variable=var, command=lambda cidx=cam, v=var: self._toggle_camera(cidx, v))
            chk.grid(row=0, column=idx+1, padx=6, pady=6)
            self.cam_checks.append(var)

        # Sensor selection (frame schematic)
        sel = ctk.CTkFrame(left)
        sel.grid(row=2, column=0, columnspan=3, sticky="ew", padx=10, pady=(0,10))
        sel.grid_columnconfigure((0,1,2,3,4,5,6), weight=1)
        ctk.CTkLabel(sel, text="Select Active Sensors on Frame:", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, padx=8, pady=6, sticky="w")
        canvas = ctk.CTkCanvas(sel, width=780, height=60, bg="#f4f4f4", highlightthickness=0)
        canvas.grid(row=1, column=0, columnspan=7, padx=8, pady=(0,8), sticky="ew")
        w, h = 760, 40
        x0, y0 = 10, 10
        canvas.create_rectangle(x0, y0, x0+w, y0+h, fill="#ffd24d", outline="#999")
        positions = [0.1, 0.3, 0.5, 0.7, 0.9]
        self.sensor_checks = []
        for idx, px in enumerate(positions):
            cx = x0 + int(px*w)
            canvas.create_line(cx, y0, cx, y0+h, fill="#333")
            var = ctk.BooleanVar(value=True)
            chk = ctk.CTkCheckBox(sel, text=f"S{idx+1}", variable=var, command=self._on_sensor_toggle)
            chk.grid(row=2, column=idx+1, padx=4, pady=(0,8))
            self.sensor_checks.append(var)

        # Data table (bins)
        table_frame = ctk.CTkFrame(left)
        table_frame.grid(row=3, column=0, columnspan=3, sticky="nsew", padx=10, pady=(5,10))
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)

        self.columns = (
            "Bin end (m)", "Speed (km/h)", "Latitude", "Longitude", "Altitude (m)",
            "TOF1 (mm)", "TOF2 (mm)", "TOF3 (mm)", "TOF4 (mm)", "TOF5 (mm)"
        )
        self.tree = ttk.Treeview(table_frame, columns=self.columns, show="headings", height=12)
        widths = [120, 120, 130, 130, 120] + [110]*5
        for col, wcol in zip(self.columns, widths):
            self.tree.heading(col, text=col)
            self.tree.column(col, width=wcol, anchor="center")
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscroll=vsb.set)
        vsb.grid(row=0, column=1, sticky="ns")

    # -------- Right sidebar --------
    def build_right(self):
        side = ctk.CTkFrame(self, width=360, corner_radius=12)
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

        # Buttons
        btn_row = ctk.CTkFrame(side)
        btn_row.grid(row=9, column=0, padx=12, pady=10, sticky="ew")
        btn_row.grid_columnconfigure((0,1,2,3), weight=1)
        self.start_btn = ctk.CTkButton(btn_row, text="Start", command=self.on_start)
        self.start_btn.grid(row=0, column=0, padx=6, pady=6, sticky="ew")
        self.pause_btn = ctk.CTkButton(btn_row, text="Pause", command=self.on_pause_toggle)
        self.pause_btn.grid(row=0, column=1, padx=6, pady=6, sticky="ew")
        self.reset_btn = ctk.CTkButton(btn_row, text="Reset", command=self.on_reset)
        self.reset_btn.grid(row=0, column=2, padx=6, pady=6, sticky="ew")
        self.stop_btn = ctk.CTkButton(btn_row, text="Stop", command=self.on_stop)
        self.stop_btn.grid(row=0, column=3, padx=6, pady=6, sticky="ew")

        self.export_btn = ctk.CTkButton(side, text="Export", command=self.on_export)
        self.export_btn.grid(row=10, column=0, padx=12, pady=(0,10), sticky="ew")

        self.status_lbl = ctk.CTkLabel(side, text="Status: idle")
        self.status_lbl.grid(row=11, column=0, padx=12, pady=(0,12), sticky="w")

        # Metrics table (Right sidebar) – rows: Rut, IRI, SMTD, RQI / cols: LWP, RWP, Avg
        metrics_frame = ctk.CTkFrame(side)
        metrics_frame.grid(row=12, column=0, padx=12, pady=(6,12), sticky="nsew")
        metrics_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkLabel(metrics_frame, text="Summary Metrics", font=ctk.CTkFont(weight="bold")).grid(row=0, column=0, sticky="w", padx=8, pady=6)
        self.metrics = ttk.Treeview(metrics_frame, columns=("Left Wheel Path", "Right Wheel Path", "Average"), show="headings", height=4)
        for col in ("Left Wheel Path", "Right Wheel Path", "Average"):
            self.metrics.heading(col, text=col)
            self.metrics.column(col, width=110, anchor="center")
        self.metrics.grid(row=1, column=0, sticky="nsew")
        # Prepopulate rows with placeholders using item text as row labels
        self.metric_rows = {
            'Rut (mm)': self.metrics.insert('', 'end', text='Rut (mm)', values=("--","--","--")),
            'IRI (m/km)': self.metrics.insert('', 'end', text='IRI (m/km)', values=("--","--","--")),
            'SMTD (mm)': self.metrics.insert('', 'end', text='SMTD (mm)', values=("--","--","--")),
            'RQI': self.metrics.insert('', 'end', text='RQI', values=("--","--","--")),
        }
        # Show row labels by using the tree column
        self.metrics.configure(show='tree headings')
        self.metrics.heading('#0', text='Metric')
        self.metrics.column('#0', width=120, anchor='w')

    # -------- Camera helpers --------
    def _start_camera(self, cam_idx):
        if cam_idx in self.cameras:
            return
        cw = CameraWorker(cam_idx, self.cam_queue, self.get_writer_for)
        self.cameras[cam_idx] = cw
        cw.start()

    def _stop_camera(self, cam_idx):
        cw = self.cameras.pop(cam_idx, None)
        if cw:
            cw.stop()

    def _toggle_camera(self, cam_idx, var):
        # immediate start/stop of preview
        if var.get():
            self._start_camera(cam_idx)
        else:
            self._stop_camera(cam_idx)
        self._on_camera_toggle()

    def _on_camera_toggle(self):
        self.camera_enabled = [var.get() for var in getattr(self, 'cam_checks', [])] or [True]*len(CAM_INDEXES)

    # -------- Polling --------
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
                    values = [
                        f"{msg['bin_end_m']:,.1f}",
                        f"{msg['speed_kmph']:.1f}",
                        f"{msg['lat']:.6f}",
                        f"{msg['lon']:.6f}",
                        f"{msg['alt']:.1f}",
                    ]
                    av = msg["avg_tof_mm"]
                    values.extend([f"{v:0.1f}" if not math.isnan(v) else "--" for v in av])
                    self.tree.insert("", "end", values=values)
                    self.tree.see(self.tree.get_children()[-1])

                    # Update simple Rut metric (approx): Left = avg(S1,S2), Right = avg(S4,S5)
                    lwp = np.nanmean(av[0:2]) if not np.isnan([av[0], av[1]]).all() else np.nan
                    rwp = np.nanmean(av[3:5]) if not np.isnan([av[3], av[4]]).all() else np.nan
                    avgp = np.nanmean([lwp, rwp])
                    # Rut as spread across beams
                    rut = np.nanmax(av) - np.nanmin(av) if len([a for a in av if not np.isnan(a)]) >= 2 else np.nan
                    def fmt(x):
                        return "--" if (x is None or (isinstance(x, float) and (np.isnan(x) or np.isinf(x)))) else f"{x:0.1f}"
                    self.metrics.item(self.metric_rows['Rut (mm)'], values=(fmt(lwp), fmt(rwp), fmt(rut)))
                    # Placeholders for others (could be updated with real models later)
                    for key in ('IRI (m/km)', 'SMTD (mm)', 'RQI'):
                        self.metrics.item(self.metric_rows[key], values=("--","--","--"))
        except queue.Empty:
            pass
        self.after(60, self.poll_ui_queue)

    # -------- Sensor mask --------
    def _on_sensor_toggle(self):
        with self._mask_lock:
            self.sensor_enabled = [var.get() for var in self.sensor_checks]

    def _get_enabled_mask(self):
        with self._mask_lock:
            return list(self.sensor_enabled)

    # -------- Writers --------
    def get_writer_for(self, cam_idx):
        # Writer is present only while running and not paused
        if not self.running or self.paused_event.is_set():
            return None
        return self.video_writers.get(cam_idx)

    # -------- Controls --------
    def on_start(self):
        if self.running:
            return
        self.running = True
        self.status_lbl.configure(text="Status: running")
        self.paused_event.clear()

        # Ensure writers for selected cameras
        self._on_camera_toggle()
        self.video_writers = {}
        self.video_temp_paths = {}
        ts = int(time.time())
        for enabled, cam_idx in zip(self.camera_enabled, CAM_INDEXES):
            if not enabled:
                continue
            temp_path = os.path.join(tempfile.gettempdir(), f"nsv_run_cam{cam_idx}_{ts}.mp4")
            fourcc = cv2.VideoWriter_fourcc(*'mp4v')
            writer = cv2.VideoWriter(temp_path, fourcc, CAM_FPS, CAM_RES)
            self.video_temp_paths[cam_idx] = temp_path
            self.video_writers[cam_idx] = writer
            # Preview threads already running; no need to restart

        # Start sensors
        self.sensor_worker = SensorWorker(self.ui_queue, self.paused_event, self._get_enabled_mask)
        self.sensor_worker.start()

    def on_pause_toggle(self):
        if not self.running:
            return
        if self.paused_event.is_set():
            self.paused_event.clear()
            self.pause_btn.configure(text="Pause")
            self.status_lbl.configure(text="Status: running")
        else:
            self.paused_event.set()
            self.pause_btn.configure(text="Resume")
            self.status_lbl.configure(text="Status: paused")

    def on_reset(self):
        for iid in self.tree.get_children():
            self.tree.delete(iid)
        if self.sensor_worker:
            self.sensor_worker.reset()
        # Reset metrics table
        for key in self.metric_rows:
            self.metrics.item(self.metric_rows[key], values=("--","--","--"))
        self.status_lbl.configure(text="Status: reset")

    def on_stop(self):
        if not self.running:
            return
        self.on_export()  # Export also pauses automatically
        # Teardown sensor + writers; keep camera preview running
        if self.sensor_worker:
            self.sensor_worker.stop()
            self.sensor_worker = None
        for w in self.video_writers.values():
            try:
                w.release()
            except Exception:
                pass
        self.video_writers.clear()
        self.running = False
        self.pause_btn.configure(text="Pause")
        self.status_lbl.configure(text="Status: stopped")
        self.on_reset()

    def on_export(self):
        # Pause data collection as requested
        self.paused_event.set()
        self.pause_btn.configure(text="Resume")
        self.status_lbl.configure(text="Status: paused (export)")

        rows = [self.tree.item(iid, 'values') for iid in self.tree.get_children()]
        if not rows:
            messagebox.showinfo("Export", "No data to export.")
            return

        fpath = filedialog.asksaveasfilename(
            defaultextension=".xlsx",
            filetypes=[("Excel Workbook", ".xlsx"), ("CSV", ".csv")],
            initialfile="nsv_data.xlsx",
            title="Export data and videos")
        if not fpath:
            return

        try:
            # Save table
            if fpath.lower().endswith('.csv'):
                with open(fpath, 'w', encoding='utf-8') as f:
                    f.write(','.join(self.columns) + "\n")
                    for r in rows:
                        f.write(','.join(map(str, r)) + "\n")
            else:
                if pd is None:
                    messagebox.showerror("Export", "Install pandas and openpyxl for Excel export, or export as CSV.")
                    return
                df = pd.DataFrame(rows, columns=self.columns)
                df.to_excel(fpath, index=False)

            # Flush writers and copy mp4s
            out_dir = os.path.dirname(fpath)
            base = os.path.splitext(os.path.basename(fpath))[0]
            copied_any = False
            for cam_idx, writer in list(self.video_writers.items()):
                try:
                    writer.release()
                except Exception:
                    pass
                temp_path = self.video_temp_paths.get(cam_idx)
                if temp_path and os.path.exists(temp_path):
                    out_video = os.path.join(out_dir, f"{base}_cam{cam_idx}.mp4")
                    try:
                        shutil.copyfile(temp_path, out_video)
                        copied_any = True
                    except Exception as e:
                        print("Video export failed:", e)

            msg = f"Saved data to:\n{fpath}"
            if copied_any:
                msg += "\nSaved camera videos alongside the data file."
            messagebox.showinfo("Export", msg)
        except Exception as e:
            messagebox.showerror("Export failed", str(e))

    def on_close(self):
        try:
            if self.sensor_worker:
                self.sensor_worker.stop()
            for cw in self.cameras.values():
                cw.stop()
            for w in self.video_writers.values():
                try:
                    w.release()
                except Exception:
                    pass
        finally:
            self.destroy()

if __name__ == "__main__":
    app = NSVApp()
    app.mainloop()
