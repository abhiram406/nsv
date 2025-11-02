"""
NSV Controller v12 — record-size fixes + labeled telemetry
Updates from v11 (warnings-fixed):
1) Recording path and saved images are now rendered at RECORD_RES (not preview size).
   - CameraWorker generates BOTH preview (for UI) and record frames (for writer).
   - Images saved from latest_record_frame at RECORD_RES with header composed at that size.
2) Right panel shows explicit labels for Speed, Chainage, and Distance alongside values.
"""

import os, time, math, threading, queue, random
from datetime import datetime

import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox

import numpy as np
import cv2
from collections import deque

# Optional Excel support
try:
    import pandas as pd
    PANDAS_OK = True
except Exception:
    PANDAS_OK = False

try:
    import openpyxl  # noqa: F401
    OPENPYXL_OK = True
except Exception:
    OPENPYXL_OK = False

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

# ---------------------------
# Layout / Runtime constants
# ---------------------------
LEFT_WIDTH, RIGHT_WIDTH = 420, 420
WINDOW_GEOM = "1680x960"

CAM_INDEXES = [0, 1, 2, 3]
CAM_NAMES   = {0: "Front", 1: "Left", 2: "Right", 3: "Back"}
PREVIEW_RES = (360, 202)     # fast UI
RECORD_RES  = (1920, 1080)    # bump to 720p for clarity
CAM_FPS     = 25
TOF_HZ      = 100
TEN_M_METERS= 10.0
TEN_M_KM    = 0.01
NEON_GREEN  = "#39ff14"
HEADER_H    = 64
HEADER_COLOR= (255, 0, 0)    # BGR pure blue
TEXT_COLOR  = (255, 255, 255)
FONT        = cv2.FONT_HERSHEY_SIMPLEX
BG_COLOR    = "#0b0f14"
GRID_COLOR  = "#263238"
SPINE_COLOR = "#607d8b"

# ---------------------------
# Helpers
# ---------------------------
now_local_str = lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")
ts_for_path   = lambda: datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def make_testcard(w, h, text="NO CAMERA"):
    img = np.zeros((h, w, 3), dtype=np.uint8)
    bars = [(255,255,255),(255,255,0),(0,255,255),(0,255,0),(255,0,255),(255,0,0),(0,0,255)]
    bw = max(1, w // len(bars))
    for i, c in enumerate(bars):
        img[:, i*bw:(i+1)*bw] = c
    cv2.putText(img, text, (10, h//2), FONT, 0.9, (0,0,0), 3, cv2.LINE_AA)
    cv2.putText(img, text, (10, h//2), FONT, 0.9, (255,255,255), 1, cv2.LINE_AA)
    return img

def draw_header_to_size(frame, ctx, size):
    """Compose header at target size (w,h) and return new frame."""
    w, h = size
    if frame is None or frame.size == 0:
        frame = np.zeros((h, w, 3), dtype=np.uint8)
    else:
        frame = cv2.resize(frame, (w, h))
    header_h = min(HEADER_H, frame.shape[0]//4)
    cv2.rectangle(frame, (0,0), (frame.shape[1], header_h), HEADER_COLOR, -1)
    col1 = [f"Project ID: {ctx.get('survey','--')}", f"NH Number: {ctx.get('nh','--')}", f"Old NH Number: {ctx.get('oldnh','--')}", f"Direction: {ctx.get('direction','--')}"]
    col2 = [f"Lane no.: {ctx.get('lane','--')}", f"Latitude: {ctx.get('lat','--')}", f"Longitude: {ctx.get('lon','--')}", f"Altitude: {ctx.get('alt','--')} m"]
    col3 = [f"Distance: {ctx.get('distance','--')} m", f"Chainage: {ctx.get('chainage','--')}", f"Speed: {ctx.get('speed','--')} km/h", f"{ctx.get('date','--')}  {ctx.get('time','--')}"]
    cols = [col1, col2, col3]
    pad_x, line_h = 16, 20
    col_w = frame.shape[1] // 3
    for ci, lines in enumerate(cols):
        x = pad_x + ci*col_w
        for li, text in enumerate(lines):
            yy = 24 + li*line_h
            if yy < header_h - 4:
                cv2.putText(frame, text, (x, yy), FONT, 0.6, TEXT_COLOR, 1, cv2.LINE_AA)
    return frame

# ---------------------------
# Camera Worker
# ---------------------------
class CameraWorker(threading.Thread):
    def __init__(self, cam_index, out_queue, get_writer, get_overlay_ctx,
                 preview_size=PREVIEW_RES, record_size=RECORD_RES):
        super().__init__(daemon=True)
        self.cam_index = cam_index
        self.out_queue = out_queue
        self.get_writer = get_writer
        self.get_overlay_ctx = get_overlay_ctx
        self.preview_size = preview_size
        self.record_size = record_size
        self.stop_event = threading.Event()
        self.cap = None
        self.latest_preview_frame = None
        self.latest_record_frame = None

    def _try_open(self, backend_flag):
        cap = cv2.VideoCapture(self.cam_index, backend_flag)
        return cap if (cap and cap.isOpened()) else None

    def _open_cap(self):
        # Prefer MSMF on Windows, then DSHOW, finally ANY
        for backend in (cv2.CAP_MSMF, cv2.CAP_DSHOW, cv2.CAP_ANY):
            cap = self._try_open(backend)
            if cap is not None:
                self.cap = cap
                return True
        return False

    def run(self):
        try:
            if not self._open_cap():
                while not self.stop_event.is_set():
                    ctx = self.get_overlay_ctx()
                    # Generate both preview & record frames at their own sizes
                    preview = draw_header_to_size(make_testcard(*self.preview_size[::-1], "CAM NOT FOUND"),
                                                 ctx, self.preview_size)
                    record  = draw_header_to_size(make_testcard(*self.record_size[::-1], "CAM NOT FOUND"),
                                                 ctx, self.record_size)
                    self.latest_preview_frame = preview
                    self.latest_record_frame  = record
                    self.out_queue.put((self.cam_index, preview))
                    writer = self.get_writer(self.cam_index)
                    if writer is not None:
                        writer.write(record)  # already at RECORD_RES
                    time.sleep(1.0/CAM_FPS)
                return

            while not self.stop_event.is_set():
                ok, frame = self.cap.read()
                if not ok or frame is None or frame.size == 0:
                    # Keep UI alive even if camera drops
                    frame = make_testcard(*self.record_size[::-1], f"{CAM_NAMES.get(self.cam_index, f'CAM {self.cam_index}')} ERROR")
                ctx = self.get_overlay_ctx()
                # Compose at both sizes
                preview = draw_header_to_size(frame, ctx, self.preview_size)
                record  = draw_header_to_size(frame, ctx, self.record_size)
                self.latest_preview_frame = preview
                self.latest_record_frame  = record
                self.out_queue.put((self.cam_index, preview))
                writer = self.get_writer(self.cam_index)
                if writer is not None:
                    writer.write(record)  # exact size
                time.sleep(max(0, 1.0/CAM_FPS))
        finally:
            if self.cap is not None:
                self.cap.release()

    def stop(self):
        self.stop_event.set()

# ---------------------------
# Simulated Inputs (replace with real drivers)
# ---------------------------
def sim_speed_kmph(t):
    return max(0.0, 45 + 10*math.sin(2*math.pi*t/20.0) + random.uniform(-3,3))

def sim_gps():
    lat = 17.385 + random.uniform(-1e-4, 1e-4)
    lon = 78.486 + random.uniform(-1e-4, 1e-4)
    alt = 505 + random.uniform(-1, 1)
    return lat, lon, alt

def sim_tof_values():
    return [800 + random.gauss(0, 4) for _ in range(6)]

# ---------------------------
# Sensor Worker
# ---------------------------
class SensorWorker(threading.Thread):
    def __init__(self, ui_queue, paused_event, calib_getter, ten_m_callback, sensor_selected_getter, raw_setter):
        super().__init__(daemon=True)
        self.ui_queue = ui_queue
        self.paused_event = paused_event
        self.calib_getter = calib_getter
        self.ten_m_callback = ten_m_callback
        self.sensor_selected_getter = sensor_selected_getter
        self.raw_setter = raw_setter
        self.stop_event = threading.Event()
        self.reset()

    def reset(self):
        self.distance_m = 0.0
        self.last_t = time.time()
        self.next_ten_m_edge = TEN_M_METERS
        self.bin_buffers = [[] for _ in range(6)]
        self.last_speed = 0.0

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
            raw = sim_tof_values()
            try: self.raw_setter(raw)
            except Exception: pass

            offs_val = self.calib_getter()
            vals = [abs(raw[i] - offs_val) for i in range(6)]
            for i, v in enumerate(vals):
                self.bin_buffers[i].append(v)

            self.last_speed = speed_kmph
            self.ui_queue.put({
                "type": "telemetry",
                "speed_kmph": speed_kmph,
                "distance_m": self.distance_m,
                "lat": lat, "lon": lon, "alt": alt,
            })

            if self.distance_m >= self.next_ten_m_edge:
                avgs = [float(np.nanmean(buf)) if len(buf) else float("nan") for buf in self.bin_buffers]
                self.bin_buffers = [[] for _ in range(6)]
                self.ui_queue.put({
                    "type": "ten_m",
                    "avg_s": avgs,
                    "speed_kmph": self.last_speed,
                    "lat": lat, "lon": lon, "alt": alt,
                })
                self.ten_m_callback(avgs, self.last_speed, (lat, lon, alt))
                self.next_ten_m_edge += TEN_M_METERS

            time.sleep(max(0.0, 1.0/TOF_HZ - (time.time()-t)))

    def stop(self):
        self.stop_event.set()

# ---------------------------
# Main App
# ---------------------------
class NSVApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        self.title("NSV Controller v12")
        self.geometry(WINDOW_GEOM)

        self.grid_columnconfigure(0, weight=0, minsize=LEFT_WIDTH)
        self.grid_columnconfigure(1, weight=1)
        self.grid_columnconfigure(2, weight=0, minsize=RIGHT_WIDTH)
        self.grid_rowconfigure(0, weight=1)

        self.ui_queue = queue.Queue()
        self.cam_queue = queue.Queue()
        self.cameras = {}
        self.sensor_worker = None

        self.running = False
        self.paused_event = threading.Event()
        self.project = None
        self.calib_value = 0.0
        self.controls_enabled = False

        self.cam_selected = {idx: tk.BooleanVar(value=True) for idx in CAM_INDEXES}
        self.sensor_selected = [tk.BooleanVar(value=True) for _ in range(6)]

        self.video_writers = {}
        self.run_dir = self.images_dir = self.stream_dir = self.data_dir = None

        self.table1_rows, self.table2_rows = [], []
        self.measure_accum_km = 0.0
        self.window_buffer = []
        self.current_chainage_km = None

        self.last_lat = self.last_lon = self.last_alt = None
        self.last_speed = 0.0
        self.last_distance_m = 0.0
        self.latest_raw = [float('nan')]*6

        self.build_menu(); self.build_layout()
        self.left.configure(width=LEFT_WIDTH);  self.left.grid_propagate(False)
        self.right.configure(width=RIGHT_WIDTH); self.right.grid_propagate(False)

        self.set_controls_state(False)
        self.set_selection_state(True)

        for idx in CAM_INDEXES: self.start_camera(idx)
        self.after(30, self.poll_cam_frames)
        self.after(60, self.poll_ui_queue)

    # ---------- Menu ----------
    def build_menu(self):
        menubar = tk.Menu(self)
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="New Project", command=self.new_project)
        file_menu.add_separator(); file_menu.add_command(label="Exit", command=self.on_close)
        menubar.add_cascade(label="File", menu=file_menu)
        cal_menu = tk.Menu(menubar, tearoff=0)
        cal_menu.add_command(label="Zero Calibrate Sensors", command=self.open_calibrate)
        menubar.add_cascade(label="Calibrate", menu=cal_menu)
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="User Manual", command=self.open_manual)
        menubar.add_cascade(label="Help", menu=help_menu)
        self.config(menu=menubar)

    # ---------- Layout ----------
    def build_layout(self):
        self.left = ctk.CTkFrame(self, corner_radius=12)
        self.left.grid(row=0, column=0, sticky="nsw", padx=(8,4), pady=8)
        self.left.grid_columnconfigure(0, weight=0, minsize=160)
        self.left.grid_columnconfigure(1, weight=1)
        self.build_left_panel(self.left)

        self.center = ctk.CTkFrame(self, corner_radius=12)
        self.center.grid(row=0, column=1, sticky="nsew", padx=4, pady=8)
        self.center.grid_columnconfigure((0,1), weight=1)
        self.center.grid_rowconfigure(0, weight=0)
        self.center.grid_rowconfigure(1, weight=1)
        self.build_cameras(self.center)
        self.build_graphs(self.center)

        self.right = ctk.CTkFrame(self, corner_radius=12)
        self.right.grid(row=0, column=2, sticky="nse", padx=(4,8), pady=8)
        self.right.grid_columnconfigure(0, weight=1)
        self.build_right_panel(self.right)

    # ---------- Left Panel ----------
    def build_left_panel(self, f):
        pad = {"padx":8, "pady":4}
        ctk.CTkLabel(f, text="Project Setup", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, sticky="w", **pad)
        def add_row(r, label, widget):
            ctk.CTkLabel(f, text=label, anchor="e").grid(row=r, column=0, sticky="e", **pad)
            widget.grid(row=r, column=1, sticky="ew", **pad)
        entry_w = 180
        self.ent_survey   = ctk.CTkEntry(f, width=entry_w)
        self.ent_client   = ctk.CTkEntry(f, width=entry_w)
        self.ent_nh       = ctk.CTkEntry(f, width=entry_w)
        self.ent_oldnh    = ctk.CTkEntry(f, width=entry_w)
        self.ent_section  = ctk.CTkEntry(f, width=entry_w)
        add_row(1,  "Survey ID",        self.ent_survey)
        add_row(2,  "Client Name/ID",   self.ent_client)
        add_row(3,  "NH Number",        self.ent_nh)
        add_row(4,  "Old NH Number",    self.ent_oldnh)
        add_row(5,  "Section Code",     self.ent_section)

        self.direction_var = ctk.StringVar(value="Increasing")
        self.direction_dd  = ctk.CTkOptionMenu(f, values=["Increasing", "Decreasing"], variable=self.direction_var, command=self.on_direction_select, width=entry_w)
        add_row(6, "Direction", self.direction_dd)

        self.lane_var = ctk.StringVar(value="L1")
        self.lane_dd  = ctk.CTkOptionMenu(f, values=[f"L{i}" for i in range(1,11)], variable=self.lane_var, width=entry_w)
        add_row(7, "Lane No.", self.lane_dd)

        self.cmb_pave = ctk.CTkComboBox(f, values=["Concrete", "Bituminous"], width=entry_w)
        self.cmb_pave.set("Bituminous")
        add_row(8, "Pavement Type", self.cmb_pave)

        self.ent_chainage   = ctk.CTkEntry(f, width=entry_w)
        self.ent_section_km = ctk.CTkEntry(f, width=entry_w)
        add_row(9,  "Initial Chainage (Km)",     self.ent_chainage)
        add_row(10, "Measurement Section (Km)",  self.ent_section_km)

        row0 = 11
        ctk.CTkLabel(f, text="Export Cameras", font=ctk.CTkFont(size=14, weight="bold")).grid(row=row0, column=0, columnspan=2, sticky="w", padx=8, pady=(12,4))
        cam_row = row0 + 1
        self.cam_checkboxes = []
        for i, cam_idx in enumerate(CAM_INDEXES):
            chk = ctk.CTkCheckBox(f, text=CAM_NAMES[cam_idx], variable=self.cam_selected[cam_idx])
            chk.grid(row=cam_row, column=i % 2, sticky="w", padx=8, pady=2)
            if i % 2 == 1: cam_row += 1
            self.cam_checkboxes.append(chk)

        sens_title_row = cam_row + 1
        ctk.CTkLabel(f, text="Export Sensors", font=ctk.CTkFont(size=14, weight="bold")).grid(row=sens_title_row, column=0, columnspan=2, sticky="w", padx=8, pady=(8,4))
        sens_row = sens_title_row + 1
        self.sensor_checkboxes = []
        for i in range(6):
            chk = ctk.CTkCheckBox(f, text=f"S{i+1}", variable=self.sensor_selected[i])
            chk.grid(row=sens_row + (i//3), column=i%3, sticky="w", padx=8, pady=2)
            self.sensor_checkboxes.append(chk)

        self.btn_set_specs = ctk.CTkButton(f, text="Set Project Specifications", command=self.set_project_specs)
        self.btn_set_specs.grid(row=sens_row+3, column=0, columnspan=2, sticky="ew", padx=8, pady=(16,8))
        self.lbl_proj_status = ctk.CTkLabel(f, text="Project not configured.")
        self.lbl_proj_status.grid(row=sens_row+4, column=0, columnspan=2, sticky="w", padx=8, pady=(0,8))

        self.left_widgets = [self.ent_survey, self.ent_client, self.ent_nh, self.ent_oldnh, self.ent_section,
                             self.direction_dd, self.lane_dd, self.cmb_pave, self.ent_chainage, self.ent_section_km, self.btn_set_specs]

    def on_direction_select(self, choice):
        lanes = [f"L{i}" for i in range(1, 11)] if choice == "Increasing" else [f"R{i}" for i in range(1, 11)]
        current = self.lane_var.get().strip()
        idx = max(1, min(10, int(current[1:]) if len(current) >= 2 and current[1:].isdigit() else 1)) - 1
        self.lane_dd.configure(values=lanes)
        self.lane_dd.set(lanes[idx]); self.lane_var.set(lanes[idx])

    # ---------- Centre: Cameras ----------
    def build_cameras(self, parent):
        f = ctk.CTkFrame(parent); f.grid(row=0, column=0, columnspan=2, sticky="ew", padx=8, pady=(8,4))
        for c in range(2): f.grid_columnconfigure(c, weight=1, uniform="cams")
        f.grid_rowconfigure(0, weight=0); f.grid_rowconfigure(1, weight=0)
        self.cam_labels = []
        for i, cam_idx in enumerate(CAM_INDEXES):
            r, c = divmod(i, 2); frame = ctk.CTkFrame(f)
            frame.grid(row=r, column=c, padx=6, pady=6, sticky="n")
            ctk.CTkLabel(frame, text=CAM_NAMES.get(cam_idx, f"Camera {cam_idx}")).pack(anchor="w")
            lbl = ctk.CTkLabel(frame, text="", width=PREVIEW_RES[0], height=PREVIEW_RES[1]); lbl.pack()
            self.cam_labels.append(lbl)

    # ---------- Centre: Graphs ----------
    def build_graphs(self, parent):
        gf = ctk.CTkFrame(parent); gf.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=8, pady=(4,8))
        gf.grid_rowconfigure(0, weight=1); gf.grid_columnconfigure(0, weight=1)
        fig = Figure(figsize=(9.6,4.6), dpi=100, facecolor=BG_COLOR)
        self.axes, self.lines = [], []
        for i in range(6):
            ax = fig.add_subplot(2,3,i+1, facecolor=BG_COLOR)
            ax.set_title(f"S{i+1}", fontsize=9, color="white")
            ax.set_xlabel(""); ax.set_ylabel("Val", fontsize=8, color="white")
            ax.set_xticks([]); ax.tick_params(axis='y', colors="white")
            for s in ax.spines.values(): s.set_color(SPINE_COLOR)
            ax.grid(True, color=GRID_COLOR, linewidth=0.6)
            self.axes.append(ax)
        self.lines = [ax.plot([], [], NEON_GREEN)[0] for ax in self.axes]
        self.graph_data = [deque(maxlen=600) for _ in range(6)]
        self.canvas_mpl = FigureCanvasTkAgg(fig, master=gf)
        self.canvas_mpl.draw(); self.canvas_mpl.get_tk_widget().pack(fill="both", expand=True)

    # ---------- Right Panel ----------
    def build_right_panel(self, parent):
        f, pad = parent, {"padx":10, "pady":4}
        ctk.CTkLabel(f, text="Telemetry & Controls", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, sticky="w", **pad)
        chip_frame = ctk.CTkFrame(f, fg_color="transparent"); chip_frame.grid(row=1, column=0, sticky="w", **pad)
        self.chip = ctk.CTkLabel(chip_frame, text="  ", width=16, height=16, corner_radius=8); self.chip.grid(row=0, column=0, padx=(0,6))
        self.set_status_color("red")
        self.lbl_status = ctk.CTkLabel(chip_frame, text="Status: stopped"); self.lbl_status.grid(row=0, column=1)

        # New labeled telemetry rows
        grid = ctk.CTkFrame(f); grid.grid(row=2, column=0, sticky="ew", padx=10, pady=(4,4))
        for i in range(2): grid.grid_columnconfigure(i, weight=1)
        self.var_speed = ctk.StringVar(value="0.0 km/h")
        self.var_dist  = ctk.StringVar(value="0.0 m")
        self.var_chain = ctk.StringVar(value="-- + 0.000")

        ctk.CTkLabel(grid, text="Speed:").grid(row=0, column=0, sticky="w", padx=(0,6))
        ctk.CTkLabel(grid, textvariable=self.var_speed).grid(row=0, column=1, sticky="w")
        ctk.CTkLabel(grid, text="Distance:").grid(row=1, column=0, sticky="w", padx=(0,6))
        ctk.CTkLabel(grid, textvariable=self.var_dist).grid(row=1, column=1, sticky="w")
        ctk.CTkLabel(grid, text="Chainage:").grid(row=2, column=0, sticky="w", padx=(0,6))
        ctk.CTkLabel(grid, textvariable=self.var_chain).grid(row=2, column=1, sticky="w")

        # GPS labels (unchanged)
        self.var_lat = ctk.StringVar(value="Lat: --")
        self.var_lon = ctk.StringVar(value="Lon: --")
        self.var_alt = ctk.StringVar(value="Alt: -- m")
        ctk.CTkLabel(f, textvariable=self.var_lat).grid(row=3, column=0, sticky="w", **pad)
        ctk.CTkLabel(f, textvariable=self.var_lon).grid(row=4, column=0, sticky="w", **pad)
        ctk.CTkLabel(f, textvariable=self.var_alt).grid(row=5, column=0, sticky="w", **pad)

        btns = ctk.CTkFrame(f); btns.grid(row=6, column=0, sticky="ew", padx=10, pady=(8,6))
        btns.grid_columnconfigure((0,1,2,3,4), weight=1)
        self.btn_start = ctk.CTkButton(btns, text="Start", command=self.on_start)
        self.btn_pause = ctk.CTkButton(btns, text="Pause", command=self.on_pause)
        self.btn_export= ctk.CTkButton(btns, text="Export", command=self.on_export)
        self.btn_stop  = ctk.CTkButton(btns, text="Stop", command=self.on_stop)
        self.btn_reset = ctk.CTkButton(btns, text="Reset", command=self.on_reset)
        for i, b in enumerate([self.btn_start, self.btn_pause, self.btn_export, self.btn_stop, self.btn_reset]):
            b.grid(row=0, column=i, padx=4, pady=4, sticky="ew")

    # ---------- Enable/Disable controls ----------
    def set_controls_state(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for btn in [self.btn_start, self.btn_pause, self.btn_export, self.btn_stop, self.btn_reset]:
            btn.configure(state=state)
        self.controls_enabled = enabled

    def set_selection_state(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for cb in getattr(self, 'cam_checkboxes', []): cb.configure(state=state)
        for cb in getattr(self, 'sensor_checkboxes', []): cb.configure(state=state)

    def set_left_state(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for w in self.left_widgets:
            try: w.configure(state=state)
            except Exception: pass

    # ---------- Cameras ----------
    def start_camera(self, cam_idx):
        if cam_idx in self.cameras: return
        cw = CameraWorker(cam_idx, self.cam_queue, self.get_writer_for, self.get_overlay_context)
        self.cameras[cam_idx] = cw
        cw.start()

    def stop_cameras(self):
        for cw in self.cameras.values(): cw.stop()
        for cw in self.cameras.values():
            try: cw.join(timeout=1.0)
            except Exception: pass
        self.cameras.clear()

    def get_writer_for(self, cam_idx):
        if not self.running or self.paused_event.is_set() or self.stream_dir is None:
            return None
        if not self.cam_selected[cam_idx].get():
            return None
        return self.video_writers.get(cam_idx)

    def get_overlay_context(self):
        p = self.project or {}
        lat = f"{self.last_lat:.6f}" if isinstance(self.last_lat, (int,float)) else "--"
        lon = f"{self.last_lon:.6f}" if isinstance(self.last_lon, (int,float)) else "--"
        alt = f"{self.last_alt:.1f}" if isinstance(self.last_alt, (int,float)) else "--"
        dist= f"{self.last_distance_m:.1f}"
        if self.project and self.current_chainage_km is not None:
            base = p['init_chain']; offset = self.current_chainage_km - base
            sign = '+' if offset >= 0 else '-'
            chain = f"{base:.0f} {sign} {abs(offset):.3f}"
        else:
            chain = "-- + 0.000"
        dt = datetime.now()
        return {
            'survey': p.get('survey','--'), 'nh': p.get('nh','--'), 'oldnh': p.get('oldnh','--'),
            'direction': p.get('direction','--'), 'lane': p.get('lane','--'),
            'lat': lat, 'lon': lon, 'alt': alt, 'distance': dist,
            'chainage': chain, 'speed': f"{self.last_speed:.1f}",
            'date': dt.strftime('%Y-%m-%d'), 'time': dt.strftime('%H:%M:%S'),
        }

    # ---------- Project specs ----------
    def set_project_specs(self):
        get = lambda e: e.get().strip()
        survey, nh, client, oldnh, section_code = map(get, [self.ent_survey, self.ent_nh, self.ent_client, self.ent_oldnh, self.ent_section])
        direction, lane, pave = self.direction_var.get().strip(), self.lane_var.get().strip(), self.cmb_pave.get().strip()
        try:
            init_chain = float(get(self.ent_chainage)); meas_km = float(get(self.ent_section_km))
        except Exception:
            messagebox.showerror("Invalid Input", "Initial Chainage and Measurement Section must be numbers."); return
        if not all([survey, nh, client, section_code, direction, lane, pave]):
            messagebox.showerror("Missing Input", "Please fill all fields."); return
        if meas_km <= 0:
            messagebox.showerror("Invalid Section", "Measurement Section must be > 0."); return
        base = filedialog.askdirectory(title="Choose base folder for project")
        if not base: return
        self.build_project_paths(base, survey, direction, lane)
        self.project = {"survey":survey, "nh":nh, "client":client, "oldnh":oldnh, "section_code":section_code,
                        "direction":direction, "lane":lane, "pave":pave, "init_chain":init_chain, "meas_km":meas_km}
        self.lbl_proj_status.configure(text=f"Specs set → {survey} | {direction} {lane}\nRun folder: {self.run_dir}")
        self.current_chainage_km = init_chain
        self.update_chainage_label(0.0)
        self.table1_rows.clear(); self.table2_rows.clear(); self.measure_accum_km = 0.0; self.window_buffer = []
        self.graph_data = [deque(maxlen=600) for _ in range(6)]
        for line in self.lines: line.set_data([], [])
        self.canvas_mpl.draw_idle()
        self.set_left_state(False); self.set_controls_state(True); self.set_selection_state(False)
        self.close_writers()

    def build_project_paths(self, base, survey, direction, lane):
        proj_root = os.path.join(base, survey)
        lhs, rhs = os.path.join(proj_root, "LHS"), os.path.join(proj_root, "RHS")
        for side_root, prefix in [(lhs, "L"), (rhs, "R")]:
            os.makedirs(side_root, exist_ok=True)
            for i in range(1,11): os.makedirs(os.path.join(side_root, f"{prefix}{i}"), exist_ok=True)
        side_root = lhs if direction == "Increasing" else rhs
        lane_root = os.path.join(side_root, lane)
        run_folder = f"Run_{ts_for_path()}"
        self.run_dir = os.path.join(lane_root, run_folder)
        self.images_dir = os.path.join(self.run_dir, "Images")
        self.stream_dir = os.path.join(self.run_dir, "Stream")
        self.data_dir   = os.path.join(self.run_dir, "Road Condition Data")
        os.makedirs(self.images_dir, exist_ok=True)
        for name in CAM_NAMES.values(): os.makedirs(os.path.join(self.images_dir, name), exist_ok=True)
        os.makedirs(self.stream_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)

    # ---------- Reset ----------
    def on_reset(self):
        self.on_stop(silent=True)
        self.project = None
        self.lbl_proj_status.configure(text="Project not configured.")
        self.current_chainage_km = None
        self.set_status_color("red"); self.lbl_status.configure(text="Status: stopped")
        self.var_speed.set("0.0 km/h"); self.var_dist.set("0.0 m"); self.var_chain.set("-- + 0.000")
        self.var_lat.set("Lat: --"); self.var_lon.set("Lon: --"); self.var_alt.set("Alt: -- m")
        self.graph_data = [deque(maxlen=600) for _ in range(6)]
        for line in self.lines: line.set_data([], [])
        self.canvas_mpl.draw_idle()
        self.set_controls_state(False); self.set_selection_state(True); self.set_left_state(True)
        self.last_lat = self.last_lon = self.last_alt = None
        self.last_speed = 0.0; self.last_distance_m = 0.0

    # ---------- Controls ----------
    def on_start(self):
        if not self.controls_enabled or self.project is None:
            messagebox.showwarning("Start", "Set Project Specifications first."); return
        self.running = True; self.paused_event.clear()
        self.set_status_color("green"); self.lbl_status.configure(text="Status: running")
        self.set_selection_state(False)
        if not self.video_writers: self.open_writers()
        if self.sensor_worker is None:
            self.sensor_worker = SensorWorker(self.ui_queue, self.paused_event, self.get_calibration_value,
                                              self.on_ten_m_tick, self.get_sensor_selection, self.set_latest_raw)
            self.sensor_worker.start()

    def on_pause(self):
        if not self.running: return
        self.paused_event.set(); self.set_status_color("yellow"); self.lbl_status.configure(text="Status: paused")

    def on_stop(self, silent=False):
        if not self.project: return
        self.on_export()
        self.running = False; self.paused_event.set()
        self.set_status_color("red"); self.lbl_status.configure(text="Status: stopped")
        self.table1_rows.clear(); self.table2_rows.clear(); self.measure_accum_km = 0.0; self.window_buffer = []
        if self.sensor_worker:
            try:
                self.sensor_worker.stop(); self.sensor_worker.join(timeout=1.0)
            except Exception: pass
            self.sensor_worker = None
        self.close_writers(); self.set_selection_state(True)
        if not silent: messagebox.showinfo("Stopped", "Run stopped and data exported.")

    def on_export(self):
        if not self.project or self.run_dir is None:
            messagebox.showinfo("Export", "No configured project/run to export."); return
        self.paused_event.set(); self.set_status_color("yellow"); self.lbl_status.configure(text="Status: paused (export)")
        if not (PANDAS_OK and OPENPYXL_OK):
            messagebox.showerror("Export", "pandas and openpyxl are required for Excel export.\nInstall with: pip install pandas openpyxl"); return
        if self.window_buffer and self.measure_accum_km > 0.0:
            dir_sign = 1 if self.project["direction"] == "Increasing" else -1
            end_chain = self.current_chainage_km if self.current_chainage_km is not None else self.project['init_chain']
            end_lat = self.window_buffer[-1]["lat"]; end_lon = self.window_buffer[-1]["lon"]
            self.flush_measurement_window(final_ts=now_local_str(), end_lat=end_lat, end_lon=end_lon, dir_sign=dir_sign, end_chain=end_chain)
        t1_cols = [
            "Timestamp","NH Number","Start Chainage (km)","End Chainage (km)",
            "Direction","Lane","Speed (km/h)",
            "Left IRI (m)","Right IRI (m)","Average IRI (m)",
            "Left Rut (mm)","Right Rut (mm)","Average Rut (mm)",
            "Average Texture (m)",
            "Lat Start","Long Start","Lat End","Long End"
        ]
        t2_cols = [
            "Timestamp","NH Number","Start Chainage (km)","End Chainage (km)",
            "Direction","Lane","Lat","Long","Alt (m)","Speed (km/h)"
        ]
        df1 = pd.DataFrame(self.table1_rows, columns=t1_cols)
        df2 = pd.DataFrame(self.table2_rows, columns=t2_cols)
        survey = self.project['survey']; lane = self.project['lane']; startc = f"{self.project['init_chain']:.3f}"
        rut_name = f"{survey}_{lane}_{startc}_Rutting.xlsx"
        iri_name = f"{survey}_{lane}_{startc}_RoughnessIRI.xlsx"
        tex_name = f"{survey}_{lane}_{startc}_Texture.xlsx"
        gps_name = f"{survey}_{lane}_{startc}_GPS.xlsx"
        rut_cols = [c for c in t1_cols if c not in ("Left IRI (m)","Right IRI (m)","Average IRI (m)","Average Texture (m)")]
        iri_cols = [c for c in t1_cols if c not in ("Left Rut (mm)","Right Rut (mm)","Average Rut (mm)","Average Texture (m)")]
        tex_cols = [c for c in t1_cols if c not in ("Left IRI (m)","Right IRI (m)","Average IRI (m)","Left Rut (mm)","Right Rut (mm)","Average Rut (mm)")]
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            df1[rut_cols].to_excel(os.path.join(self.data_dir, rut_name), index=False)
            df1[iri_cols].to_excel(os.path.join(self.data_dir, iri_name), index=False)
            df1[tex_cols].to_excel(os.path.join(self.data_dir, tex_name), index=False)
            df2.to_excel(os.path.join(self.data_dir, gps_name), index=False)
            messagebox.showinfo("Export", f"Data exported to:\n{self.data_dir}\n\nRows → T1: {len(df1)} | T2: {len(df2)}")
        except Exception as e:
            messagebox.showerror("Export failed", str(e))

    # ---------- Graph + data helpers ----------
    def update_graphs(self, avg_six):
        for i in range(6):
            self.graph_data[i].append(avg_six[i])
            xs = range(1, len(self.graph_data[i]) + 1)
            self.lines[i].set_data(list(xs), list(self.graph_data[i]))
            self.axes[i].relim(); self.axes[i].autoscale_view()
        self.canvas_mpl.draw_idle()

    def on_ten_m_tick(self, avg_six, speed_kmph, latlonalt):
        if self.project is None or not self.running: return
        dir_sign = 1 if self.project["direction"] == "Increasing" else -1
        start_chain = self.current_chainage_km
        end_chain = start_chain + dir_sign * TEN_M_KM
        ts = now_local_str()
        lat, lon, alt = latlonalt
        self.last_lat, self.last_lon, self.last_alt = lat, lon, alt
        self.last_speed = speed_kmph; self.last_distance_m += TEN_M_METERS
        self.table2_rows.append([ts, self.project["nh"], f"{start_chain:.3f}", f"{end_chain:.3f}", self.project["direction"], self.project["lane"], f"{lat:.6f}", f"{lon:.6f}", f"{alt:.1f}", f"{speed_kmph:.1f}"])
        self.capture_images(chainage_label=self.format_chainage_label(start_chain), lat=lat, lon=lon, alt=alt)
        self.measure_accum_km += TEN_M_KM
        self.window_buffer.append({"S": avg_six, "speed": speed_kmph, "lat": lat, "lon": lon})
        if self.measure_accum_km + 1e-9 >= self.project["meas_km"]:
            self.flush_measurement_window(final_ts=ts, end_lat=lat, end_lon=lon, dir_sign=dir_sign, end_chain=end_chain)
        self.current_chainage_km = end_chain
        self.update_chainage_label(dir_sign * TEN_M_KM)
        self.update_graphs(avg_six)

    def get_sensor_selection(self):
        return [v.get() for v in self.sensor_selected]

    def _fmt_or_none(self, x, ndigits):
        if x is None or (isinstance(x, float) and np.isnan(x)): return None
        return round(float(x), ndigits)

    def flush_measurement_window(self, final_ts, end_lat, end_lon, dir_sign, end_chain):
        if not self.window_buffer: return
        S_matrix = np.array([w["S"] for w in self.window_buffer])  # (n,6)
        mean_S = np.nanmean(S_matrix, axis=0)
        sel = np.array(self.get_sensor_selection(), dtype=bool)
        masked = np.array(mean_S, dtype=float); masked[~sel] = np.nan
        S1,S2,S3,S4,S5,S6 = masked
        left_rut   = np.nanmean([S1, S2])
        right_rut  = np.nanmean([S5, S6])
        avg_rut    = np.nanmean([S1, S2, S5, S6])
        left_iri   = np.nanmean([S2, S3])
        right_iri  = np.nanmean([S4, S5])
        avg_iri    = np.nanmean([S2, S3, S4, S5])
        avg_texture= np.nanmean([S1, S2, S3, S4, S5, S6])
        speed_mean = float(np.nanmean([w["speed"] for w in self.window_buffer]))
        start_c = end_chain - dir_sign * self.measure_accum_km
        end_c   = start_c + dir_sign * self.measure_accum_km
        lat_start = self.window_buffer[0]["lat"]; lon_start = self.window_buffer[0]["lon"]
        row1 = [
            final_ts, self.project["nh"], f"{start_c:.3f}", f"{end_c:.3f}", self.project["direction"], self.project["lane"],
            self._fmt_or_none(speed_mean, 1),
            self._fmt_or_none(left_iri, 3), self._fmt_or_none(right_iri, 3), self._fmt_or_none(avg_iri, 3),
            self._fmt_or_none(left_rut, 1), self._fmt_or_none(right_rut, 1), self._fmt_or_none(avg_rut, 1),
            self._fmt_or_none(avg_texture, 3),
            f"{lat_start:.6f}", f"{lon_start:.6f}", f"{end_lat:.6f}", f"{end_lon:.6f}"
        ]
        self.table1_rows.append(row1)
        self.measure_accum_km = 0.0; self.window_buffer = []

    def set_latest_raw(self, raw_vals): self.latest_raw = list(raw_vals)

    def capture_images(self, chainage_label, lat=None, lon=None, alt=None):
        if self.images_dir is None: return
        stamp = ts_for_path()
        for cam_idx, worker in self.cameras.items():
            if not self.cam_selected.get(cam_idx, tk.BooleanVar(value=True)).get():
                continue
            record_frame = getattr(worker, "latest_record_frame", None)
            if record_frame is None:
                # Fallback to preview frame upscaled with header at record size
                preview = getattr(worker, "latest_preview_frame", None)
                if preview is None: continue
                ctx = self.get_overlay_context()
                record_frame = draw_header_to_size(preview, ctx, RECORD_RES)
            name = CAM_NAMES.get(cam_idx, f"CAM{cam_idx}")
            folder = os.path.join(self.images_dir, name); os.makedirs(folder, exist_ok=True)
            fname = f"{name}_{chainage_label}_{stamp}.jpg".replace(" ", "_")
            path = os.path.join(folder, fname)
            try:
                cv2.imwrite(path, record_frame)  # RECORD_RES exact
            except Exception:
                pass

    # ---------- Telemetry/UI queue ----------
    def poll_cam_frames(self):
        try:
            from PIL import Image
            while True:
                cam_idx, frame = self.cam_queue.get_nowait()
                rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                pil_img = Image.fromarray(rgb)
                ctk_img = ctk.CTkImage(light_image=pil_img, dark_image=pil_img, size=PREVIEW_RES)
                try: i = CAM_INDEXES.index(cam_idx)
                except ValueError: i = cam_idx
                if i < len(self.cam_labels):
                    lbl = self.cam_labels[i]
                    lbl.configure(image=ctk_img)
                    lbl.image = ctk_img  # keep a reference
        except queue.Empty:
            pass
        self.after(30, self.poll_cam_frames)

    def poll_ui_queue(self):
        try:
            while True:
                msg = self.ui_queue.get_nowait()
                if msg["type"] == "telemetry" and self.running:
                    self.last_speed = msg['speed_kmph']; self.last_distance_m = msg['distance_m']
                    self.last_lat = msg['lat']; self.last_lon = msg['lon']; self.last_alt = msg['alt']
                    self.var_speed.set(f"{msg['speed_kmph']:.1f} km/h")
                    self.var_dist.set(f"{msg['distance_m']:.1f} m")
                    self.var_lat.set(f"Lat: {msg['lat']:.6f}")
                    self.var_lon.set(f"Lon: {msg['lon']:.6f}")
                    self.var_alt.set(f"Alt: {msg['alt']:.1f} m")
        except queue.Empty:
            pass
        self.after(60, self.poll_ui_queue)

    # ---------- Writers (video) ----------
    def open_writers(self):
        self.close_writers()
        if self.stream_dir is None or self.project is None: return
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        for cam_idx in CAM_INDEXES:
            if not self.cam_selected[cam_idx].get(): continue
            name = CAM_NAMES.get(cam_idx, f"CAM{cam_idx}")
            out_path = os.path.join(self.stream_dir, f"{name}_{self.project['survey']}_{self.project['lane']}_{self.project['init_chain']:.3f}.mp4")
            vw = cv2.VideoWriter(out_path, fourcc, CAM_FPS, RECORD_RES)
            if not vw.isOpened():
                messagebox.showerror("Video", f"Failed to open writer for {name}")
            else:
                self.video_writers[cam_idx] = vw

    def close_writers(self):
        for vw in self.video_writers.values():
            try: vw.release()
            except Exception: pass
        self.video_writers.clear()

    # ---------- Chainage & labels ----------
    def set_status_color(self, color):
        mapping = {"red":"#d32f2f","yellow":"#fbc02d","green":"#43a047"}
        self.chip.configure(fg_color=mapping.get(color, "#d32f2f"))

    def update_chainage_label(self, _delta_km):
        if self.project is None: return
        base = self.project['init_chain']; total_offset = (self.current_chainage_km - base)
        sign = '+' if total_offset >= 0 else '-'
        self.var_chain.set(f"{base:.0f} {sign} {abs(total_offset):.3f}")

    def format_chainage_label(self, start_chain):
        base = self.project['init_chain'] if self.project else 0.0
        offset = start_chain - base; sign = '+' if offset >= 0 else '-'
        return f"{base:.0f}{sign}{abs(offset):.3f}"

    # ---------- Calibration ----------
    def open_calibrate(self):
        win = ctk.CTkToplevel(self); win.title("Zero Calibration — All Sensors (Live Raw)"); win.geometry("420x300")
        win.transient(self); win.grab_set()
        try: win.attributes("-topmost", True)
        except Exception: pass
        ctk.CTkLabel(win, text="Calibration value (applied to all sensors)").pack(pady=(12,4))
        e = ctk.CTkEntry(win); e.insert(0, str(self.calib_value)); e.pack(pady=4)
        table = ctk.CTkFrame(win); table.pack(fill="x", padx=10, pady=8)
        ctk.CTkLabel(table, text="Live RAW sensor values (S1..S6)").grid(row=0, column=0, columnspan=6, pady=(0,6))
        self.raw_labels = []
        for i in range(6):
            ctk.CTkLabel(table, text=f"S{i+1}").grid(row=1, column=i, padx=6)
            lbl = ctk.CTkLabel(table, text="--"); lbl.grid(row=2, column=i, padx=6); self.raw_labels.append(lbl)
        def tick():
            for i in range(6):
                try: self.raw_labels[i].configure(text=f"{self.latest_raw[i]:.2f}")
                except Exception: self.raw_labels[i].configure(text="--")
            if win.winfo_exists(): win.after(100, tick)
        tick()
        def apply_and_close():
            try:
                self.calib_value = float(e.get()); messagebox.showinfo("Calibration", f"Applied calibration: {self.calib_value}"); win.destroy()
            except Exception:
                messagebox.showerror("Calibration", "Please enter a numeric value.")
        ctk.CTkButton(win, text="Apply and Close", command=apply_and_close).pack(pady=12)
        e.focus_set()

    def get_calibration_value(self): return float(self.calib_value)

    # ---------- Misc ----------
    def new_project(self):
        if messagebox.askyesno("New Project", "This will reset configuration. Continue?"): self.on_reset()

    def open_manual(self): messagebox.showinfo("User Manual", "Hook your manual PDF path in open_manual().")

    def on_close(self):
        try:
            if self.sensor_worker:
                try: self.sensor_worker.stop(); self.sensor_worker.join(timeout=1.0)
                except Exception: pass
                self.sensor_worker = None
            self.close_writers(); self.stop_cameras()
        finally:
            self.destroy()

if __name__ == "__main__":
    app = NSVApp(); app.mainloop()
