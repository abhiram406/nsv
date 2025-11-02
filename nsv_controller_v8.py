# NSV Controller v8 — COMPLETE
# -------------------------------------------------------------
# Three-panel GUI (Left: Project Setup, Centre: Cameras+Graphs, Right: Telemetry+Controls)
# Changes from v7/v7-fixes:
# - Left panel button renamed to "Set Project Specifications"; validates inputs + creates run folder only
#   (DOES NOT start sensors). Centre/Right remain disabled until specs are valid.
# - After specs are set, Left panel is locked until "Reset" on right panel is clicked.
# - Direction dropdown callback updates Lane dropdown values (Increasing → L1..L10, Decreasing → R1..R10),
#   preserving lane index where possible (L3 ↔ R3) for convenience.
# - Cameras always preview; graphs remain empty until Start.
# - Data collection at every 10 m; measurement windows per Measurement Section (km).
# - Flush partial measurement window on Export/Stop so Excel sheets are never empty.
# - Folder structure only creates Images/Stream/Road Condition Data inside Run_* folder under selected lane.
# - Exports 4 Excel files with specified filenames (chainage fixed from project start) + timestamp column in tables.
# - Layout-optimized: fixed left/right widths, 2×2 camera previews, compact 2×3 graphs.
# -------------------------------------------------------------

import os
import time
import math
import threading
import queue
import random
from datetime import datetime

import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox

import numpy as np
import cv2

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
LEFT_WIDTH  = 360
RIGHT_WIDTH = 380
WINDOW_GEOM = "1600x900"

CAM_INDEXES = [0, 1, 2, 3]
CAM_RES = (320, 180)     # compact preview size for 2×2 grid
CAM_FPS = 25
TOF_HZ = 100
TEN_M_METERS = 10.0
TEN_M_KM = 0.01

# ---------------------------
# Helpers
# ---------------------------

def now_local_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ts_for_path():
    return datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def make_testcard(w, h, text="NO CAMERA"):
    img = np.zeros((h, w, 3), dtype=np.uint8)
    bars = [(255,255,255),(255,255,0),(0,255,255),(0,255,0),(255,0,255),(255,0,0),(0,0,255)]
    bw = max(1, w // len(bars))
    for i, c in enumerate(bars):
        img[:, i*bw:(i+1)*bw] = c
    cv2.putText(img, text, (10, h//2), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0,0,0), 3, cv2.LINE_AA)
    cv2.putText(img, text, (10, h//2), cv2.FONT_HERSHEY_SIMPLEX, 0.7, (255,255,255), 1, cv2.LINE_AA)
    return img

# ---------------------------
# Camera Worker
# ---------------------------
class CameraWorker(threading.Thread):
    def __init__(self, cam_index, out_queue, get_writer, width=CAM_RES[0], height=CAM_RES[1]):
        super().__init__(daemon=True)
        self.cam_index = cam_index
        self.out_queue = out_queue
        self.get_writer = get_writer
        self.width = width
        self.height = height
        self.stop_event = threading.Event()
        self.cap = None
        self.latest_frame = None

    def run(self):
        try:
            self.cap = cv2.VideoCapture(self.cam_index, cv2.CAP_DSHOW)
            if not (self.cap and self.cap.isOpened()):
                while not self.stop_event.is_set():
                    frame = make_testcard(self.width, self.height, f"CAM {self.cam_index} NOT FOUND")
                    self.latest_frame = frame
                    self.out_queue.put((self.cam_index, frame))
                    time.sleep(1.0/CAM_FPS)
                return
            self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
            self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)
            while not self.stop_event.is_set():
                ok, frame = self.cap.read()
                if not ok:
                    frame = make_testcard(self.width, self.height, f"CAM {self.cam_index} ERROR")
                else:
                    frame = cv2.resize(frame, (self.width, self.height))
                self.latest_frame = frame
                self.out_queue.put((self.cam_index, frame))
                writer = self.get_writer(self.cam_index)
                if writer is not None:
                    writer.write(frame)
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
# Sensor Worker (distance-driven, 10 m sampling)
# ---------------------------
class SensorWorker(threading.Thread):
    def __init__(self, ui_queue, paused_event, calib_getter, ten_m_callback):
        super().__init__(daemon=True)
        self.ui_queue = ui_queue
        self.paused_event = paused_event
        self.calib_getter = calib_getter
        self.ten_m_callback = ten_m_callback
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
            offs = self.calib_getter()
            vals = [(raw[i] - offs[i]) if offs[i] is not None else raw[i] for i in range(6)]
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
        ctk.set_appearance_mode("light")
        ctk.set_default_color_theme("blue")
        self.title("NSV Controller v8 — COMPLETE")
        self.geometry(WINDOW_GEOM)

        # Grid: lock side panel min widths to avoid center squashing them
        self.grid_columnconfigure(0, weight=0, minsize=LEFT_WIDTH)
        self.grid_columnconfigure(1, weight=1)   # centre grows
        self.grid_columnconfigure(2, weight=0, minsize=RIGHT_WIDTH)
        self.grid_rowconfigure(0, weight=1)

        # Queues/threads
        self.ui_queue = queue.Queue()
        self.cam_queue = queue.Queue()
        self.cameras = {}
        self.sensor_worker = None

        # State
        self.running = False
        self.paused_event = threading.Event()
        self.project = None
        self.calib_offsets = [0.0]*6
        self.controls_enabled = False  # center/right functionality gate

        # Folders/writers
        self.video_writers = {}
        self.run_dir = None
        self.images_dir = None
        self.stream_dir = None
        self.data_dir = None
        self.last_frames = {}

        # Data tables
        self.table1_rows = []
        self.table2_rows = []
        self.measure_accum_km = 0.0
        self.window_buffer = []
        self.current_chainage_km = None

        # Build UI
        self.build_menu()
        self.build_layout()
        # Keep side panel widths
        self.left.configure(width=LEFT_WIDTH); self.left.grid_propagate(False)
        self.right.configure(width=RIGHT_WIDTH); self.right.grid_propagate(False)

        # Initially disable center/right functionality until specs are validated
        self.set_controls_state(False)

        # Start camera previews immediately
        for idx in CAM_INDEXES:
            self.start_camera(idx)

        # Pollers
        self.after(30, self.poll_cam_frames)
        self.after(60, self.poll_ui_queue)

    # ---------- Menu ----------
    def build_menu(self):
        menubar = tk.Menu(self)
        # File
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="New Project", command=self.new_project)
        file_menu.add_separator(); file_menu.add_command(label="Exit", command=self.on_close)
        menubar.add_cascade(label="File", menu=file_menu)
        # Calibrate
        cal_menu = tk.Menu(menubar, tearoff=0)
        cal_menu.add_command(label="Zero Calibrate Sensors", command=self.open_calibrate)
        menubar.add_cascade(label="Calibrate", menu=cal_menu)
        # Help
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="User Manual", command=self.open_manual)
        menubar.add_cascade(label="Help", menu=help_menu)
        self.config(menu=menubar)

    # ---------- Layout ----------
    def build_layout(self):
        # Left panel (form: labels left, fields right; compact widths)
        self.left = ctk.CTkFrame(self, corner_radius=12)
        self.left.grid(row=0, column=0, sticky="nsw", padx=(8,4), pady=8)
        self.left.grid_columnconfigure(0, weight=0, minsize=140)
        self.left.grid_columnconfigure(1, weight=1)
        self.build_project_setup(self.left)

        # Centre panel (flex)
        self.center = ctk.CTkFrame(self, corner_radius=12)
        self.center.grid(row=0, column=1, sticky="nsew", padx=4, pady=8)
        self.center.grid_columnconfigure((0,1), weight=1)
        self.center.grid_rowconfigure(0, weight=0)  # cameras row fixed height
        self.center.grid_rowconfigure(1, weight=1)  # graphs row expands
        self.build_cameras(self.center)
        self.build_graphs(self.center)

        # Right panel (fixed width)
        self.right = ctk.CTkFrame(self, corner_radius=12)
        self.right.grid(row=0, column=2, sticky="nse", padx=(4,8), pady=8)
        self.right.grid_columnconfigure(0, weight=1)
        self.build_right_panel(self.right)

    # ---------- Project Setup ----------
    def create_labeled_dropdown(self, parent, label, options, variable=None, command=None, width=160):
        ctk.CTkLabel(parent, text=label, anchor="e").grid(sticky="e", padx=8, pady=4)
        dropdown = ctk.CTkOptionMenu(parent, values=options, variable=variable, command=command, width=width)
        dropdown.grid(row=parent.grid_size()[1]-1, column=1, sticky="ew", padx=8, pady=4)
        return dropdown

    def build_project_setup(self, f):
        pad = {"padx":8, "pady":4}
        title = ctk.CTkLabel(f, text="Project Setup", font=ctk.CTkFont(size=16, weight="bold"))
        title.grid(row=0, column=0, columnspan=2, sticky="w", **pad)

        def add_row(r, label, widget):
            ctk.CTkLabel(f, text=label, anchor="e").grid(row=r, column=0, sticky="e", **pad)
            widget.grid(row=r, column=1, sticky="ew", **pad)

        entry_w = 160
        self.ent_survey = ctk.CTkEntry(f, width=entry_w)
        self.ent_client = ctk.CTkEntry(f, width=entry_w)
        self.ent_nh = ctk.CTkEntry(f, width=entry_w)
        self.ent_oldnh = ctk.CTkEntry(f, width=entry_w)
        self.ent_section = ctk.CTkEntry(f, width=entry_w)
        add_row(1, "Survey ID", self.ent_survey)
        add_row(2, "Client Name/ID", self.ent_client)
        add_row(3, "NH Number", self.ent_nh)
        add_row(4, "Old NH Number", self.ent_oldnh)
        add_row(5, "Section Code", self.ent_section)

        self.direction_var = ctk.StringVar(value="Increasing")
        self.direction_dd = ctk.CTkOptionMenu(f, values=["Increasing", "Decreasing"], variable=self.direction_var, command=self.on_direction_select, width=entry_w)
        add_row(6, "Direction", self.direction_dd)

        self.lane_var = ctk.StringVar(value="L1")
        self.lane_dd = ctk.CTkOptionMenu(f, values=[f"L{i}" for i in range(1,11)], variable=self.lane_var, width=entry_w)
        add_row(7, "Lane No.", self.lane_dd)

        self.cmb_pave = ctk.CTkComboBox(f, values=["Concrete", "Bituminous"], width=entry_w)
        self.cmb_pave.set("Bituminous")
        add_row(8, "Pavement Type", self.cmb_pave)

        self.ent_chainage = ctk.CTkEntry(f, width=entry_w)
        add_row(9, "Initial Chainage (Km)", self.ent_chainage)
        self.ent_section_km = ctk.CTkEntry(f, width=entry_w)
        add_row(10, "Measurement Section (Km)", self.ent_section_km)

        # Set Project Specifications (validates + prepares paths; does NOT start sensors)
        self.btn_set_specs = ctk.CTkButton(f, text="Set Project Specifications", command=self.set_project_specs)
        self.btn_set_specs.grid(row=11, column=0, columnspan=2, sticky="ew", padx=8, pady=(12,8))

        self.lbl_proj_status = ctk.CTkLabel(f, text="Project not configured.")
        self.lbl_proj_status.grid(row=12, column=0, columnspan=2, sticky="w", **pad)

        # track left widgets to disable after validation
        self.left_widgets = [
            self.ent_survey, self.ent_client, self.ent_nh, self.ent_oldnh, self.ent_section,
            self.direction_dd, self.lane_dd, self.cmb_pave, self.ent_chainage, self.ent_section_km, self.btn_set_specs
        ]

    def on_direction_select(self, choice):
        """Update lanes when Direction changes via OptionMenu callback. Preserve lane index if possible."""
        lanes = [f"L{i}" for i in range(1, 11)] if choice == "Increasing" else [f"R{i}" for i in range(1, 11)]
        # Try to preserve numeric index from current selection
        current = self.lane_var.get().strip()
        idx = 0
        if len(current) >= 2 and current[1:].isdigit():
            idx = max(1, min(10, int(current[1:]))) - 1
        self.lane_dd.configure(values=lanes)
        new_selection = lanes[idx]
        try:
            self.lane_dd.set(new_selection)
        except Exception:
            pass
        self.lane_var.set(new_selection)

    # ---------- Centre: Cameras (2×2 compact previews) ----------
    def build_cameras(self, parent):
        self.cam_frame = ctk.CTkFrame(parent)
        self.cam_frame.grid(row=0, column=0, columnspan=2, sticky="ew", padx=8, pady=(8,4))
        for c in range(2):
            self.cam_frame.grid_columnconfigure(c, weight=1, uniform="cams")
        self.cam_frame.grid_rowconfigure(0, weight=0)
        self.cam_frame.grid_rowconfigure(1, weight=0)
        self.cam_labels = []
        for i in range(4):
            r, c = divmod(i, 2)
            lbl = ctk.CTkLabel(self.cam_frame, text=f"Camera {i}", width=CAM_RES[0], height=CAM_RES[1])
            lbl.grid(row=r, column=c, padx=6, pady=6, sticky="n")
            self.cam_labels.append(lbl)

    # ---------- Centre: Graphs (2×3 compact) ----------
    def build_graphs(self, parent):
        self.graph_frame = ctk.CTkFrame(parent)
        self.graph_frame.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=8, pady=(4,8))
        self.graph_frame.grid_rowconfigure(0, weight=1)
        self.graph_frame.grid_columnconfigure(0, weight=1)
        fig = Figure(figsize=(9,4.2), dpi=100)
        self.axes = []
        for i in range(6):
            ax = fig.add_subplot(2,3,i+1)
            ax.set_title(f"S{i+1}", fontsize=9)
            ax.set_xlabel("Seg", fontsize=8)
            ax.set_ylabel("Val", fontsize=8)
            self.axes.append(ax)
        self.lines = [ax.plot([], [])[0] for ax in self.axes]
        self.graph_data = [[] for _ in range(6)]
        self.canvas_mpl = FigureCanvasTkAgg(fig, master=self.graph_frame)
        self.canvas_mpl.draw()
        self.canvas_mpl.get_tk_widget().pack(fill="both", expand=True)

    # ---------- Right Panel ----------
    def build_right_panel(self, parent):
        f = parent
        pad = {"padx":10, "pady":4}
        ctk.CTkLabel(f, text="Telemetry & Controls", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, sticky="w", **pad)
        chip_frame = ctk.CTkFrame(f, fg_color="transparent"); chip_frame.grid(row=1, column=0, sticky="w", **pad)
        self.chip = ctk.CTkLabel(chip_frame, text="  ", width=16, height=16, corner_radius=8); self.chip.grid(row=0, column=0, padx=(0,6))
        self.set_status_color("red")
        self.lbl_status = ctk.CTkLabel(chip_frame, text="Status: stopped"); self.lbl_status.grid(row=0, column=1)
        self.var_speed = ctk.StringVar(value="0.0 km/h"); self.var_dist = ctk.StringVar(value="0.0 m")
        self.var_chain = ctk.StringVar(value="-- + 0.000"); self.var_lat = ctk.StringVar(value="Lat: --")
        self.var_lon = ctk.StringVar(value="Lon: --"); self.var_alt = ctk.StringVar(value="Alt: -- m")
        ctk.CTkLabel(f, textvariable=self.var_speed).grid(row=2, column=0, sticky="w", **pad)
        ctk.CTkLabel(f, textvariable=self.var_dist).grid(row=3, column=0, sticky="w", **pad)
        ctk.CTkLabel(f, textvariable=self.var_chain).grid(row=4, column=0, sticky="w", **pad)
        ctk.CTkLabel(f, textvariable=self.var_lat).grid(row=5, column=0, sticky="w", **pad)
        ctk.CTkLabel(f, textvariable=self.var_lon).grid(row=6, column=0, sticky="w", **pad)
        ctk.CTkLabel(f, textvariable=self.var_alt).grid(row=7, column=0, sticky="w", **pad)
        btns = ctk.CTkFrame(f); btns.grid(row=8, column=0, sticky="ew", padx=10, pady=(8,6))
        btns.grid_columnconfigure((0,1,2,3,4), weight=1)
        self.btn_start = ctk.CTkButton(btns, text="Start", command=self.on_start)
        self.btn_pause = ctk.CTkButton(btns, text="Pause", command=self.on_pause)
        self.btn_export = ctk.CTkButton(btns, text="Export", command=self.on_export)
        self.btn_stop = ctk.CTkButton(btns, text="Stop", command=self.on_stop)
        self.btn_reset = ctk.CTkButton(btns, text="Reset", command=self.on_reset)
        self.btn_start.grid(row=0, column=0, padx=4, pady=4, sticky="ew")
        self.btn_pause.grid(row=0, column=1, padx=4, pady=4, sticky="ew")
        self.btn_export.grid(row=0, column=2, padx=4, pady=4, sticky="ew")
        self.btn_stop.grid(row=0, column=3, padx=4, pady=4, sticky="ew")
        self.btn_reset.grid(row=0, column=4, padx=4, pady=4, sticky="ew")

    # ---------- Enable/Disable controls ----------
    def set_controls_state(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for btn in [self.btn_start, self.btn_pause, self.btn_export, self.btn_stop, self.btn_reset]:
            btn.configure(state=state)
        self.controls_enabled = enabled

    def set_left_state(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for w in self.left_widgets:
            try:
                w.configure(state=state)
            except Exception:
                pass

    # ---------- Cameras ----------
    def start_camera(self, cam_idx):
        if cam_idx in self.cameras:
            return
        cw = CameraWorker(cam_idx, self.cam_queue, self.get_writer_for)
        self.cameras[cam_idx] = cw
        cw.start()

    def stop_cameras(self):
        for cw in self.cameras.values():
            cw.stop()
        self.cameras.clear()

    def get_writer_for(self, cam_idx):
        if not self.running or self.paused_event.is_set() or self.stream_dir is None:
            return None
        return self.video_writers.get(cam_idx)

    # ---------- Project specs (validate only) ----------
    def set_project_specs(self):
        survey = self.ent_survey.get().strip()
        nh = self.ent_nh.get().strip()
        client = self.ent_client.get().strip()
        oldnh = self.ent_oldnh.get().strip()
        section_code = self.ent_section.get().strip()
        direction = self.direction_var.get().strip()
        lane = self.lane_var.get().strip()
        pave = self.cmb_pave.get().strip()
        try:
            init_chain = float(self.ent_chainage.get().strip())
            meas_km = float(self.ent_section_km.get().strip())
        except Exception:
            messagebox.showerror("Invalid Input", "Initial Chainage and Measurement Section must be numbers.")
            return
        if not all([survey, nh, client, section_code, direction, lane, pave]):
            messagebox.showerror("Missing Input", "Please fill all fields.")
            return
        if meas_km <= 0:
            messagebox.showerror("Invalid Section", "Measurement Section must be > 0.")
            return
        base = filedialog.askdirectory(title="Choose base folder for project")
        if not base:
            return
        # Build folders but DO NOT start sensors
        self.build_project_paths(base, survey, direction, lane)
        self.project = {
            "survey": survey,
            "nh": nh,
            "client": client,
            "oldnh": oldnh,
            "section_code": section_code,
            "direction": direction,
            "lane": lane,
            "pave": pave,
            "init_chain": init_chain,
            "meas_km": meas_km,
        }
        self.lbl_proj_status.configure(text=f"Specs set → {survey} | {direction} {lane}\nRun folder: {self.run_dir}")
        self.current_chainage_km = init_chain
        self.update_chainage_label(0.0)
        # Reset data buffers/graphs (empty until Start)
        self.table1_rows.clear(); self.table2_rows.clear()
        self.measure_accum_km = 0.0
        self.window_buffer = []
        self.graph_data = [[] for _ in range(6)]
        for line in self.lines:
            line.set_data([], [])
        self.canvas_mpl.draw_idle()
        # Lock left panel; enable right controls
        self.set_left_state(False)
        self.set_controls_state(True)
        # Writers opened only when Start pressed
        self.close_writers()

    def build_project_paths(self, base, survey, direction, lane):
        proj_root = os.path.join(base, survey)
        lhs = os.path.join(proj_root, "LHS")
        rhs = os.path.join(proj_root, "RHS")
        for side_root, prefix in [(lhs, "L"), (rhs, "R")]:
            os.makedirs(side_root, exist_ok=True)
            for i in range(1,11):
                lane_dir = os.path.join(side_root, f"{prefix}{i}")
                os.makedirs(lane_dir, exist_ok=True)
        side_root = lhs if direction == "Increasing" else rhs
        lane_root = os.path.join(side_root, lane)
        run_folder = f"Run_{ts_for_path()}"
        self.run_dir = os.path.join(lane_root, run_folder)
        self.images_dir = os.path.join(self.run_dir, "Images")
        self.stream_dir = os.path.join(self.run_dir, "Stream")
        self.data_dir = os.path.join(self.run_dir, "Road Condition Data")
        os.makedirs(self.images_dir, exist_ok=True)
        os.makedirs(self.stream_dir, exist_ok=True)
        os.makedirs(self.data_dir, exist_ok=True)

    # ---------- Reset ----------
    def on_reset(self):
        # Stop any running work
        self.on_stop(silent=True)
        # Clear project and re-enable left inputs
        self.project = None
        self.lbl_proj_status.configure(text="Project not configured.")
        self.current_chainage_km = None
        self.var_chain.set("-- + 0.000")
        # Clear graphs
        self.graph_data = [[] for _ in range(6)]
        for line in self.lines:
            line.set_data([], [])
        self.canvas_mpl.draw_idle()
        # Disable center/right controls until specs are set again
        self.set_controls_state(False)
        self.set_left_state(True)

    # ---------- Controls ----------
    def on_start(self):
        if not self.controls_enabled or self.project is None:
            messagebox.showwarning("Start", "Set Project Specifications first.")
            return
        self.running = True
        self.paused_event.clear()
        self.set_status_color("green")
        self.lbl_status.configure(text="Status: running")
        if not self.video_writers:
            self.open_writers()
        if self.sensor_worker is None:
            self.sensor_worker = SensorWorker(self.ui_queue, self.paused_event, self.get_calibration_offsets, self.on_ten_m_tick)
            self.sensor_worker.start()

    def on_pause(self):
        if not self.running:
            return
        self.paused_event.set()
        self.set_status_color("yellow")
        self.lbl_status.configure(text="Status: paused")

    def on_stop(self, silent=False):
        if not self.project:
            return
        # Export and reset measurement tables
        self.on_export()
        self.running = False
        self.paused_event.set()
        self.set_status_color("red")
        self.lbl_status.configure(text="Status: stopped")
        self.table1_rows.clear(); self.table2_rows.clear()
        self.measure_accum_km = 0.0
        self.window_buffer = []
        if self.sensor_worker:
            self.sensor_worker.stop(); self.sensor_worker = None
        self.close_writers()
        if not silent:
            messagebox.showinfo("Stopped", "Run stopped and data exported.")

    def on_export(self):
        if not self.project or self.run_dir is None:
            messagebox.showinfo("Export", "No configured project/run to export.")
            return
        # Pause during export
        self.paused_event.set()
        self.set_status_color("yellow")
        self.lbl_status.configure(text="Status: paused (export)")
        if not PANDAS_OK or not OPENPYXL_OK:
            messagebox.showerror("Export", "pandas and openpyxl are required for Excel export.\nInstall with: pip install pandas openpyxl")
            return
        # Flush partial window if needed
        if self.window_buffer and self.measure_accum_km > 0.0:
            dir_sign = 1 if self.project["direction"] == "Increasing" else -1
            end_chain = self.current_chainage_km if self.current_chainage_km is not None else self.project['init_chain']
            end_lat = self.window_buffer[-1]["lat"]
            end_lon = self.window_buffer[-1]["lon"]
            self.flush_measurement_window(final_ts=now_local_str(), end_lat=end_lat, end_lon=end_lon, dir_sign=dir_sign, end_chain=end_chain)
        # Build DataFrames
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
            # keep paused state after export; Start can resume
        except Exception as e:
            messagebox.showerror("Export failed", str(e))

    # ---------- Graph + data helpers ----------
    def update_graphs(self, avg_six):
        for i in range(6):
            self.graph_data[i].append(avg_six[i])
            xs = list(range(1, len(self.graph_data[i])+1))
            self.lines[i].set_data(xs, self.graph_data[i])
            self.axes[i].relim(); self.axes[i].autoscale_view()
        self.canvas_mpl.draw_idle()

    def on_ten_m_tick(self, avg_six, speed_kmph, latlonalt):
        if self.project is None or not self.running:
            return
        dir_sign = 1 if self.project["direction"] == "Increasing" else -1
        start_chain = self.current_chainage_km
        end_chain = start_chain + dir_sign * TEN_M_KM
        ts = now_local_str()
        lat, lon, alt = latlonalt
        # Table 2 (10 m)
        self.table2_rows.append([ts, self.project["nh"], f"{start_chain:.3f}", f"{end_chain:.3f}", self.project["direction"], self.project["lane"], f"{lat:.6f}", f"{lon:.6f}", f"{alt:.1f}", f"{speed_kmph:.1f}"])
        # image per 10 m
        self.capture_images(chainage_label=self.format_chainage_label(start_chain))
        # measurement window
        self.measure_accum_km += TEN_M_KM
        self.window_buffer.append({"S": avg_six, "speed": speed_kmph, "lat": lat, "lon": lon})
        if self.measure_accum_km + 1e-9 >= self.project["meas_km"]:
            self.flush_measurement_window(final_ts=ts, end_lat=lat, end_lon=lon, dir_sign=dir_sign, end_chain=end_chain)
        # advance
        self.current_chainage_km = end_chain
        self.update_chainage_label(dir_sign * TEN_M_KM)
        self.update_graphs(avg_six)

    def flush_measurement_window(self, final_ts, end_lat, end_lon, dir_sign, end_chain):
        if not self.window_buffer:
            return
        S_matrix = np.array([w["S"] for w in self.window_buffer])
        mean_S = np.nanmean(S_matrix, axis=0)
        S1,S2,S3,S4,S5,S6 = mean_S
        left_rut = np.nanmean([S1, S2])
        right_rut = np.nanmean([S5, S6])
        avg_rut = np.nanmean([S1, S2, S5, S6])
        left_iri = np.nanmean([S2, S3])
        right_iri = np.nanmean([S4, S5])
        avg_iri = np.nanmean([S2, S3, S4, S5])
        avg_texture = np.nanmean([S1, S2, S3, S4, S5, S6])
        speed_mean = float(np.nanmean([w["speed"] for w in self.window_buffer]))
        start_c = end_chain - dir_sign * self.measure_accum_km
        end_c = start_c + dir_sign * self.measure_accum_km
        lat_start = self.window_buffer[0]["lat"]; lon_start = self.window_buffer[0]["lon"]
        row1 = [final_ts, self.project["nh"], f"{start_c:.3f}", f"{end_c:.3f}", self.project["direction"], self.project["lane"], f"{speed_mean:.1f}", f"{left_iri:.3f}", f"{right_iri:.3f}", f"{avg_iri:.3f}", f"{left_rut:.1f}", f"{right_rut:.1f}", f"{avg_rut:.1f}", f"{avg_texture:.3f}", f"{lat_start:.6f}", f"{lon_start:.6f}", f"{end_lat:.6f}", f"{end_lon:.6f}"]
        self.table1_rows.append(row1)
        self.measure_accum_km = 0.0
        self.window_buffer = []

    def capture_images(self, chainage_label):
        if self.images_dir is None:
            return
        stamp = ts_for_path()
        for cam_idx, worker in self.cameras.items():
            frame = worker.latest_frame
            if frame is None:
                continue
            fname = f"CAM{cam_idx}_{chainage_label}_{stamp}.jpg".replace(" ", "_")
            path = os.path.join(self.images_dir, fname)
            try:
                cv2.imwrite(path, frame)
            except Exception:
                pass

    # ---------- Telemetry/UI queue ----------
    def poll_cam_frames(self):
        try:
            from PIL import Image, ImageTk
            while True:
                cam_idx, frame = self.cam_queue.get_nowait()
                rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                imgtk = ImageTk.PhotoImage(image=Image.fromarray(rgb))
                if cam_idx < len(self.cam_labels):
                    lbl = self.cam_labels[cam_idx]
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
                    if self.running:  # only update while running
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
        if self.stream_dir is None or self.project is None:
            return
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        for cam_idx in CAM_INDEXES:
            out_path = os.path.join(self.stream_dir, f"CAM{cam_idx}_{self.project['survey']}_{self.project['lane']}_{self.project['init_chain']:.3f}.mp4")
            vw = cv2.VideoWriter(out_path, fourcc, CAM_FPS, CAM_RES)
            self.video_writers[cam_idx] = vw

    def close_writers(self):
        for vw in self.video_writers.values():
            try:
                vw.release()
            except Exception:
                pass
        self.video_writers.clear()

    # ---------- Chainage & labels ----------
    def set_status_color(self, color):
        mapping = {"red":"#E53935","yellow":"#FDD835","green":"#43A047"}
        self.chip.configure(fg_color=mapping.get(color, "#E53935"))

    def update_chainage_label(self, _delta_km):
        if self.project is None:
            return
        base = self.project['init_chain']
        total_offset = (self.current_chainage_km - base)
        sign = '+' if total_offset >= 0 else '-'
        self.var_chain.set(f"{base:.0f} {sign} {abs(total_offset):.3f}")

    def format_chainage_label(self, start_chain):
        base = self.project['init_chain'] if self.project else 0.0
        offset = start_chain - base
        sign = '+' if offset >= 0 else '-'
        return f"{base:.0f}{sign}{abs(offset):.3f}"

    # ---------- Misc ----------
    def new_project(self):
        if messagebox.askyesno("New Project", "This will reset configuration. Continue?"):
            self.on_reset()

    def open_manual(self):
        messagebox.showinfo("User Manual", "Hook your manual PDF path in open_manual().")

    def open_calibrate(self):
        win = ctk.CTkToplevel(self)
        win.title("Zero Calibration (S1..S6)")
        win.geometry("360x360")
        entries = []
        for i in range(6):
            ctk.CTkLabel(win, text=f"S{i+1} zero (mm)").pack(pady=4)
            e = ctk.CTkEntry(win)
            e.insert(0, str(self.calib_offsets[i]))
            e.pack(pady=2)
            entries.append(e)
        def apply():
            try:
                offs = [float(e.get()) for e in entries]
                self.calib_offsets = offs
                messagebox.showinfo("Calibration", "Calibration applied.")
            except Exception:
                messagebox.showerror("Calibration", "Please enter numeric values.")
        ctk.CTkButton(win, text="Apply", command=apply).pack(pady=10)

    def get_calibration_offsets(self):
        return list(self.calib_offsets)

    def on_close(self):
        try:
            if self.sensor_worker:
                self.sensor_worker.stop(); self.sensor_worker = None
            self.close_writers()
            self.stop_cameras()
        finally:
            self.destroy()

if __name__ == "__main__":
    app = NSVApp()
    app.mainloop()
