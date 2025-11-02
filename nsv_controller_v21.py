#!/usr/bin/env python3
"""
NSV Controller v21 — integrated updates (finalized):
- Plots update once per chainage tick (one point per 10 m) and keep last 20 points.
- Sensor values < 0 are set to 0. Values > 1000 replaced by average of last 20 points (fallback 1000).
- Single-row live calibration UI, averaged capture, persisted offsets.
- Absolute-valued metrics for Rutting / IRI / Texture.
- Header 3rd column nudged right by 30 px.
- Calibration required before a run (prompt).
- Other robustness and logging improvements kept from earlier iteration.
"""
import os
import time
import math
import threading
import queue
import random
from datetime import datetime
import logging

import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox

import numpy as np
import cv2
from collections import deque
from MEDAQLib import MEDAQLib, ME_SENSOR, ERR_CODE  # Requires medaqlib available

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

from PIL import Image

from math import radians, sin, cos, sqrt, atan2

# ---------------------------
# Logging Setup
# ---------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------
# Layout / Runtime constants
# ---------------------------
LEFT_WIDTH = 300
WINDOW_GEOM = "1680x960"

CAM_IDS = [0, 1, 2, 3]
CAM_NAMES = {0: "Front", 1: "Left", 2: "Right", 3: "Back"}
CAM_URLS = [
    "rtsp://192.168.1.103:554/stream1",  # Front - replace with actual URL
    "rtsp://192.168.1.102:554/stream1",  # Left - replace with actual URL
    "rtsp://192.168.1.100:554/stream1",  # Right - replace with actual URL
    "rtsp://192.168.1.101:554/stream0?username=admin&password=E10ADC3949BA59ABBE56E057F20F883E"   # Back - replace with actual URL
]
PREVIEW_RES = (360, 202)     # (w,h) fast UI
RECORD_RES  = (1280, 720)    # (w,h) saved video/images
CAM_FPS     = 25
PREVIEW_FPS = 15             # Lower FPS for preview to reduce load
TOF_HZ      = 100
BIN_SIZE_METERS = 10.0
BIN_SIZE_KM = 0.01
NEON_GREEN  = "#39ff14"
HEADER_COLOR= (255, 0, 0)    # BGR: blue (comment preserved)
TEXT_COLOR  = (255, 255, 255)
FONT        = cv2.FONT_HERSHEY_SIMPLEX
BG_COLOR    = "#0b0f14"
GRID_COLOR  = "#263238"
SPINE_COLOR = "#607d8b"
HEADER_MAX_FRAC = 0.20       # Header height 20% of frame
HEADER_LINES = 5             # Lines per column (max for Col2)
LOGO_PATH = "cropped-Roadworks-LogoR-768x576-1.jpeg"       # Path to your logo image
MAX_QUEUE_SIZE = 5           # Limit queue to prevent backlog

# Constants for laser sensors
SENSORS = 13
# COM_PORTS must be set correctly for your system
COM_PORTS = [f"COM{i}" for i in [13,12,5,12,10,12,10,10,10,10,10,10,10]]
EXPECTED_BLOCK_SIZE = 1  # Fetch one value per sensor per cycle for 100 Hz
TOF_HZ = 100

# Graph history size — show last N points
GRAPH_HISTORY = 20

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
    """Compose header at target size (w,h) with updated columns and smaller font."""
    w, h = size

    # Ensure `frame` is a numpy BGR image of shape (H,W,3)
    if frame is None or not isinstance(frame, (np.ndarray,)) or frame.size == 0:
        frame = np.zeros((h, w, 3), dtype=np.uint8)
    else:
        # If PIL image passed in convert to BGR numpy
        if hasattr(frame, "mode"):
            frame = cv2.cvtColor(np.array(frame), cv2.COLOR_RGBA2BGR if frame.mode == "RGBA" else cv2.COLOR_RGB2BGR)
        frame = cv2.resize(frame, (w, h))

    # Reduced font scale for more lines
    if h >= 1080:
        font_scale = 0.6
    elif h >= 720:
        font_scale = 0.5
    elif h >= 480:
        font_scale = 0.4
    else:
        font_scale = 0.3
    thickness = 1

    # Measure text to compute line height
    (_, th), base = cv2.getTextSize("Ag", FONT, font_scale, thickness)
    line_h = max(th + base, int(16 * font_scale))
    pad_x = max(10, int(10 * (h / 480)))
    pad_top = max(10, int(12 * (h / 480)))
    pad_bottom = max(5, int(8 * (h / 480)))

    # Determine required header height
    needed_h = pad_top + HEADER_LINES * line_h + pad_bottom
    max_h = int(h * HEADER_MAX_FRAC)
    header_h = min(needed_h, max_h) if needed_h > 0 else max_h
    # Shrink font if needed
    attempts = 0
    while needed_h > max_h and attempts < 5 and font_scale > 0.3:
        font_scale *= 0.85
        (_, th), base = cv2.getTextSize("Ag", FONT, font_scale, thickness)
        line_h = max(th + base, int(16 * font_scale))
        needed_h = pad_top + HEADER_LINES * line_h + pad_bottom
        attempts += 1
    header_h = min(needed_h, max_h)

    # Draw header
    cv2.rectangle(frame, (0,0), (w, header_h), HEADER_COLOR, -1)

    # Build text columns
    col1 = [
        f"Project ID: {ctx.get('survey','--')}",
        f"Survey Loc: {ctx.get('survey_location','--')}",
        f"NH Number: {ctx.get('nh','--')}",
        f"Old NH Num: {ctx.get('oldnh','--')}",
        f"Section: {ctx.get('section_code','--')}",
        f"Direction: {ctx.get('direction','--')}"
    ]
    col2 = [
        f"Latitude: {ctx.get('lat','--')}",
        f"Longitude: {ctx.get('lon','--')}",
        f"Altitude: {ctx.get('alt','--')} m",
        f"Dist: {ctx.get('distance','--')} m | Chain: {ctx.get('chainage','--')} Km | Speed: {ctx.get('speed','--')} km/h",
        f"LRP: {ctx.get('lrp','--')} Km"
    ]
    col3 = [
        f"Date: {ctx.get('date','--')}",
        f"Time: {ctx.get('time','--')}"
    ]
    cols = [col1[:5], col2, col3]  # Limit col1 to 5 lines to match col2

    # Render text
    col_w = w // 3
    y = pad_top
    for ci, lines in enumerate(cols):
        # shift third column slightly right to reduce overlap (tweak value as required)
        x = pad_x + ci * col_w + (30 if ci == 2 else 0)
        y = pad_top
        for line in lines:
            y += line_h
            cv2.putText(frame, line, (x, y), FONT, font_scale, TEXT_COLOR, thickness, cv2.LINE_AA)

    return frame

def haversine(lat1, lon1, lat2, lon2):
    R = 6371e3  # meters
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def read_laser_values(sensors):
    """Read scaled values from Micro-Epsilon ILD1320 lasers."""
    values = [float('nan')] * len(sensors)
    for i, sensor in enumerate(sensors):
        if sensor is None:
            logger.warning(f"Sensor {i+1} not initialized")
            continue
        try:
            # Check if data is available
            if sensor.DataAvail() >= EXPECTED_BLOCK_SIZE and sensor.GetLastError() == ERR_CODE.ERR_NOERROR:
                # Fetch one scaled value
                transferred_data = sensor.TransferData(EXPECTED_BLOCK_SIZE)
                if sensor.GetLastError() == ERR_CODE.ERR_NOERROR and transferred_data[2] > 0:
                    values[i] = transferred_data[1][-1]  # Latest scaled value
                else:
                    error_msg = sensor.GetError(1024)
                    logger.warning(f"Sensor {i+1} data error: {error_msg}")
            else:
                error_msg = sensor.GetError(1024)
                logger.warning(f"Sensor {i+1} data unavailable: {error_msg}")
        except Exception as e:
            logger.error(f"Sensor {i+1} read failed: {str(e)}")
    return values

def initialize_lasers():
    """Initialize up to 13 ILD1320 sensors on specified COM ports."""
    sensors = [None] * SENSORS
    for i in range(SENSORS):
        try:
            sensor = MEDAQLib.CreateSensorInstance(ME_SENSOR.SENSOR_ILD1320)
            if sensor.iSensor == 0:
                logger.error(f"Sensor {i+1} failed to create instance")
                continue
            # Configure RS232 interface
            sensor.SetParameterString("IP_Interface", "RS232")
            sensor.SetParameterString("IP_Port", COM_PORTS[i])
            sensor.SetParameterInt("IP_AutomaticMode", 3)
            sensor.SetParameterInt("IP_EnableLogging", 1)
            sensor.SetParameterString("IP_LogFile", f"MEDAQLib-log-S{i+1}.txt")
            # Open sensor
            sensor.OpenSensor()
            if sensor.GetLastError() != ERR_CODE.ERR_NOERROR:
                error_msg = sensor.GetError(1024)
                logger.error(f"Sensor {i+1} failed to open: {error_msg}")
                sensor.CloseSensor()
                sensor.ReleaseSensorInstance()
                continue
            sensors[i] = sensor
            logger.info(f"Sensor {i+1} initialized on {COM_PORTS[i]}")
        except Exception as e:
            logger.error(f"Sensor {i+1} setup failed: {str(e)}")
    return sensors

def cleanup_lasers(sensors):
    """Close and release all laser sensor instances."""
    for i, sensor in enumerate(sensors):
        if sensor is not None:
            try:
                sensor.CloseSensor()
                sensor.ReleaseSensorInstance()
                logger.info(f"Sensor {i+1} closed")
            except Exception as e:
                logger.error(f"Sensor {i+1} cleanup failed: {str(e)}")

# ---------------------------
# Camera Worker
# ---------------------------
class CameraWorker(threading.Thread):
    def __init__(self, cam_id, out_queue, get_writer, get_overlay_ctx,
                 preview_size=PREVIEW_RES, record_size=RECORD_RES):
        super().__init__(daemon=True)
        self.cam_id = cam_id
        self.cam_url = CAM_URLS[cam_id]
        self.out_queue = out_queue
        self.get_writer = get_writer
        self.get_overlay_ctx = get_overlay_ctx
        self.preview_size = preview_size
        self.record_size = record_size
        self.stop_event = threading.Event()
        self.cap = None
        self.latest_preview_frame = None
        self.latest_record_frame = None
        self.latest_frame_ts = 0.0
        self.last_frame_time = 0.0
        self.frame_interval = 1.0 / PREVIEW_FPS  # Control preview frame rate
        self.consecutive_fails = 0

    def _try_open(self, backend_flag):
        # Configure low-latency RTSP
        if backend_flag == cv2.CAP_FFMPEG:
            os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;udp|fflags;nobuffer|probesize;32768|analyzeduration;100000"
        try:
            cap = cv2.VideoCapture(self.cam_url, backend_flag)
            if cap and getattr(cap, "isOpened", lambda: False)():
                # Set smaller buffer size if supported
                try: cap.set(cv2.CAP_PROP_BUFFERSIZE, 2)
                except Exception: pass
            return cap
        except Exception as e:
            logger.warning(f"_try_open backend {backend_flag} failed: {e}")
            return None

    def _open_cap(self):
        for backend in (cv2.CAP_FFMPEG, cv2.CAP_MSMF, cv2.CAP_DSHOW, cv2.CAP_ANY):
            cap = self._try_open(backend)
            if cap is not None and getattr(cap, "isOpened", lambda: False)():
                self.cap = cap
                logger.info(f"Camera {self.cam_id} opened with backend {backend}")
                return True
            else:
                try:
                    if cap is not None:
                        cap.release()
                except Exception:
                    pass
        logger.error(f"Failed to open camera {self.cam_id}")
        return False

    def run(self):
        if not self._open_cap():
            logger.error(f"Camera {self.cam_id} initial open failed")
        while not self.stop_event.is_set():
            if self.cap is None:
                time.sleep(1)
                self._open_cap()
                continue
            start_time = time.time()
            ok, frame = self.cap.read()
            if not ok or frame is None or frame.size == 0:
                self.consecutive_fails += 1
                logger.warning(f"Camera {self.cam_id} read failed ({self.consecutive_fails})")
                if self.consecutive_fails > 5:
                    frame = make_testcard(self.record_size[0], self.record_size[1], f"{CAM_NAMES.get(self.cam_id, f'CAM {self.cam_id}')} PERMANENT ERROR")
                else:
                    try:
                        self.cap.release()
                    except Exception: pass
                    self.cap = None
                    time.sleep(0.5)
                    continue
            else:
                self.consecutive_fails = 0
            ctx = self.get_overlay_ctx()
            preview = cv2.resize(frame, self.preview_size)
            record = draw_header_to_size(frame, ctx, self.record_size)
            preview_rgb = cv2.cvtColor(preview, cv2.COLOR_BGR2RGB)
            self.latest_preview_frame = preview_rgb
            self.latest_record_frame = record
            self.latest_frame_ts = time.time()
            if start_time - self.last_frame_time >= self.frame_interval:
                try:
                    self.out_queue.put_nowait((self.cam_id, preview_rgb))
                    self.last_frame_time = start_time
                except queue.Full:
                    logger.warning(f"Camera {self.cam_id} queue full, dropping frame")
            writer = self.get_writer(self.cam_id)
            if writer is not None:
                try:
                    writer.write(record)
                except Exception as e:
                    logger.warning(f"Failed to write frame for cam {self.cam_id}: {e}")
            elapsed = time.time() - start_time
            sleep_time = max(0, 1.0 / CAM_FPS - elapsed)
            time.sleep(sleep_time)
        if self.cap is not None:
            try: self.cap.release()
            except Exception: pass
            logger.info(f"Camera {self.cam_id} released")

    def stop(self):
        self.stop_event.set()

# ---------------------------
# Simulated Inputs (replace with real drivers)
# ---------------------------
sim_lat = 17.385
sim_lon = 78.486
sim_t = time.time()

def sim_speed_kmph(t):
    return max(0.0, 45 + 10*math.sin(2*math.pi*t/20.0) + random.uniform(-3,3))

def sim_gps():
    global sim_lat, sim_lon, sim_t
    t = time.time()
    dt = t - sim_t
    sim_t = t
    speed_kmph = sim_speed_kmph(t)
    speed_mps = speed_kmph / 3.6
    delta_m = speed_mps * dt
    dir_s = 1 if math.sin(t/30) > 0 else -1  # simulate occasional reversals
    delta_lat = (delta_m * dir_s) / 111120  # approximate meters to degrees
    sim_lat += delta_lat
    sim_lat += random.uniform(-1e-6, 1e-6)
    sim_lon += random.uniform(-1e-6, 1e-6)
    alt = 505 + random.uniform(-1, 1)
    return sim_lat, sim_lon, alt

def sim_tof_values():
    return [800 + random.gauss(0, 4) for _ in range(SENSORS)]

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
        self.prev_lat = None
        self.prev_lon = None
        self.prev_t = None
        self.bin_start_lat = None
        self.bin_start_lon = None
        self.sensors = initialize_lasers()  # Initialize lasers
        self.reset()

    def reset(self):
        self.distance_m = 0.0
        self.last_t = time.time()
        self.next_ten_m_edge = BIN_SIZE_METERS
        self.bin_buffers = [[] for _ in range(SENSORS)]
        self.last_speed = 0.0
        self.bin_start_lat = None
        self.bin_start_lon = None

    def run(self):
        while not self.stop_event.is_set():
            t = time.time()
            if self.paused_event.is_set():
                self.prev_t = t
                time.sleep(0.01)
                continue

            lat, lon, alt = sim_gps()  # Replace with real GPS in production
            raw = read_laser_values(self.sensors)  # Read from lasers
            try: self.raw_setter(raw)
            except Exception: pass

            calib_vals = self.calib_getter()
            vals = [raw[i] - calib_vals[i] if np.isfinite(raw[i]) else float('nan') for i in range(SENSORS)]
            for i, v in enumerate(vals):
                self.bin_buffers[i].append(v)

            if self.bin_start_lat is None:
                self.bin_start_lat = lat
                self.bin_start_lon = lon

            if self.prev_lat is not None:
                dist_delta = haversine(self.prev_lat, self.prev_lon, lat, lon)
                dt = t - self.prev_t
                speed_mps = dist_delta / dt if dt > 0 else 0.0
                self.distance_m += dist_delta
                self.last_speed = speed_mps * 3.6
            self.prev_lat = lat
            self.prev_lon = lon
            self.prev_t = t

            self.ui_queue.put({
                "type": "telemetry",
                "speed_kmph": self.last_speed,
                "distance_m": self.distance_m,
                "lat": lat, "lon": lon, "alt": alt,
            })

            if self.distance_m >= self.next_ten_m_edge:
                avgs = [float(np.nanmean(buf)) if len(buf) else float("nan") for buf in self.bin_buffers]
                self.bin_buffers = [[] for _ in range(SENSORS)]
                start_lat = self.bin_start_lat
                start_lon = self.bin_start_lon
                self.ui_queue.put({
                    "type": "ten_m",
                    "avg_s": avgs,
                    "speed_kmph": self.last_speed,
                    "lat": lat, "lon": lon, "alt": alt,
                    "start_lat": start_lat, "start_lon": start_lon
                })
                # Callback with raw avgs (sanitization performed in app before storing/plotting)
                self.ten_m_callback(avgs, self.last_speed, (lat, lon, alt), (start_lat, start_lon, alt))
                self.next_ten_m_edge += BIN_SIZE_METERS
                self.bin_start_lat = lat
                self.bin_start_lon = lon

            time.sleep(max(0.0, 1.0/TOF_HZ - (time.time()-t)))

    def stop(self):
        self.stop_event.set()
        cleanup_lasers(self.sensors)
        self.sensors = [None] * SENSORS

# ---------------------------
# Main App
# ---------------------------
class NSVApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        self.title("NSV Controller")
        self.geometry(WINDOW_GEOM)

        self.grid_columnconfigure(0, weight=0, minsize=LEFT_WIDTH)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.ui_queue = queue.Queue()
        self.cam_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE * len(CAM_IDS))
        self.cameras = {}
        self.sensor_worker = None

        self.running = False
        self.paused_event = threading.Event()
        self.project = None
        self.calib_values = [0.0] * SENSORS
        self.calibrated = False
        self.controls_enabled = False

        self.cam_selected = {idx: tk.BooleanVar(value=True) for idx in CAM_IDS}
        self.sensor_selected = [tk.BooleanVar(value=True if i < 6 else False) for i in range(SENSORS)]

        self.video_writers = {}
        self.run_dir = self.images_dir = self.stream_dir = self.data_dir = None

        self.table1_rows, self.table2_rows = [], []
        self.measure_accum_km = 0.0
        self.window_buffer = []
        self.current_chainage_km = None

        self.last_lat = self.last_lon = self.last_alt = None
        self.last_speed = 0.0
        self.last_distance_m = 0.0
        self.latest_raw = [float('nan')]*SENSORS

        # Graph storage: last GRAPH_HISTORY points per sensor
        self.selected_sensor_indices = [i for i, v in enumerate(self.sensor_selected) if v.get()]
        self.graph_data = [deque(maxlen=GRAPH_HISTORY) for _ in range(SENSORS)]

        # calibration rolling buffers (for the window)
        self._calib_buffers = [deque(maxlen=150) for _ in range(SENSORS)]

        self.build_menu()
        self.build_layout()
        self.left.configure(width=LEFT_WIDTH)
        self.left.grid_propagate(False)

        self.set_controls_state(False)
        self.set_selection_state(True)

        for idx in CAM_IDS: self.start_camera(idx)
        self.after(int(1000 / PREVIEW_FPS), self.poll_cam_frames)  # Match preview FPS
        self.after(60, self.poll_ui_queue)

    # ---------- Menu ----------
    def build_menu(self):
        menubar = tk.Menu(self)
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="New Project", command=self.new_project)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.on_close)
        menubar.add_cascade(label="File", menu=file_menu)
        cal_menu = tk.Menu(menubar, tearoff=0)
        cal_menu.add_command(label="Calibrate and Select Sensors", command=self.open_calibrate)
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

        self.merged_right = ctk.CTkFrame(self, corner_radius=12)
        self.merged_right.grid(row=0, column=1, sticky="nsew", padx=(4,8), pady=8)
        self.merged_right.grid_rowconfigure(0, weight=0)
        self.merged_right.grid_rowconfigure(1, weight=1)
        self.merged_right.grid_columnconfigure(0, weight=1)

        top = ctk.CTkFrame(self.merged_right)
        top.grid(row=0, column=0, sticky="ew", padx=4, pady=4)
        top.grid_columnconfigure(0, weight=3)  # cameras
        top.grid_columnconfigure(1, weight=1)  # telemetry

        cam_container = ctk.CTkFrame(top)
        cam_container.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
        self.build_cameras(cam_container)

        tele_container = ctk.CTkFrame(top)
        tele_container.grid(row=0, column=1, sticky="ns", padx=4, pady=4)
        self.build_right_panel(tele_container)

        bottom = ctk.CTkFrame(self.merged_right)
        bottom.grid(row=1, column=0, sticky="nsew", padx=4, pady=4)
        self.build_graphs(bottom)

    # ---------- Left Panel ----------
    def build_left_panel(self, f):
        pad = {"padx":6, "pady":4}
        ctk.CTkLabel(f, text="Project Setup", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, sticky="w", **pad)
        def add_row(r, label, widget):
            ctk.CTkLabel(f, text=label, anchor="e").grid(row=r, column=0, sticky="e", **pad)
            widget.grid(row=r, column=1, sticky="ew", **pad)
        entry_w = 60
        self.ent_survey   = ctk.CTkEntry(f, width=entry_w)
        self.ent_survey.insert(0, "Hyderabad to Vijayawada")
        self.ent_survey_location = ctk.CTkEntry(f, width=entry_w)
        self.ent_survey_location.insert(0, "Hyderabad")
        self.ent_client   = ctk.CTkEntry(f, width=entry_w)
        self.ent_client.insert(0, "NHAI")
        self.ent_nh       = ctk.CTkEntry(f, width=entry_w)
        self.ent_nh.insert(0, "NH9")
        self.ent_oldnh    = ctk.CTkEntry(f, width=entry_w)
        self.ent_oldnh.insert(0, "NH9")
        self.ent_section  = ctk.CTkEntry(f, width=entry_w)
        self.ent_section.insert(0, "1")
        self.ent_lrp      = ctk.CTkEntry(f, width=entry_w)
        self.ent_lrp.insert(0, "00.000")
        add_row(1,  "Survey ID",        self.ent_survey)
        add_row(2,  "Survey Location",  self.ent_survey_location)
        add_row(3,  "Client Name/ID",   self.ent_client)
        add_row(4,  "NH Number",        self.ent_nh)
        add_row(5,  "Old NH Number",    self.ent_oldnh)
        add_row(6,  "Section Code",     self.ent_section)
        add_row(7,  "LRP (Km)",         self.ent_lrp)

        self.direction_var = ctk.StringVar(value="Increasing")
        self.direction_dd  = ctk.CTkOptionMenu(f, values=["Increasing", "Decreasing"], variable=self.direction_var, command=self.on_direction_select, width=entry_w)
        add_row(8, "Direction", self.direction_dd)

        self.lane_var = ctk.StringVar(value="L1")
        self.lane_dd  = ctk.CTkOptionMenu(f, values=[f"L{i}" for i in range(1,11)], variable=self.lane_var, width=entry_w)
        add_row(9, "Lane No.", self.lane_dd)

        self.cmb_pave = ctk.CTkComboBox(f, values=["Concrete", "Bituminous"], width=entry_w)
        self.cmb_pave.set("Bituminous")
        add_row(10, "Pavement Type", self.cmb_pave)

        self.ent_chainage   = ctk.CTkEntry(f, width=entry_w)
        self.ent_chainage.insert(0, "100")
        self.ent_section_km = ctk.CTkEntry(f, width=entry_w)
        self.ent_section_km.insert(0, "0.01")
        add_row(11, "Initial Chainage (Km)",     self.ent_chainage)
        add_row(12, "Measurement \nSection (Km)",  self.ent_section_km)

        row0 = 13
        ctk.CTkLabel(f, text="Export Cameras", font=ctk.CTkFont(size=14, weight="bold")).grid(row=row0, column=0, columnspan=2, sticky="w", padx=8, pady=(12,4))
        cam_row = row0 + 1
        self.cam_checkboxes = []
        for i, cam_id in enumerate(CAM_IDS):
            chk = ctk.CTkCheckBox(f, text=CAM_NAMES[cam_id], variable=self.cam_selected[cam_id])
            chk.grid(row=cam_row, column=i % 2, sticky="w", padx=2, pady=2)
            if i % 2 == 1: cam_row += 1
            self.cam_checkboxes.append(chk)

        self.btn_set_specs = ctk.CTkButton(f, text="Set Project Specifications", command=self.set_project_specs)
        self.btn_set_specs.grid(row=cam_row +1, column=0, columnspan=2, sticky="ew", padx=8, pady=(16,8))
        self.lbl_proj_status = ctk.CTkLabel(f, text="Project not configured.")
        self.lbl_proj_status.grid(row=cam_row +2, column=0, columnspan=2, sticky="w", padx=8, pady=(0,8))

        self.left_widgets = [self.ent_survey, self.ent_survey_location, self.ent_client, self.ent_nh, self.ent_oldnh, self.ent_section,
                             self.ent_lrp, self.direction_dd, self.lane_dd, self.cmb_pave, self.ent_chainage, self.ent_section_km, self.btn_set_specs]

    def on_direction_select(self, choice):
        lanes = [f"L{i}" for i in range(1, 11)] if choice == "Increasing" else [f"R{i}" for i in range(1, 11)]
        current = self.lane_var.get().strip()
        idx = max(1, min(10, int(current[1:]) if len(current) >= 2 and current[1:].isdigit() else 1)) - 1
        self.lane_dd.configure(values=lanes)
        self.lane_dd.set(lanes[idx])
        self.lane_var.set(lanes[idx])

    # ---------- Cameras ----------
    def build_cameras(self, parent):
        f = ctk.CTkFrame(parent)
        f.pack(fill="both", expand=True)
        for c in range(2): f.grid_columnconfigure(c, weight=1, uniform="cams")
        f.grid_rowconfigure(0, weight=1)
        f.grid_rowconfigure(1, weight=1)
        self.cam_labels = []
        for i, cam_id in enumerate(CAM_IDS):
            r, c = divmod(i, 2)
            frame = ctk.CTkFrame(f)
            frame.grid(row=r, column=c, padx=6, pady=6, sticky="nsew")
            ctk.CTkLabel(frame, text=CAM_NAMES.get(cam_id, f"Camera {cam_id}")).pack(anchor="w")
            lbl = ctk.CTkLabel(frame, text="", width=PREVIEW_RES[0], height=PREVIEW_RES[1])
            lbl.pack(fill="both", expand=True)
            self.cam_labels.append(lbl)

    # ---------- Graphs ----------
    def build_graphs(self, parent):
        self.graph_container = parent
        self.rebuild_graphs()

    def rebuild_graphs(self):
        for widget in self.graph_container.winfo_children():
            widget.destroy()

        self.axes = []
        self.lines = []
        self.selected_sensor_indices = [i for i, v in enumerate(self.sensor_selected) if v.get()]
        num = len(self.selected_sensor_indices)
        if num == 0:
            ctk.CTkLabel(self.graph_container, text="No sensors selected").pack(expand=True)
            return

        rows = math.ceil(num / 3)
        cols = min(num, 3)
        fig = Figure(figsize=(12, rows * 2.5), dpi=100, facecolor=BG_COLOR)
        fig.tight_layout(pad=0.5)
        for j, sens_idx in enumerate(self.selected_sensor_indices):
            ax = fig.add_subplot(rows, cols, j+1, facecolor=BG_COLOR)
            # Only show plot (no axes, ticks, spines)
            ax.set_xlabel("")
            ax.set_ylabel("")
            ax.set_xticks([])
            ax.set_yticks([])
            ax.tick_params(axis='both', which='both', length=0)
            for s in ax.spines.values():
                s.set_visible(False)
            ax.grid(False)
            try:
                ax.margins(0)
            except Exception:
                pass
            self.axes.append(ax)
            line, = ax.plot([], [], NEON_GREEN, linewidth=1.5)
            self.lines.append(line)

        self.canvas_mpl = FigureCanvasTkAgg(fig, master=self.graph_container)
        self.canvas_mpl.draw()
        self.canvas_mpl.get_tk_widget().pack(fill="both", expand=True)

    # ---------- Right Panel (Telemetry & Controls) ----------
    def build_right_panel(self, parent):
        f = parent
        pad = {"padx":10, "pady":4}
        ctk.CTkLabel(f, text="Telemetry & Controls", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, sticky="w", **pad)
        chip_frame = ctk.CTkFrame(f, fg_color="transparent")
        chip_frame.grid(row=1, column=0, sticky="w", **pad)
        self.chip = ctk.CTkLabel(chip_frame, text="  ", width=16, height=16, corner_radius=8)
        self.chip.grid(row=0, column=0, padx=(0,6))
        self.set_status_color("red")
        self.lbl_status = ctk.CTkLabel(chip_frame, text="Status: stopped")
        self.lbl_status.grid(row=0, column=1)

        grid = ctk.CTkFrame(f)
        grid.grid(row=2, column=0, sticky="ew", padx=10, pady=(4,4))
        for i in range(2): grid.grid_columnconfigure(i, weight=1)
        self.var_speed = ctk.StringVar(value="0.0 km/h")
        self.var_dist  = ctk.StringVar(value="0.0 m")
        self.var_chain = ctk.StringVar(value="-- + 0.000 Km")
        self.var_lat = ctk.StringVar(value="--")
        self.var_lon = ctk.StringVar(value="--")
        self.var_alt = ctk.StringVar(value="-- m")
        ctk.CTkLabel(grid, text="Speed:").grid(row=0, column=0, sticky="w", padx=(0,6))
        ctk.CTkLabel(grid, textvariable=self.var_speed).grid(row=0, column=1, sticky="w")
        ctk.CTkLabel(grid, text="Distance:").grid(row=1, column=0, sticky="w", padx=(0,6))
        ctk.CTkLabel(grid, textvariable=self.var_dist).grid(row=1, column=1, sticky="w")
        ctk.CTkLabel(grid, text="Chainage:").grid(row=2, column=0, sticky="w", padx=(0,6))
        ctk.CTkLabel(grid, textvariable=self.var_chain).grid(row=2, column=1, sticky="w")
        ctk.CTkLabel(grid, text="Latitude:").grid(row=3, column=0, sticky="w", padx=(0,6))
        ctk.CTkLabel(grid, textvariable=self.var_lat).grid(row=3, column=1, sticky="w")
        ctk.CTkLabel(grid, text="Longitude:").grid(row=4, column=0, sticky="w", padx=(0,6))
        ctk.CTkLabel(grid, textvariable=self.var_lon).grid(row=4, column=1, sticky="w")
        ctk.CTkLabel(grid, text="Altitude:").grid(row=5, column=0, sticky="w", padx=(0,6))
        ctk.CTkLabel(grid, textvariable=self.var_alt).grid(row=5, column=1, sticky="w")

        ctk.CTkLabel(f, text="Speed Progress:").grid(row=6, column=0, sticky="w", **pad)
        self.speed_progress = ctk.CTkProgressBar(f, mode='determinate', width=200)
        self.speed_progress.grid(row=7, column=0, sticky="ew", padx=10, pady=4)
        self.speed_progress.set(0)

        self.btn_toggle_dir = ctk.CTkButton(f, text="Toggle Direction", command=self.toggle_direction)
        self.btn_toggle_dir.grid(row=8, column=0, sticky="ew", padx=10, pady=4)

        btns = ctk.CTkFrame(f)
        btns.grid(row=9, column=0, sticky="ew", padx=10, pady=(8,6))
        btns.grid_rowconfigure((0,1), weight=1)
        btns.grid_columnconfigure((0,1,2), weight=1)

        # Initialize buttons
        self.btn_start = ctk.CTkButton(btns, text="Start", command=self.on_start)
        self.btn_pause = ctk.CTkButton(btns, text="Pause", command=self.on_pause)
        self.btn_export = ctk.CTkButton(btns, text="Export", command=self.on_export)
        self.btn_stop = ctk.CTkButton(btns, text="Stop", command=self.on_stop)
        self.btn_reset = ctk.CTkButton(btns, text="Reset", command=self.on_reset)

        # Arrange buttons in two rows
        self.btn_start.grid(row=0, column=0, padx=4, pady=4, sticky="ew")
        self.btn_pause.grid(row=0, column=1, padx=4, pady=4, sticky="ew")
        self.btn_export.grid(row=0, column=2, padx=4, pady=4, sticky="ew")
        self.btn_stop.grid(row=1, column=0, padx=4, pady=4, sticky="ew")
        self.btn_reset.grid(row=1, column=1, padx=4, pady=4, sticky="ew")

    def toggle_direction(self):
        if not self.running:
            return
        current = self.project['direction']
        new_dir = "Decreasing" if current == "Increasing" else "Increasing"
        self.project['direction'] = new_dir
        self.direction_var.set(new_dir)
        self.on_direction_select(new_dir)
        messagebox.showinfo("Direction Toggled", f"Direction changed to {new_dir}. Chainage will adjust accordingly.")

    # ---------- Enable/Disable controls ----------
    def set_controls_state(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for btn in [self.btn_start, self.btn_pause, self.btn_export, self.btn_stop, self.btn_reset, self.btn_toggle_dir]:
            btn.configure(state=state)
        self.controls_enabled = enabled

    def set_selection_state(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for cb in getattr(self, 'cam_checkboxes', []): cb.configure(state=state)

    def set_left_state(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for w in self.left_widgets:
            try: w.configure(state=state)
            except Exception: pass

    # ---------- Cameras ----------
    def start_camera(self, cam_id):
        if cam_id in self.cameras: return
        cw = CameraWorker(cam_id, self.cam_queue, self.get_writer_for, self.get_overlay_context)
        self.cameras[cam_id] = cw
        cw.start()

    def stop_cameras(self):
        for cw in self.cameras.values(): cw.stop()
        for cw in self.cameras.values():
            try: cw.join(timeout=1.0)
            except Exception: pass
        self.cameras.clear()

    def get_writer_for(self, cam_id):
        if not self.running or self.paused_event.is_set() or self.stream_dir is None:
            return None
        if not self.cam_selected[cam_id].get():
            return None
        return self.video_writers.get(cam_id)

    def get_overlay_context(self):
        p = self.project or {}
        lat = f"{self.last_lat:.6f}" if isinstance(self.last_lat, (int,float)) else "--"
        lon = f"{self.last_lon:.6f}" if isinstance(self.last_lon, (int,float)) else "--"
        alt = f"{self.last_alt:.1f}" if isinstance(self.last_alt, (int,float)) else "--"
        dist = f"{self.last_distance_m:.1f}"
        if self.project and self.current_chainage_km is not None:
            base = p['init_chain']
            offset = self.current_chainage_km - base
            sign = '+' if offset >= 0 else '-'
            chain = f"{base:.0f} {sign} {abs(offset):.3f} Km"
        else:
            chain = "-- + 0.000 Km"
        dt = datetime.now()
        lrp = p.get('lrp', '--')
        try:
            lrp = f"{float(lrp):.3f}" if lrp != '--' else '--'
        except Exception:
            lrp = '--'
        return {
            'survey': p.get('survey','--'),
            'survey_location': p.get('survey_location','--'),
            'nh': p.get('nh','--'),
            'oldnh': p.get('oldnh','--'),
            'section_code': p.get('section_code','--'),
            'direction': p.get('direction','--'),
            'lane': p.get('lane','--'),
            'lat': lat,
            'lon': lon,
            'alt': alt,
            'distance': dist,
            'chainage': chain,
            'speed': f"{self.last_speed:.1f}",
            'lrp': lrp,
            'date': dt.strftime('%d-%m-%Y'),
            'time': dt.strftime('%H:%M:%S'),
        }

    # ---------- Project specs ----------
    def set_project_specs(self):
        get = lambda e: e.get().strip()
        survey = get(self.ent_survey)
        survey_location = get(self.ent_survey_location)
        nh = get(self.ent_nh)
        client = get(self.ent_client)
        oldnh = get(self.ent_oldnh)
        section_code = get(self.ent_section)
        lrp = get(self.ent_lrp)
        direction = self.direction_var.get().strip()
        lane = self.lane_var.get().strip()
        pave = self.cmb_pave.get().strip()
        try:
            init_chain = float(get(self.ent_chainage))
            meas_km = float(get(self.ent_section_km))
            lrp_float = float(lrp)
            if not lrp_float >= 0:
                raise ValueError("LRP must be non-negative.")
        except Exception:
            messagebox.showerror("Invalid Input", "Initial Chainage, Measurement Section, and LRP must be numbers, and LRP must be non-negative.")
            return
        if not all([survey, survey_location, nh, client, section_code, lrp, direction, lane, pave]):
            messagebox.showerror("Missing Input", "Please fill all fields.")
            return
        if meas_km <= 0:
            messagebox.showerror("Invalid Section", "Measurement Section must be > 0.")
            return
        base = filedialog.askdirectory(title="Choose base folder for project")
        if not base: return
        self.build_project_paths(base, survey, direction, lane)
        self.project = {
            "survey": survey,
            "survey_location": survey_location,
            "nh": nh,
            "client": client,
            "oldnh": oldnh,
            "section_code": section_code,
            "lrp": lrp_float,
            "direction": direction,
            "lane": lane,
            "pave": pave,
            "init_chain": init_chain,
            "meas_km": meas_km
        }
        self.lbl_proj_status.configure(text=f"Specs set → {survey} | {direction} {lane}\nRun folder: {self.run_dir}")
        self.current_chainage_km = init_chain
        self.update_chainage_label(0.0)
        self.table1_rows.clear()
        self.table2_rows.clear()
        self.measure_accum_km = 0.0
        self.window_buffer = []
        self.graph_data = [deque(maxlen=GRAPH_HISTORY) for _ in range(SENSORS)]
        self.rebuild_graphs()
        self.set_left_state(False)
        self.set_controls_state(True)
        self.set_selection_state(False)
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
        self.set_status_color("red")
        self.lbl_status.configure(text="Status: stopped")
        self.var_speed.set("0.0 km/h")
        self.var_dist.set("0.0 m")
        self.var_chain.set("-- + 0.000 Km")
        self.var_lat.set("--")
        self.var_lon.set("--")
        self.var_alt.set("-- m")
        self.speed_progress.set(0)
        self.graph_data = [deque(maxlen=GRAPH_HISTORY) for _ in range(SENSORS)]
        self.rebuild_graphs()
        self.set_controls_state(False)
        self.set_selection_state(True)
        self.set_left_state(True)
        self.last_lat = self.last_lon = self.last_alt = None
        self.last_speed = 0.0
        self.last_distance_m = 0.0
        self.calibrated = False

    # ---------- Controls ----------
    def on_start(self):
        if not self.controls_enabled or self.project is None:
            messagebox.showwarning("Start", "Set Project Specifications first.")
            return
        # Require calibration before run
        if not getattr(self, 'calibrated', False):
            if not messagebox.askyesno("Calibration required", "Sensors are not calibrated for this run. Open calibration now?"):
                return
            else:
                self.open_calibrate()
                return
        self.running = True
        self.paused_event.clear()
        self.set_status_color("green")
        self.lbl_status.configure(text="Status: running")
        self.set_selection_state(False)
        if not self.video_writers: self.open_writers()
        if self.sensor_worker is None:
            self.sensor_worker = SensorWorker(self.ui_queue, self.paused_event, self.get_calibration_value,
                                              self.on_ten_m_tick, self.get_sensor_selection, self.set_latest_raw)
            self.sensor_worker.start()

    def on_pause(self):
        if not self.running: return
        self.paused_event.set()
        self.set_status_color("yellow")
        self.lbl_status.configure(text="Status: paused")

    def on_stop(self, silent=False):
        if not self.project: return
        self.on_export()
        self.running = False
        self.paused_event.set()
        self.set_status_color("red")
        self.lbl_status.configure(text="Status: stopped")
        self.table1_rows.clear()
        self.table2_rows.clear()
        self.measure_accum_km = 0.0
        self.window_buffer = []
        if self.sensor_worker:
            try:
                self.sensor_worker.stop()
                self.sensor_worker.join(timeout=1.0)
            except Exception: pass
            self.sensor_worker = None
        self.close_writers()
        self.set_selection_state(True)
        if not silent: messagebox.showinfo("Stopped", "Run stopped and data exported.")

    def on_export(self):
        if not self.project or self.run_dir is None:
            messagebox.showinfo("Export", "No configured project/run to export.")
            return
        self.paused_event.set()
        self.set_status_color("yellow")
        self.lbl_status.configure(text="Status: paused (export)")
        if not (PANDAS_OK and OPENPYXL_OK):
            messagebox.showerror("Export", "pandas and openpyxl are required for Excel export.\nInstall with: pip install pandas openpyxl")
            return
        # Flush partial bin if any
        if self.sensor_worker and self.sensor_worker.bin_buffers[0]:
            avgs = [float(np.nanmean(buf)) if len(buf) else float("nan") for buf in self.sensor_worker.bin_buffers]
            # sanitize avgs before use
            avgs = self.sanitize_avgs(avgs)
            last_speed = self.sensor_worker.last_speed
            lat, lon, alt = self.last_lat, self.last_lon, self.last_alt
            start_lat = self.sensor_worker.bin_start_lat
            start_lon = self.sensor_worker.bin_start_lon
            traveled_m = self.sensor_worker.distance_m - (self.sensor_worker.next_ten_m_edge - BIN_SIZE_METERS)
            partial_km = traveled_m / 1000
            dir_sign = 1 if self.project["direction"] == "Increasing" else -1
            start_chain = self.current_chainage_km
            end_chain = start_chain + dir_sign * partial_km
            ts = now_local_str()
            self.table2_rows.append([ts, self.project["nh"], f"{start_chain:.3f}", f"{end_chain:.3f}", self.project["direction"], self.project["lane"], f"{start_lat:.6f}", f"{start_lon:.6f}", f"{alt:.1f}", f"{last_speed:.1f}"])
            # For table1 (partial)
            sel = np.array(self.get_sensor_selection(), dtype=bool)
            masked = np.array(avgs, dtype=float)
            masked[~sel] = np.nan
            S1,S2,S3,S4,S5,S6 = masked[:6]
            left_rut   = np.nanmean(np.abs([S1, S2]))
            right_rut  = np.nanmean(np.abs([S5, S6]))
            avg_rut    = np.nanmean(np.abs([S1, S2, S5, S6]))
            left_iri   = np.nanmean(np.abs([S2, S3]))
            right_iri  = np.nanmean(np.abs([S4, S5]))
            avg_iri    = np.nanmean(np.abs([S2, S3, S4, S5]))
            avg_texture= np.nanmean(np.abs(masked))
            row1 = [
                ts, self.project["nh"], f"{start_chain:.3f}", f"{end_chain:.3f}", self.project["direction"], self.project["lane"],
                self._fmt_or_none(last_speed, 1),
                self._fmt_or_none(left_iri, 3), self._fmt_or_none(right_iri, 3), self._fmt_or_none(avg_iri, 3),
                self._fmt_or_none(left_rut, 1), self._fmt_or_none(right_rut, 1), self._fmt_or_none(avg_rut, 1),
                self._fmt_or_none(avg_texture, 3),
                f"{start_lat:.6f}", f"{start_lon:.6f}", f"{lat:.6f}", f"{lon:.6f}"
            ]
            self.table1_rows.append(row1)
            self.capture_images(chainage_label=self.format_chainage_label(start_chain), lat=lat, lon=lon, alt=alt)
            # update graphs with sanitized avgs (single new point)
            self.update_graphs(avgs)
            self.current_chainage_km = end_chain
            self.update_chainage_label(dir_sign * partial_km)

        if self.window_buffer and self.measure_accum_km > 0.0:
            dir_sign = 1 if self.project["direction"] == "Increasing" else -1
            end_chain = self.current_chainage_km if self.current_chainage_km is not None else self.project['init_chain']
            end_lat = self.window_buffer[-1]["lat"]
            end_lon = self.window_buffer[-1]["lon"]
            self.flush_measurement_window(final_ts=now_local_str(), end_lat=end_lat, end_lon=end_lon, dir_sign=dir_sign, end_chain=end_chain)
        t1_cols = [
            "Timestamp","NH Number","Start Chainage (Km)","End Chainage (Km)",
            "Direction","Lane","Speed (km/h)",
            "Left IRI (m)","Right IRI (m)","Average IRI (m)",
            "Left Rut (mm)","Right Rut (mm)","Average Rut (mm)",
            "Average Texture (m)",
            "Lat Start","Long Start","Lat End","Long End"
        ]
        t2_cols = [
            "Timestamp","NH Number","Start Chainage (Km)","End Chainage (Km)",
            "Direction","Lane","Lat","Long","Alt (m)","Speed (km/h)"
        ]
        df1 = pd.DataFrame(self.table1_rows, columns=t1_cols)
        df2 = pd.DataFrame(self.table2_rows, columns=t2_cols)
        survey = self.project['survey']
        lane = self.project['lane']
        startc = f"{self.project['init_chain']:.3f}"
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
    def sanitize_avgs(self, avgs):
        """
        Apply rules:
          - values < 0 -> 0
          - values > 1000 -> replace with average of last GRAPH_HISTORY points (if available), else clamp to 1000
        """
        out = []
        for i, v in enumerate(avgs):
            try:
                if not np.isfinite(v):
                    out.append(float('nan'))
                    continue
                if v < 0:
                    out.append(0.0)
                elif v > 1000:
                    # compute average of recent history for sensor i
                    hist = list(self.graph_data[i])
                    if len(hist) >= 1:
                        avg_prev = float(np.nanmean([x for x in hist if np.isfinite(x)])) if any(np.isfinite(x) for x in hist) else 1000.0
                        # if avg_prev NaN fallback
                        if not np.isfinite(avg_prev):
                            avg_prev = 1000.0
                        out.append(avg_prev)
                    else:
                        out.append(1000.0)
                else:
                    out.append(float(v))
            except Exception:
                out.append(float('nan'))
        return out

    def update_graphs(self, avgs):
        """
        Called once per chainage tick (i.e., one new point per sensor).
        Appends a single sanitized point per selected sensor and redraws plots.
        Only last GRAPH_HISTORY points are kept.
        """
        # sanitize incoming avgs
        avgs_s = self.sanitize_avgs(avgs)
        # append for each sensor in selected list
        for j, sens_idx in enumerate(self.selected_sensor_indices):
            try:
                val = avgs_s[sens_idx]
            except Exception:
                val = float('nan')
            # Append sanitized value to sensor graph data
            self.graph_data[sens_idx].append(val)
            xs = list(range(1, len(self.graph_data[sens_idx]) + 1))
            ys = list(self.graph_data[sens_idx])
            # update plotted line (lines list corresponds to selected sensors order)
            if j < len(self.lines):
                try:
                    self.lines[j].set_data(xs, ys)
                    self.axes[j].relim()
                    self.axes[j].autoscale_view()
                except Exception:
                    pass
        if hasattr(self, 'canvas_mpl'):
            self.canvas_mpl.draw_idle()

    def on_ten_m_tick(self, avgs, speed_kmph, end_gps, start_gps):
        """
        Called by SensorWorker every BIN_SIZE_METERS (10 m).
        We sanitize the avgs, store them to window_buffer and table rows, and call update_graphs once.
        """
        if self.project is None or not self.running: return
        dir_sign = 1 if self.project["direction"] == "Increasing" else -1

        # sanitize avgs before storing/plotting
        avgs_s = self.sanitize_avgs(avgs)

        start_chain = self.current_chainage_km
        end_chain = start_chain + dir_sign * BIN_SIZE_KM
        ts = now_local_str()
        lat, lon, alt = end_gps
        start_lat, start_lon, _ = start_gps
        self.last_lat, self.last_lon, self.last_alt = lat, lon, alt
        self.last_speed = speed_kmph
        self.last_distance_m += BIN_SIZE_METERS
        self.table2_rows.append([ts, self.project["nh"], f"{start_chain:.3f}", f"{end_chain:.3f}", self.project["direction"], self.project["lane"], f"{lat:.6f}", f"{lon:.6f}", f"{alt:.1f}", f"{speed_kmph:.1f}"])
        self.capture_images(chainage_label=self.format_chainage_label(start_chain), lat=lat, lon=lon, alt=alt)
        self.measure_accum_km += BIN_SIZE_KM
        # store sanitized avgs into window buffer for later aggregation
        self.window_buffer.append({"S": avgs_s, "speed": speed_kmph, "lat": lat, "lon": lon, "start_lat": start_lat, "start_lon": start_lon})
        # update graphs with sanitized avgs (one new data point)
        self.update_graphs(avgs_s)
        if self.measure_accum_km + 1e-9 >= self.project["meas_km"]:
            self.flush_measurement_window(final_ts=ts, end_lat=lat, end_lon=lon, dir_sign=dir_sign, end_chain=end_chain)
        self.current_chainage_km = end_chain
        self.update_chainage_label(dir_sign * BIN_SIZE_KM)

    def get_sensor_selection(self):
        return [v.get() for v in self.sensor_selected]

    def _fmt_or_none(self, x, ndigits):
        if x is None or (isinstance(x, float) and np.isnan(x)): return None
        return round(float(x), ndigits)

    def flush_measurement_window(self, final_ts, end_lat, end_lon, dir_sign, end_chain):
        """
        Aggregates the window_buffer (which contains sanitized values already) and writes metrics.
        Uses absolute values for rut/IRI/texture as required.
        """
        if not self.window_buffer: return
        S_matrix = np.array([w["S"] for w in self.window_buffer])  # (n,SENSORS)
        mean_S = np.nanmean(S_matrix, axis=0)
        sel = np.array(self.get_sensor_selection(), dtype=bool)
        masked = np.array(mean_S, dtype=float)
        masked[~sel] = np.nan
        S1,S2,S3,S4,S5,S6 = masked[:6]  # keep for 6, ignore others for metrics
        left_rut   = np.nanmean(np.abs([S1, S2]))
        right_rut  = np.nanmean(np.abs([S5, S6]))
        avg_rut    = np.nanmean(np.abs([S1, S2, S5, S6]))
        left_iri   = np.nanmean(np.abs([S2, S3]))
        right_iri  = np.nanmean(np.abs([S4, S5]))
        avg_iri    = np.nanmean(np.abs([S2, S3, S4, S5]))
        avg_texture= np.nanmean(np.abs(masked))  # use absolute values across selected sensors
        speed_mean = float(np.nanmean([w["speed"] for w in self.window_buffer]))
        start_c = end_chain - dir_sign * self.measure_accum_km
        end_c   = start_c + dir_sign * self.measure_accum_km
        lat_start = self.window_buffer[0]["start_lat"]
        lon_start = self.window_buffer[0]["start_lon"]
        row1 = [
            final_ts, self.project["nh"], f"{start_c:.3f}", f"{end_c:.3f}", self.project["direction"], self.project["lane"],
            self._fmt_or_none(speed_mean, 1),
            self._fmt_or_none(left_iri, 3), self._fmt_or_none(right_iri, 3), self._fmt_or_none(avg_iri, 3),
            self._fmt_or_none(left_rut, 1), self._fmt_or_none(right_rut, 1), self._fmt_or_none(avg_rut, 1),
            self._fmt_or_none(avg_texture, 3),
            f"{lat_start:.6f}", f"{lon_start:.6f}", f"{end_lat:.6f}", f"{end_lon:.6f}"
        ]
        self.table1_rows.append(row1)
        self.measure_accum_km = 0.0
        self.window_buffer = []

    def set_latest_raw(self, raw_vals):
        # Ensure a fixed-length list of floats / nans
        vals = []
        for i in range(SENSORS):
            try:
                v = float(raw_vals[i])
                vals.append(v)
            except Exception:
                vals.append(float('nan'))
        self.latest_raw = vals

    def capture_images(self, chainage_label, lat=None, lon=None, alt=None):
        if self.images_dir is None: return
        stamp = ts_for_path()
        current_time = time.time()
        for cam_id, worker in self.cameras.items():
            if not self.cam_selected.get(cam_id, tk.BooleanVar(value=True)).get(): continue
            record_frame = getattr(worker, "latest_record_frame", None)
            if record_frame is None or current_time - worker.latest_frame_ts > 0.5:
                logger.warning(f"Stale or missing frame for cam {cam_id}")
                ctx = self.get_overlay_context()
                preview = getattr(worker, "latest_preview_frame", None)
                if preview is None:
                    preview = make_testcard(PREVIEW_RES[0], PREVIEW_RES[1], "STALE FRAME")
                else:
                    preview = cv2.cvtColor(preview, cv2.COLOR_RGB2BGR)
                record_frame = draw_header_to_size(preview, ctx, RECORD_RES)
            name = CAM_NAMES.get(cam_id, f"CAM{cam_id}")
            folder = os.path.join(self.images_dir, name)
            os.makedirs(folder, exist_ok=True)
            fname = f"{name}_{chainage_label}_{stamp}.jpg".replace(" ", "_")
            path = os.path.join(folder, fname)
            try: cv2.imwrite(path, record_frame)
            except Exception as e:
                logger.error(f"Failed to write image {path}: {e}")

    # ---------- Telemetry/UI queue ----------
    def poll_cam_frames(self):
        start_time = time.time()
        frame_count = 0
        max_frames_per_cycle = 4  # Limit frames processed per cycle
        try:
            while frame_count < max_frames_per_cycle:
                cam_id, frame = self.cam_queue.get_nowait()
                frame_count += 1
                pil_img = Image.fromarray(frame)  # Frame already in RGB
                ctk_img = ctk.CTkImage(light_image=pil_img, dark_image=pil_img, size=PREVIEW_RES)
                try: i = CAM_IDS.index(cam_id)
                except ValueError: i = cam_id
                if i < len(self.cam_labels):
                    lbl = self.cam_labels[i]
                    lbl.configure(image=ctk_img)
                    lbl.image = ctk_img
            # Log queue size
            qsize = self.cam_queue.qsize()
            if qsize > MAX_QUEUE_SIZE / 2:
                logger.warning(f"Camera queue size: {qsize}")
        except queue.Empty:
            pass
        elapsed = time.time() - start_time
        logger.debug(f"Processed {frame_count} frames in {elapsed:.3f}s")
        self.after(int(1000 / PREVIEW_FPS), self.poll_cam_frames)

    def poll_ui_queue(self):
        try:
            while True:
                msg = self.ui_queue.get_nowait()
                if msg["type"] == "telemetry" and self.running:
                    self.last_speed = msg['speed_kmph']
                    self.last_distance_m = msg['distance_m']
                    self.last_lat = msg['lat']
                    self.last_lon = msg['lon']
                    self.last_alt = msg['alt']
                    self.var_speed.set(f"{msg['speed_kmph']:.1f} km/h")
                    self.var_dist.set(f"{msg['distance_m']:.1f} m")
                    self.var_lat.set(f"{msg['lat']:.6f}")
                    self.var_lon.set(f"{msg['lon']:.6f}")
                    self.var_alt.set(f"{msg['alt']:.1f} m")
                    self.speed_progress.set(min(msg['speed_kmph'] / 100.0, 1.0))
                elif msg["type"] == "ten_m":
                    # Already handled in callback
                    pass
        except queue.Empty:
            pass
        self.after(60, self.poll_ui_queue)

    # ---------- Writers (video) ----------
    def open_writers(self):
        self.close_writers()
        if self.stream_dir is None or self.project is None: return
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        for cam_id in CAM_IDS:
            if not self.cam_selected[cam_id].get(): continue
            name = CAM_NAMES.get(cam_id, f"CAM{cam_id}")
            out_path = os.path.join(self.stream_dir, f"{name}_{self.project['survey']}_{self.project['lane']}_{self.project['init_chain']:.3f}.mp4")
            vw = cv2.VideoWriter(out_path, fourcc, CAM_FPS, RECORD_RES)
            if not vw.isOpened():
                messagebox.showerror("Video", f"Failed to open writer for {name}")
            else:
                self.video_writers[cam_id] = vw

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
        base = self.project['init_chain']
        total_offset = (self.current_chainage_km - base)
        sign = '+' if total_offset >= 0 else '-'
        self.var_chain.set(f"{base:.0f} {sign} {abs(total_offset):.3f} Km")

    def format_chainage_label(self, start_chain):
        base = self.project['init_chain'] if self.project else 0.0
        offset = start_chain - base
        sign = '+' if offset >= 0 else '-'
        return f"{base:.0f}{sign}{abs(offset):.3f} Km"

    # ---------- Calibration ----------
    def open_calibrate(self):
        win = ctk.CTkToplevel(self)
        win.title("Calibrate and Select Sensors")
        win.geometry("1000x160")
        win.transient(self)
        win.grab_set()
        try: win.attributes("-topmost", True)
        except Exception: pass

        # Rolling buffers for smoothing / display
        avg_len = 150  # number of samples to average for offset capture (tuneable)
        if not hasattr(self, "_calib_buffers") or len(self._calib_buffers) != SENSORS:
            self._calib_buffers = [deque(maxlen=avg_len) for _ in range(SENSORS)]

        # Try to load previously saved offsets if available
        try:
            if self.run_dir:
                path = os.path.join(self.run_dir, "calib_offsets.npy")
                if os.path.exists(path):
                    self.calib_values = list(np.load(path))
                    self.calibrated = True
        except Exception as e:
            logger.debug(f"Could not load saved calibration: {e}")

        # Top: One-row display of live sensor values (raw and calibrated)
        top_frame = ctk.CTkFrame(win)
        top_frame.pack(fill="x", padx=8, pady=(8,4))

        lbl_title = ctk.CTkLabel(top_frame, text="Live RAW (avg)  |  Calibrated (raw - offset)", font=ctk.CTkFont(size=12, weight="bold"))
        lbl_title.grid(row=0, column=0, columnspan=SENSORS, pady=(0,8))

        self.raw_live_labels = []
        self.cal_live_labels = []
        for i in range(SENSORS):
            colf = ctk.CTkFrame(top_frame)
            colf.grid(row=1, column=i, padx=4, pady=0, sticky="n")
            # sensor id
            ctk.CTkLabel(colf, text=f"S{i+1}", font=ctk.CTkFont(size=10, weight="bold")).pack(anchor="center")
            # raw (avg)
            raw_lbl = ctk.CTkLabel(colf, text="--\n(--)", justify="center")
            raw_lbl.pack(anchor="center")
            self.raw_live_labels.append(raw_lbl)
            # calibrated
            cal_lbl = ctk.CTkLabel(colf, text="--", justify="center")
            cal_lbl.pack(anchor="center")
            self.cal_live_labels.append(cal_lbl)

        # Sensor selection row (single row of checkboxes)
        sens_select_frame = ctk.CTkFrame(win)
        sens_select_frame.pack(fill="x", padx=8, pady=(6,4))
        ctk.CTkLabel(sens_select_frame, text="Select Sensors to Export", font=ctk.CTkFont(size=12, weight="bold")).grid(row=0, column=0, sticky="w")
        subframe = ctk.CTkFrame(sens_select_frame)
        subframe.grid(row=1, column=0, sticky="w", pady=(4,4))
        self.sensor_checkboxes = []
        for i in range(SENSORS):
            chk = ctk.CTkCheckBox(subframe, text=f"S{i+1}", variable=self.sensor_selected[i])
            chk.grid(row=0, column=i, sticky="w", padx=2)
            self.sensor_checkboxes.append(chk)

        # Buttons
        btn_frame = ctk.CTkFrame(win)
        btn_frame.pack(fill="x", padx=8, pady=(4,8))
        def capture_offsets_avg():
            # Take average of rolling buffers (if empty, use latest raw)
            avgs = []
            for i, buf in enumerate(self._calib_buffers):
                if len(buf) >= 1:
                    avgs.append(float(np.nanmean(buf)))
                else:
                    v = self.latest_raw[i] if np.isfinite(self.latest_raw[i]) else 0.0
                    avgs.append(float(v))
            self.calib_values = avgs
            self.calibrated = True
            # persist offsets if we have run_dir
            try:
                if self.run_dir:
                    np.save(os.path.join(self.run_dir, "calib_offsets.npy"), np.array(self.calib_values))
            except Exception as e:
                logger.warning(f"Failed to save calibration offsets: {e}")
            messagebox.showinfo("Calibration", "Offsets captured (averaged). Calibrated values should now be near 0.")

        def capture_offsets_single():
            # immediate one-sample capture (keeps old behaviour for quick capture)
            self.calib_values = [self.latest_raw[i] if np.isfinite(self.latest_raw[i]) else 0.0 for i in range(SENSORS)]
            self.calibrated = True
            try:
                if self.run_dir:
                    np.save(os.path.join(self.run_dir, "calib_offsets.npy"), np.array(self.calib_values))
            except Exception as e:
                logger.warning(f"Failed to save calibration offsets: {e}")
            messagebox.showinfo("Calibration", "Offsets captured from current raw values. Calibrated values should now be near 0.")

        ctk.CTkButton(btn_frame, text="Capture Offsets (Avg)", command=capture_offsets_avg).pack(side="left", padx=8)
        ctk.CTkButton(btn_frame, text="Capture Offsets (Single)", command=capture_offsets_single).pack(side="left", padx=8)
        ctk.CTkButton(btn_frame, text="Close", command=win.destroy).pack(side="right", padx=8)

        # Tick: update live labels regularly using latest_raw and rolling buffers
        def tick():
            # Append latest raw to per-sensor buffers
            for i in range(SENSORS):
                v = self.latest_raw[i]
                if np.isfinite(v):
                    self._calib_buffers[i].append(float(v))
            # Update labels (raw avg and raw latest, and calibrated)
            for i in range(SENSORS):
                buf = self._calib_buffers[i]
                raw_avg = float(np.nanmean(buf)) if len(buf) else (self.latest_raw[i] if np.isfinite(self.latest_raw[i]) else float('nan'))
                raw_latest = self.latest_raw[i]
                raw_text = f"{raw_latest:.2f}\n({raw_avg:.2f})" if np.isfinite(raw_latest) else f"--\n(--)"
                self.raw_live_labels[i].configure(text=raw_text)
                # calibrated value = raw_latest - offset (if calibrated)
                if hasattr(self, 'calib_values') and len(self.calib_values) == SENSORS and np.isfinite(raw_latest):
                    try:
                        calv = raw_latest - float(self.calib_values[i])
                        self.cal_live_labels[i].configure(text=f"{calv:.2f}")
                    except Exception:
                        self.cal_live_labels[i].configure(text="--")
                else:
                    self.cal_live_labels[i].configure(text="--")

            # schedule next update while window exists
            if win.winfo_exists():
                win.after(100, tick)  # 100 ms update (10 Hz) — tune as desired

        # start live update
        tick()

    def get_calibration_value(self): return self.calib_values

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
    app = NSVApp()
    app.withdraw()  # Hide the main window initially

    # Create splash screen
    splash = ctk.CTkToplevel(app)
    splash.overrideredirect(True)
    splash.geometry("400x300")
    try:
        logo_img = ctk.CTkImage(light_image=Image.open(LOGO_PATH), size=(300, 200))
        ctk.CTkLabel(splash, image=logo_img, text="").pack(expand=True, fill="both")
    except Exception:
        ctk.CTkLabel(splash, text="Loading NSV Controller...", font=ctk.CTkFont(size=16)).pack(expand=True)

    # Center the splash screen
    splash.update_idletasks()
    width = splash.winfo_width()
    height = splash.winfo_height()
    x = (splash.winfo_screenwidth() // 2) - (width // 2)
    y = (splash.winfo_screenheight() // 2) - (height // 2)
    splash.geometry(f"{width}x{height}+{x}+{y}")

    # Schedule closing splash and showing main app
    def show_main():
        splash.destroy()
        app.deiconify()
        try:
            app.state('zoomed')
        except Exception:
            try:
                app.attributes('-zoomed', True)
            except Exception:
                pass

    splash.after(3000, show_main)

    app.mainloop()
