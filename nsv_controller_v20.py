import os, time, math, threading, queue, random
from datetime import datetime
import logging

import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox

import numpy as np
import cv2
from collections import deque
from MEDAQLib import MEDAQLib, ME_SENSOR, ERR_CODE  # Assuming the provided library is saved as medaqlib.py

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
HEADER_COLOR= (255, 0, 0)    # BGR: blue
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
#COM_PORTS = [f"COM{i}" for i in range(10, 10 + SENSORS)]  # COM10 to COM22
COM_PORTS = [f"COM{i}" for i in [13,12,7,13,10,12,10,10,10,10,10,10,10]]
EXPECTED_BLOCK_SIZE = 1  # Fetch one value per sensor per cycle for 100 Hz
TOF_HZ = 100


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
        x = pad_x + ci * col_w
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
        cap = cv2.VideoCapture(self.cam_url, backend_flag)
        if cap and cap.isOpened():
            # Set smaller buffer size
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 2)
        return cap

    def _open_cap(self):
        for backend in (cv2.CAP_FFMPEG, cv2.CAP_MSMF, cv2.CAP_DSHOW, cv2.CAP_ANY):
            cap = self._try_open(backend)
            if cap is not None:
                self.cap = cap
                logger.info(f"Camera {self.cam_id} opened with backend {backend}")
                return True
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
                    self.cap.release()
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
                if self.out_queue.qsize() < MAX_QUEUE_SIZE:
                    self.out_queue.put((self.cam_id, preview_rgb))
                    self.last_frame_time = start_time
                else:
                    logger.warning(f"Camera {self.cam_id} queue full, dropping frame")
            writer = self.get_writer(self.cam_id)
            if writer is not None:
                writer.write(record)
            elapsed = time.time() - start_time
            sleep_time = max(0, 1.0 / CAM_FPS - elapsed)
            time.sleep(sleep_time)
        if self.cap is not None:
            self.cap.release()
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
        self.next_ten_m_edge = 10.0  # BIN_SIZE_METERS
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
                self.ten_m_callback(avgs, self.last_speed, (lat, lon, alt), (start_lat, start_lon, alt))
                self.next_ten_m_edge += 10.0
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
        #self.state('zoomed')  # Maximize window on startup

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

        self.selected_sensor_indices = [i for i, v in enumerate(self.sensor_selected) if v.get()]
        self.graph_data = [deque(maxlen=600) for _ in range(SENSORS)]

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
        fig.tight_layout(pad=3.0)
        for j, sens_idx in enumerate(self.selected_sensor_indices):
            ax = fig.add_subplot(rows, cols, j+1, facecolor=BG_COLOR)
            ax.set_title(f"S{sens_idx+1}", fontsize=9, color="white")
            ax.set_xlabel("")
            ax.set_ylabel("Val", fontsize=8, color="white")
            ax.set_xticks([])
            ax.tick_params(axis='y', colors="white")
            for s in ax.spines.values(): s.set_color(SPINE_COLOR)
            ax.grid(True, color=GRID_COLOR, linewidth=0.6)
            self.axes.append(ax)
            self.lines.append(ax.plot([], [], NEON_GREEN)[0])

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
        lrp = f"{float(lrp):.3f}" if lrp != '--' else '--'
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
        self.graph_data = [deque(maxlen=600) for _ in range(SENSORS)]
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
        self.graph_data = [deque(maxlen=600) for _ in range(SENSORS)]
        self.rebuild_graphs()
        self.set_controls_state(False)
        self.set_selection_state(True)
        self.set_left_state(True)
        self.last_lat = self.last_lon = self.last_alt = None
        self.last_speed = 0.0
        self.last_distance_m = 0.0

    # ---------- Controls ----------
    def on_start(self):
        if not self.controls_enabled or self.project is None:
            messagebox.showwarning("Start", "Set Project Specifications first.")
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
            # Use absolute values so metrics are never negative
            left_rut   = np.nanmean(np.abs([S1, S2]))
            right_rut  = np.nanmean(np.abs([S5, S6]))
            avg_rut    = np.nanmean(np.abs([S1, S2, S5, S6]))
            left_iri   = np.nanmean(np.abs([S3]))
            right_iri  = np.nanmean(np.abs([S4]))
            avg_iri    = np.nanmean(np.abs([S3, S4]))
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
    def update_graphs(self, avgs):
        for j, sens_idx in enumerate(self.selected_sensor_indices):
            self.graph_data[sens_idx].append(avgs[sens_idx])
            xs = range(1, len(self.graph_data[sens_idx]) + 1)
            self.lines[j].set_data(list(xs), list(self.graph_data[sens_idx]))
            self.axes[j].relim()
            self.axes[j].autoscale_view()
        if hasattr(self, 'canvas_mpl'):
            self.canvas_mpl.draw_idle()

    def on_ten_m_tick(self, avgs, speed_kmph, end_gps, start_gps):
        if self.project is None or not self.running: return
        dir_sign = 1 if self.project["direction"] == "Increasing" else -1
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
        self.window_buffer.append({"S": avgs, "speed": speed_kmph, "lat": lat, "lon": lon, "start_lat": start_lat, "start_lon": start_lon})
        if self.measure_accum_km + 1e-9 >= self.project["meas_km"]:
            self.flush_measurement_window(final_ts=ts, end_lat=lat, end_lon=lon, dir_sign=dir_sign, end_chain=end_chain)
        self.current_chainage_km = end_chain
        self.update_chainage_label(dir_sign * BIN_SIZE_KM)
        self.update_graphs(avgs)

    def get_sensor_selection(self):
        return [v.get() for v in self.sensor_selected]

    def _fmt_or_none(self, x, ndigits):
        if x is None or (isinstance(x, float) and np.isnan(x)): return None
        return round(float(x), ndigits)

    def flush_measurement_window(self, final_ts, end_lat, end_lon, dir_sign, end_chain):
        if not self.window_buffer: return
        S_matrix = np.array([w["S"] for w in self.window_buffer])  # (n,SENSORS)
        mean_S = np.nanmean(S_matrix, axis=0)
        sel = np.array(self.get_sensor_selection(), dtype=bool)
        masked = np.array(mean_S, dtype=float)
        masked[~sel] = np.nan
        S1,S2,S3,S4,S5,S6 = masked[:6]  # keep for 6, ignore others for metrics
        # Use absolute values for rutting, IRI and texture (roughness) so output is non-negative
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
        self.latest_raw = list(raw_vals)

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
            except Exception: pass

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
        win.geometry("800x500")
        win.transient(self)
        win.grab_set()
        try: win.attributes("-topmost", True)
        except Exception: pass
        table = ctk.CTkFrame(win)
        table.pack(fill="x", padx=10, pady=8)
        ctk.CTkLabel(table, text="Live RAW sensor values (S1..S13)").grid(row=0, column=0, columnspan=7, pady=(0,6))
        self.raw_labels = []
        self.cal_labels = []
        for i in range(SENSORS):
            row = 1 if i < 7 else 4
            col = i % 7
            if i == 7:
                ctk.CTkLabel(table, text="").grid(row=3, column=0)  # spacer
            ctk.CTkLabel(table, text=f"S{i+1}").grid(row=row, column=col, padx=6)
            raw_lbl = ctk.CTkLabel(table, text="--")
            raw_lbl.grid(row=row+1, column=col, padx=6)
            self.raw_labels.append(raw_lbl)
            cal_lbl = ctk.CTkLabel(table, text="--")
            cal_lbl.grid(row=row+2, column=col, padx=6)
            self.cal_labels.append(cal_lbl)
        ctk.CTkLabel(table, text="Calibrated:").grid(row=2 if SENSORS > 0 else 0, column=0, sticky="w")

        def tick():
            for i in range(SENSORS):
                raw_val = self.latest_raw[i]
                cal_val = raw_val - self.calib_values[i] if np.isfinite(raw_val) else float('nan')
                self.raw_labels[i].configure(text=f"{raw_val:.2f}" if np.isfinite(raw_val) else "--")
                self.cal_labels[i].configure(text=f"{cal_val:.2f}" if np.isfinite(cal_val) else "--")
            if win.winfo_exists(): win.after(100, tick)

        tick()

        sens_select_frame = ctk.CTkFrame(win)
        sens_select_frame.pack(fill="x", padx=10, pady=8)
        ctk.CTkLabel(sens_select_frame, text="Select Sensors to Export", font=ctk.CTkFont(size=14, weight="bold")).pack(anchor="w", pady=(4,4))
        subframe = ctk.CTkFrame(sens_select_frame)
        subframe.pack(anchor="w")
        self.sensor_checkboxes = []
        for i in range(SENSORS):
            chk = ctk.CTkCheckBox(subframe, text=f"S{i+1}", variable=self.sensor_selected[i])
            chk.grid(row=i//3, column=i%3, sticky="w", padx=2, pady=2)
            self.sensor_checkboxes.append(chk)

        def capture_offsets():
            self.calib_values = [self.latest_raw[i] if np.isfinite(self.latest_raw[i]) else 0.0 for i in range(SENSORS)]
            messagebox.showinfo("Calibration", "Offsets captured from current raw values. Calibrated values should now be near 0.")

        ctk.CTkButton(win, text="Capture Offsets", command=capture_offsets).pack(pady=6)

        def apply_and_close():
            win.destroy()
            self.rebuild_graphs()

        ctk.CTkButton(win, text="Close", command=apply_and_close).pack(pady=12)

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
    except Exception as e:
        ctk.CTkLabel(splash, text="Loading NSV Controller...", font=ctk.CTkFont(size=16)).pack(expand=True)

    # Center the splash screen
    splash.update_idletasks()
    width = splash.winfo_width()
    height = splash.winfo_height()
    x = (splash.winfo_screenwidth() // 2) - (width // 2)
    y = (splash.winfo_screenheight() // 2) - (height // 2)
    splash.geometry(f"{width}x{height}+{x}+{y}")

    # Schedule closing splash and showing main app
    splash.after(3000, lambda: (splash.destroy(), app.deiconify()))

    app.mainloop()
