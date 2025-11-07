import os, time, math, threading, queue, random
from datetime import datetime
import logging

import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox

import numpy as np
import cv2
from collections import deque
from MEDAQLib import MEDAQLib, ME_SENSOR, ERR_CODE

# Serial communication for GPS
try:
    import serial
    SERIAL_OK = True
except ImportError:
    SERIAL_OK = False
    logging.warning("pyserial not installed. GPS functionality will not work. Install with: pip install pyserial")

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
    "rtsp://192.168.1.103:554/stream1",
    "rtsp://192.168.1.102:554/stream1",
    "rtsp://192.168.1.100:554/stream1",
    "rtsp://192.168.1.101:554/stream0?username=admin&password=E10ADC3949BA59ABBE56E057F20F883E"
]
PREVIEW_RES = (360, 202)
RECORD_RES  = (1280, 720)
CAM_FPS     = 25
PREVIEW_FPS = 15
TOF_HZ      = 100
BIN_SIZE_METERS = 10.0
BIN_SIZE_KM = 0.01
NEON_GREEN  = "#39ff14"
HEADER_COLOR= (255, 0, 0)
TEXT_COLOR  = (255, 255, 255)
FONT        = cv2.FONT_HERSHEY_SIMPLEX
BG_COLOR    = "#0b0f14"
GRID_COLOR  = "#263238"
SPINE_COLOR = "#607d8b"
HEADER_MAX_FRAC = 0.20
HEADER_LINES = 5
LOGO_PATH = "cropped-Roadworks-LogoR-768x576-1.jpeg"
MAX_QUEUE_SIZE = 5

# Constants for laser sensors
SENSORS = 6
COM_PORTS = [f"COM{i}" for i in [12,13,14,5,10,7]]
EXPECTED_BLOCK_SIZE = 100
TOF_HZ = 100

PLOT_MAX_POINTS = 10

# GPS Configuration
GPS_COM_PORT = "COM30"
GPS_BAUDRATE = 57600

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
    if frame is None or not isinstance(frame, (np.ndarray,)) or frame.size == 0:
        frame = np.zeros((h, w, 3), dtype=np.uint8)
    else:
        if hasattr(frame, "mode"):
            frame = cv2.cvtColor(np.array(frame), cv2.COLOR_RGBA2BGR if frame.mode == "RGBA" else cv2.COLOR_RGB2BGR)
        frame = cv2.resize(frame, (w, h))

    if h >= 1080:
        font_scale = 0.6
    elif h >= 720:
        font_scale = 0.5
    elif h >= 480:
        font_scale = 0.4
    else:
        font_scale = 0.3
    thickness = 1

    (_, th), base = cv2.getTextSize("Ag", FONT, font_scale, thickness)
    line_h = max(th + base, int(16 * font_scale))
    pad_x = max(10, int(10 * (h / 480)))
    pad_top = max(10, int(12 * (h / 480)))
    pad_bottom = max(5, int(8 * (h / 480)))

    needed_h = pad_top + HEADER_LINES * line_h + pad_bottom
    max_h = int(h * HEADER_MAX_FRAC)
    header_h = min(needed_h, max_h) if needed_h > 0 else max_h
    attempts = 0
    while needed_h > max_h and attempts < 5 and font_scale > 0.3:
        font_scale *= 0.85
        (_, th), base = cv2.getTextSize("Ag", FONT, font_scale, thickness)
        line_h = max(th + base, int(16 * font_scale))
        needed_h = pad_top + HEADER_LINES * line_h + pad_bottom
        attempts += 1
    header_h = min(needed_h, max_h)

    cv2.rectangle(frame, (0,0), (w, header_h), HEADER_COLOR, -1)

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
    cols = [col1[:5], col2, col3]

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
    R = 6371e3
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

# ---------------------------
# GPS Reader (Emlid Reach M2)
# ---------------------------
class GPSReader:
    """Reads NMEA sentences from Emlid Reach M2 GPS receiver over serial."""
    def __init__(self, port=GPS_COM_PORT, baudrate=GPS_BAUDRATE):
        self.port = port
        self.baudrate = baudrate
        self.serial = None
        self.lat = None
        self.lon = None
        self.alt = None
        self.speed_knots = None
        self.speed_kmph = None
        self.fix_quality = 0
        self.num_satellites = 0
        self.hdop = 99.9
        self.lock = threading.Lock()
        self.last_update = 0
        
    def connect(self):
        """Open serial connection to GPS receiver."""
        if not SERIAL_OK:
            logger.error("pyserial not installed. Cannot connect to GPS.")
            return False
        try:
            self.serial = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=1,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE
            )
            logger.info(f"GPS connected on {self.port} at {self.baudrate} baud")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to GPS on {self.port}: {e}")
            return False
    
    def disconnect(self):
        """Close serial connection."""
        if self.serial and self.serial.is_open:
            try:
                self.serial.close()
                logger.info("GPS disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting GPS: {e}")
    
    def parse_nmea_coordinate(self, coord_str, direction):
        """Parse NMEA coordinate format to decimal degrees."""
        try:
            if not coord_str or not direction:
                return None
            
            if direction in ['N', 'S']:
                degrees = float(coord_str[:2])
                minutes = float(coord_str[2:])
            else:
                degrees = float(coord_str[:3])
                minutes = float(coord_str[3:])
            
            decimal = degrees + (minutes / 60.0)
            
            if direction in ['S', 'W']:
                decimal = -decimal
            
            return decimal
        except Exception as e:
            logger.warning(f"Failed to parse coordinate {coord_str} {direction}: {e}")
            return None
    
    def parse_gga(self, sentence):
        """Parse GGA sentence for position, altitude, and fix quality."""
        try:
            parts = sentence.split(',')
            if len(parts) < 15:
                return
            
            lat_str = parts[2]
            lat_dir = parts[3]
            lon_str = parts[4]
            lon_dir = parts[5]
            
            with self.lock:
                self.lat = self.parse_nmea_coordinate(lat_str, lat_dir)
                self.lon = self.parse_nmea_coordinate(lon_str, lon_dir)
                self.fix_quality = int(parts[6]) if parts[6] else 0
                self.num_satellites = int(parts[7]) if parts[7] else 0
                self.hdop = float(parts[8]) if parts[8] else 99.9
                self.alt = float(parts[9]) if parts[9] else None
                self.last_update = time.time()
                
        except Exception as e:
            logger.warning(f"Failed to parse GGA sentence: {e}")
    
    def parse_rmc(self, sentence):
        """Parse RMC sentence for speed and position."""
        try:
            parts = sentence.split(',')
            if len(parts) < 10:
                return
            
            status = parts[2]
            if status != 'A':
                return
            
            lat_str = parts[3]
            lat_dir = parts[4]
            lon_str = parts[5]
            lon_dir = parts[6]
            speed_str = parts[7]
            
            with self.lock:
                self.lat = self.parse_nmea_coordinate(lat_str, lat_dir)
                self.lon = self.parse_nmea_coordinate(lon_str, lon_dir)
                self.speed_knots = float(speed_str) if speed_str else 0.0
                self.speed_kmph = self.speed_knots * 1.852
                self.last_update = time.time()
                
        except Exception as e:
            logger.warning(f"Failed to parse RMC sentence: {e}")
    
    def parse_vtg(self, sentence):
        """Parse VTG sentence for speed over ground."""
        try:
            parts = sentence.split(',')
            if len(parts) < 9:
                return
            
            speed_kmh_str = parts[7]
            
            with self.lock:
                if speed_kmh_str:
                    self.speed_kmph = float(speed_kmh_str)
                    self.speed_knots = self.speed_kmph / 1.852
                    self.last_update = time.time()
                
        except Exception as e:
            logger.warning(f"Failed to parse VTG sentence: {e}")
    
    def read_sentence(self):
        """Read one NMEA sentence from serial port."""
        if not self.serial or not self.serial.is_open:
            return None
        
        try:
            line = self.serial.readline().decode('ascii', errors='ignore').strip()
            return line
        except Exception as e:
            logger.warning(f"Error reading GPS sentence: {e}")
            return None
    
    def process_sentence(self, sentence):
        """Process a single NMEA sentence."""
        if not sentence or not sentence.startswith('$'):
            return
        
        if '*' in sentence:
            msg, checksum = sentence.rsplit('*', 1)
            calc_checksum = 0
            for char in msg[1:]:
                calc_checksum ^= ord(char)
            try:
                if int(checksum, 16) != calc_checksum:
                    logger.debug(f"Checksum mismatch in: {sentence}")
                    return
            except ValueError:
                return
        
        if 'GGA' in sentence:
            self.parse_gga(sentence)
        elif 'RMC' in sentence:
            self.parse_rmc(sentence)
        elif 'VTG' in sentence:
            self.parse_vtg(sentence)
    
    def get_position(self):
        """Get current GPS position and speed."""
        with self.lock:
            # Don't log warnings if serial is closed (during shutdown)
            if time.time() - self.last_update > 2.0:
                if self.serial and self.serial.is_open:
                    logger.warning("GPS data stale")
                return (None, None, None, 0.0)
            
            if self.fix_quality == 0:
                logger.warning("No GPS fix")
                return (None, None, None, 0.0)
            
            lat = self.lat if self.lat is not None else None
            lon = self.lon if self.lon is not None else None
            alt = self.alt if self.alt is not None else None
            speed = self.speed_kmph if self.speed_kmph is not None else 0.0
            
            return (lat, lon, alt, speed)
    
    def get_fix_info(self):
        """Get GPS fix quality information."""
        with self.lock:
            fix_types = {
                0: "No Fix",
                1: "GPS Fix",
                2: "DGPS Fix",
                4: "RTK Fixed",
                5: "RTK Float"
            }
            return {
                'fix_type': fix_types.get(self.fix_quality, "Unknown"),
                'satellites': self.num_satellites,
                'hdop': self.hdop
            }

# ---------------------------
# GPS Worker Thread
# ---------------------------
class GPSWorker(threading.Thread):
    """Background thread that continuously reads GPS data."""
    def __init__(self, gps_reader):
        super().__init__(daemon=True)
        self.gps_reader = gps_reader
        self.stop_event = threading.Event()
    
    def run(self):
        logger.info("GPS worker thread started")
        while not self.stop_event.is_set():
            try:
                sentence = self.gps_reader.read_sentence()
                if sentence:
                    self.gps_reader.process_sentence(sentence)
            except Exception as e:
                if not self.stop_event.is_set():
                    logger.error(f"GPS worker error: {e}")
            time.sleep(0.01)
        logger.info("GPS worker thread stopped")
    
    def stop(self):
        self.stop_event.set()

# ---------------------------
# DMI (Distance Measurement Instrument) Simulation
# ---------------------------
class DMISimulator:
    """Simulates a Distance Measurement Instrument (DMI) sensor."""
    def __init__(self, base_speed_kmh=30.0, speed_variation=5.0):
        self.base_speed_kmh = base_speed_kmh
        self.speed_variation = speed_variation
        self.total_distance_m = 0.0
        self.current_speed_kmh = base_speed_kmh
        self.last_update_time = time.time()
        self.lock = threading.Lock()
        
        self.speed_change_interval = 2.0
        self.last_speed_change = time.time()
        
    def update(self):
        """Update distance based on elapsed time and current speed."""
        current_time = time.time()
        
        with self.lock:
            dt = current_time - self.last_update_time
            self.last_update_time = current_time
            
            if current_time - self.last_speed_change >= self.speed_change_interval:
                speed_change = random.uniform(-2, 2)
                self.current_speed_kmh += speed_change
                self.current_speed_kmh = max(25.0, min(35.0, self.current_speed_kmh))
                self.last_speed_change = current_time
            
            speed_mps = self.current_speed_kmh / 3.6
            distance_delta = speed_mps * dt
            self.total_distance_m += distance_delta
    
    def get_speed_kmh(self):
        """Get current speed in km/h."""
        with self.lock:
            return self.current_speed_kmh
    
    def get_distance_m(self):
        """Get total distance traveled in meters."""
        with self.lock:
            return self.total_distance_m
    
    def reset(self):
        """Reset distance counter."""
        with self.lock:
            self.total_distance_m = 0.0
            self.last_update_time = time.time()

# ---------------------------
# DMI Worker Thread
# ---------------------------
class DMIWorker(threading.Thread):
    """Background thread that continuously updates DMI simulation."""
    def __init__(self, dmi_simulator):
        super().__init__(daemon=True)
        self.dmi_simulator = dmi_simulator
        self.stop_event = threading.Event()
    
    def run(self):
        logger.info("DMI worker thread started")
        while not self.stop_event.is_set():
            try:
                self.dmi_simulator.update()
            except Exception as e:
                if not self.stop_event.is_set():
                    logger.error(f"DMI worker error: {e}")
            time.sleep(0.01)
        logger.info("DMI worker thread stopped")
    
    def stop(self):
        self.stop_event.set()

def read_laser_values(sensors):
    """Read scaled values from Micro-Epsilon ILD1320 lasers."""
    values = [float('nan')] * len(sensors)
    for i, sensor in enumerate(sensors):
        if sensor is None:
            continue
        try:
            if sensor.DataAvail() >= EXPECTED_BLOCK_SIZE and sensor.GetLastError() == ERR_CODE.ERR_NOERROR:
                transferred_data = sensor.TransferData(EXPECTED_BLOCK_SIZE)
                if sensor.GetLastError() == ERR_CODE.ERR_NOERROR and transferred_data[2] > 0:
                    values[i] = transferred_data[1][-1]
        except Exception as e:
            logger.debug(f"Sensor {i+1} read exception: {str(e)}")
    return values

def initialize_lasers_with_timeout():
    """Initialize lasers with per-port timeout to prevent hanging."""
    sensors = [None] * SENSORS
    initialized_count = 0
    timeout_sec = 3.0

    ACTIVE_COM_PORTS = ["COM12", "COM13", "COM14", "COM5", "COM10", "COM7"]

    def try_init(port, idx, result_queue):
        sensor = None
        try:
            sensor = MEDAQLib.CreateSensorInstance(ME_SENSOR.SENSOR_ILD1320)
            if sensor.iSensor == 0:
                result_queue.put((idx, None, "Create failed"))
                return

            sensor.SetParameterString("IP_Interface", "RS232")
            sensor.SetParameterString("IP_Port", port)
            sensor.SetParameterInt("IP_AutomaticMode", 3)
            sensor.SetParameterInt("IP_EnableLogging", 0)
            sensor.SetParameterInt("IP_Timeout", int(timeout_sec * 1000))

            sensor.OpenSensor()
            err = sensor.GetLastError()
            if err != ERR_CODE.ERR_NOERROR:
                error_msg = f"Open failed: {err}"
                logger.warning(f"Sensor {idx+1} on {port}: {error_msg}")
                result_queue.put((idx, None, error_msg))
                sensor.CloseSensor()
                sensor.ReleaseSensorInstance()
                return

            result_queue.put((idx, sensor, "OK"))
            logger.info(f"Sensor {idx+1} initialized on {port}")

        except Exception as e:
            result_queue.put((idx, None, f"Exception: {str(e)}"))
            if sensor:
                try: sensor.CloseSensor()
                except: pass
                try: sensor.ReleaseSensorInstance()
                except: pass

    result_queue = queue.Queue()
    threads = []

    for i, port in enumerate(ACTIVE_COM_PORTS):
        if i >= SENSORS:
            break
        t = threading.Thread(target=try_init, args=(port, i, result_queue), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=timeout_sec + 1.0)

    while not result_queue.empty():
        idx, sensor, msg = result_queue.get()
        if sensor is not None:
            sensors[idx] = sensor
            initialized_count += 1
        else:
            logger.warning(f"Sensor {idx+1} failed: {msg}")

    logger.info(f"Laser initialization complete: {initialized_count}/{len(ACTIVE_COM_PORTS)} sensors connected")
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
        self.frame_interval = 1.0 / PREVIEW_FPS
        self.consecutive_fails = 0
        self.is_connected = False
        self.connection_attempts = 0
        self.max_connection_attempts = 1
        self.reconnect_interval = 5.0
        self.last_reconnect_attempt = 0

    def _try_open(self, backend_flag):
        """Try to open camera with specific backend, with timeout."""
        try:
            if backend_flag == cv2.CAP_FFMPEG:
                # inside _try_open when selecting FFMPEG backend
                os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
                    "rtsp_transport;tcp|"
                    "fflags;nobuffer|"
                    "probesize;32768|"
                    "analyzeduration;100000|"
                    "stimeout;5000000"   # 5,000,000 microseconds = 5 seconds
)
            
            cap = cv2.VideoCapture(self.cam_url, backend_flag)
            cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 2000)   # open timeout (ms)
            cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 2000)   # read timeout (ms)

            if cap and cap.isOpened():
                cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
                ret, frame = cap.read()
                if ret and frame is not None:
                    return cap
                else:
                    cap.release()
                    return None
            return None
        except Exception as e:
            logger.error(f"Camera {self.cam_id} backend {backend_flag} error: {e}")
            return None

    def _open_cap(self):
        """Try to open camera with various backends."""
        if self.connection_attempts >= self.max_connection_attempts:
            current_time = time.time()
            if current_time - self.last_reconnect_attempt < self.reconnect_interval:
                return False
            self.connection_attempts = 0
            self.last_reconnect_attempt = current_time
        
        self.connection_attempts += 1
        logger.info(f"Camera {self.cam_id} connection attempt {self.connection_attempts}/{self.max_connection_attempts}")
        
        for backend in (cv2.CAP_FFMPEG, cv2.CAP_MSMF, cv2.CAP_ANY):
            cap = self._try_open(backend)
            if cap is not None:
                self.cap = cap
                self.is_connected = True
                self.consecutive_fails = 0
                self.connection_attempts = 0
                logger.info(f"Camera {self.cam_id} ({CAM_NAMES.get(self.cam_id)}) connected with backend {backend}")
                return True
        
        logger.warning(f"Camera {self.cam_id} ({CAM_NAMES.get(self.cam_id)}) failed to connect")
        self.is_connected = False
        return False

    def run(self):
        self._open_cap()
        
        while not self.stop_event.is_set():
            if self.cap is None or not self.is_connected:
                ctx = self.get_overlay_ctx()
                testcard = make_testcard(self.record_size[0], self.record_size[1], 
                                        f"{CAM_NAMES.get(self.cam_id, f'CAM {self.cam_id}')} DISCONNECTED")
                testcard_overlay = draw_header_to_size(testcard, ctx, self.record_size)
                preview = cv2.resize(testcard_overlay, self.preview_size)
                preview_rgb = cv2.cvtColor(preview, cv2.COLOR_BGR2RGB)
                
                self.latest_preview_frame = preview_rgb
                self.latest_record_frame = testcard_overlay
                self.latest_frame_ts = time.time()
                
                if self.out_queue.qsize() < MAX_QUEUE_SIZE:
                    self.out_queue.put((self.cam_id, preview_rgb))
                
                time.sleep(1)
                self._open_cap()
                continue
                
            start_time = time.time()
            # replace ok, frame = self.cap.read() with non-blocking style
            start = time.time()
            frame = None
            while time.time() - start < 2.0:   # 2 second custom timeout
                grabbed = self.cap.grab()
                if not grabbed:
                    time.sleep(0.05)
                    continue
                ret, frame = self.cap.retrieve()
                if ret and frame is not None and frame.size:
                    ok = True
                    break
            else:
                ok = False

            
            if not ok or frame is None or frame.size == 0:
                self.consecutive_fails += 1
                logger.warning(f"Camera {self.cam_id} read failed ({self.consecutive_fails})")
                
                if self.consecutive_fails > 5:
                    logger.error(f"Camera {self.cam_id} too many failures, reconnecting...")
                    self.cap.release()
                    self.cap = None
                    self.is_connected = False
                    self.consecutive_fails = 0
                    time.sleep(0.5)
                    continue
                else:
                    time.sleep(0.1)
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
# Sensor Worker
# ---------------------------
class SensorWorker(threading.Thread):
    def __init__(self, ui_queue, paused_event, calib_getter, ten_m_callback,
                 sensor_selected_getter, raw_setter, gps_reader, dmi_simulator,
                 sensors=None):
        super().__init__(daemon=True)
        self.ui_queue = ui_queue
        self.paused_event = paused_event
        self.calib_getter = calib_getter
        self.ten_m_callback = ten_m_callback
        self.sensor_selected_getter = sensor_selected_getter
        self.raw_setter = raw_setter
        self.gps_reader = gps_reader
        self.dmi_simulator = dmi_simulator
        self.stop_event = threading.Event()
        self.prev_distance_m = 0.0
        self.bin_start_lat = None
        self.bin_start_lon = None

        if sensors is not None:
            self.sensors = sensors
            self._owns_sensors = False
        else:
            self.sensors = initialize_lasers_with_timeout()
            self._owns_sensors = True

        # FIXED: Track if first reading has been processed
        
        self.reset()

    def reset(self):
        self.last_t = time.time()
        self.next_ten_m_edge = 10.0
        self.bin_buffers = [[] for _ in range(SENSORS)]
        self.bin_start_lat = None
        self.bin_start_lon = None
        self.prev_distance_m = 0.0

    def run(self):
        logger.info("Sensor worker thread started")
        while not self.stop_event.is_set():
            t = time.time()
            if self.paused_event.is_set():
                time.sleep(0.01)
                continue

            lat, lon, alt, _ = self.gps_reader.get_position()
            current_speed_kmh = self.dmi_simulator.get_speed_kmh()
            current_distance_m = self.dmi_simulator.get_distance_m()
            
            if lat is None or lon is None:
                logger.warning("No GPS fix available - position data unavailable")
                lat = self.bin_start_lat if self.bin_start_lat is not None else 0.0
                lon = self.bin_start_lon if self.bin_start_lon is not None else 0.0
                alt = 0.0
            
            raw = read_laser_values(self.sensors)
            try: self.raw_setter(raw)
            except Exception: pass

            calib_vals = self.calib_getter()

            vals = [raw[i] - calib_vals[i] if np.isfinite(raw[i]) else float('nan') for i in range(SENSORS)]
            
            # Add calibrated values to buffers
            for i, v in enumerate(vals):
                self.bin_buffers[i].append(v)

            if self.bin_start_lat is None:
                self.bin_start_lat = lat
                self.bin_start_lon = lon

            self.ui_queue.put({
                "type": "telemetry",
                "speed_kmph": current_speed_kmh,
                "distance_m": current_distance_m,
                "lat": lat, "lon": lon, "alt": alt,
            })

            if current_distance_m >= self.next_ten_m_edge:
                # FIXED: These averages now include the properly calibrated first data point (which will be ~0.0)
                avgs = [float(np.nanmean(buf)) if len(buf) else float("nan") for buf in self.bin_buffers]
                self.bin_buffers = [[] for _ in range(SENSORS)]
                start_lat = self.bin_start_lat
                start_lon = self.bin_start_lon
                self.ui_queue.put({
                    "type": "ten_m",
                    "avg_s": avgs,
                    "speed_kmph": current_speed_kmh,
                    "lat": lat, "lon": lon, "alt": alt,
                    "start_lat": start_lat, "start_lon": start_lon
                })
                self.ten_m_callback(avgs, current_speed_kmh, (lat, lon, alt), (start_lat, start_lon, alt))
                self.next_ten_m_edge += 10.0
                self.bin_start_lat = lat
                self.bin_start_lon = lon

            time.sleep(max(0.0, 1.0/TOF_HZ - (time.time()-t)))
            logger.info("Sensor worker thread stopped")

    def stop(self):
        self.stop_event.set()
        if getattr(self, "_owns_sensors", False):
            cleanup_lasers(self.sensors)

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

        # FIXED: Initialize devices in proper order: Lasers -> GPS -> DMI -> Cameras
        logger.info("=" * 60)
        logger.info("INITIALIZING DEVICES")
        logger.info("=" * 60)
        
        # 1. Initialize Lasers FIRST
        logger.info("1/4: Initializing Laser Sensors...")
        try:
            self.sensors = initialize_lasers_with_timeout()
            connected_lasers = sum(1 for s in self.sensors if s is not None)
            logger.info(f"✓ Lasers initialized: {connected_lasers}/{SENSORS} sensors connected")
        except Exception as e:
            logger.error(f"✗ Laser initialization failed: {e}")
            self.sensors = [None] * SENSORS
        
        # 2. Initialize GPS
        logger.info("2/4: Initializing GPS...")
        self.gps_reader = GPSReader(GPS_COM_PORT, GPS_BAUDRATE)
        self.gps_worker = None
        if self.gps_reader.connect():
            self.gps_worker = GPSWorker(self.gps_reader)
            self.gps_worker.start()
            logger.info("✓ GPS initialized and worker started")
        else:
            logger.error("✗ GPS initialization failed")
        
        # 3. Initialize DMI Simulator
        logger.info("3/4: Initializing DMI Simulator...")
        self.dmi_simulator = DMISimulator(base_speed_kmh=30.0, speed_variation=5.0)
        self.dmi_worker = DMIWorker(self.dmi_simulator)
        self.dmi_worker.start()
        logger.info("✓ DMI simulator initialized")
        
        logger.info("=" * 60)

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

        self.camera_status = {cam_id: False for cam_id in CAM_IDS}
        self.laser_status = [False] * SENSORS
        self.gps_status = False
        
        self.selected_sensor_indices = [i for i, v in enumerate(self.sensor_selected) if v.get()]
        self.graph_data = [deque(maxlen=PLOT_MAX_POINTS) for _ in range(SENSORS)]

        self.build_menu()
        self.build_layout()

        self.left.configure(width=LEFT_WIDTH)
        self.left.grid_propagate(False)

        # FIXED: Update initial status indicators based on actual initialization
        self.update_initial_device_status()

        self.set_controls_state(False)
        self.set_selection_state(True)
        
        # 4. Initialize Cameras LAST
        logger.info("4/4: Initializing Cameras...")
        for idx in CAM_IDS: 
            self.start_camera(idx)
        logger.info("✓ Camera workers started")
        logger.info("=" * 60)
        
        self.after(int(1000 / PREVIEW_FPS), self.poll_cam_frames)
        self.after(60, self.poll_ui_queue)
                # Start device status monitoring (runs continuously every 2 seconds)
        self.after(2000, self.update_all_device_status)
        logger.info("Device status monitoring started")

    def update_initial_device_status(self):
        """FIXED: Update device status indicators based on actual initialization."""
        # Update laser status
        connected_lasers = sum(1 for s in self.sensors if s is not None)
        if connected_lasers > 0:
            self.laser_status_light.configure(text_color="#43a047")  # green
            self.laser_status_text.configure(text=f"{connected_lasers}/{SENSORS} Initialized")
        else:
            self.laser_status_light.configure(text_color="#d32f2f")  # red
            self.laser_status_text.configure(text="No lasers detected")
        
        # Update GPS status
        if self.gps_worker and self.gps_worker.is_alive():
            self.gps_status_light.configure(text_color="#fbc02d")  # yellow (connected but waiting for fix)
            self.gps_status_text.configure(text="Waiting for fix")
        else:
            self.gps_status_light.configure(text_color="#d32f2f")  # red
            self.gps_status_text.configure(text="Disconnected")
        
        # Cameras will update asynchronously as they connect
        self.cam_status_light.configure(text_color="#fbc02d")  # yellow
        self.cam_status_text.configure(text="Connecting...")

    # ---------- Menu ----------
    def build_menu(self):
        menubar = tk.Menu(self)
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="New Project", command=self.new_project)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.on_close)
        menubar.add_cascade(label="File", menu=file_menu)
        sensors_menu = tk.Menu(menubar, tearoff=0)
        sensors_menu.add_command(label="Select Sensors to Export", command=self.open_sensor_selection)
        menubar.add_cascade(label="Sensors", menu=sensors_menu)
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
        
        # Device Status Section
        ctk.CTkLabel(f, text="Device Status", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, sticky="w", **pad)
        
        status_frame = ctk.CTkFrame(f)
        status_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=4)
        status_frame.grid_columnconfigure(1, weight=1)
        
        # GPS Status
        ctk.CTkLabel(status_frame, text="GPS:").grid(row=0, column=0, sticky="w", padx=(5,10))
        self.gps_status_light = ctk.CTkLabel(status_frame, text="●", font=ctk.CTkFont(size=20), text_color="#d32f2f")
        self.gps_status_light.grid(row=0, column=1, sticky="w")
        self.gps_status_text = ctk.CTkLabel(status_frame, text="Disconnected", font=ctk.CTkFont(size=10))
        self.gps_status_text.grid(row=0, column=2, sticky="w", padx=(5,5))
        
        # Camera Status (consolidated)
        ctk.CTkLabel(status_frame, text="Cameras:").grid(row=1, column=0, sticky="w", padx=(5,10))
        self.cam_status_light = ctk.CTkLabel(status_frame, text="●", font=ctk.CTkFont(size=20), text_color="#d32f2f")
        self.cam_status_light.grid(row=1, column=1, sticky="w")
        self.cam_status_text = ctk.CTkLabel(status_frame, text="0/4 Connected", font=ctk.CTkFont(size=10))
        self.cam_status_text.grid(row=1, column=2, sticky="w", padx=(5,5))
        
        # Laser Sensors Status (consolidated)
        ctk.CTkLabel(status_frame, text="Lasers:").grid(row=2, column=0, sticky="w", padx=(5,10))
        self.laser_status_light = ctk.CTkLabel(status_frame, text="●", font=ctk.CTkFont(size=20), text_color="#d32f2f")
        self.laser_status_light.grid(row=2, column=1, sticky="w")
        self.laser_status_text = ctk.CTkLabel(status_frame, text="Not initialized", font=ctk.CTkFont(size=10))
        self.laser_status_text.grid(row=2, column=2, sticky="w", padx=(5,5))
        
        # Telemetry Section
        ctk.CTkLabel(f, text="Telemetry", font=ctk.CTkFont(size=14, weight="bold")).grid(row=2, column=0, sticky="w", **pad)
        
        chip_frame = ctk.CTkFrame(f, fg_color="transparent")
        chip_frame.grid(row=3, column=0, sticky="w", **pad)
        self.chip = ctk.CTkLabel(chip_frame, text="  ", width=16, height=16, corner_radius=8)
        self.chip.grid(row=0, column=0, padx=(0,6))
        self.set_status_color("red")
        self.lbl_status = ctk.CTkLabel(chip_frame, text="Status: stopped")
        self.lbl_status.grid(row=0, column=1)

        grid = ctk.CTkFrame(f)
        grid.grid(row=4, column=0, sticky="ew", padx=10, pady=(4,4))
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

        ctk.CTkLabel(f, text="Speed Progress:").grid(row=5, column=0, sticky="w", **pad)
        self.speed_progress = ctk.CTkProgressBar(f, mode='determinate', width=200)
        self.speed_progress.grid(row=6, column=0, sticky="ew", padx=10, pady=4)
        self.speed_progress.set(0)

        self.btn_toggle_dir = ctk.CTkButton(f, text="Toggle Direction", command=self.toggle_direction)
        self.btn_toggle_dir.grid(row=7, column=0, sticky="ew", padx=10, pady=4)

        btns = ctk.CTkFrame(f)
        btns.grid(row=8, column=0, sticky="ew", padx=10, pady=(8,6))
        btns.grid_rowconfigure((0,1), weight=1)
        btns.grid_columnconfigure((0,1,2), weight=1)

        # Initialize buttons - merged Pause and Stop
        self.btn_start = ctk.CTkButton(btns, text="Start", command=self.on_start)
        self.btn_stop = ctk.CTkButton(btns, text="Stop", command=self.on_stop)
        self.btn_export = ctk.CTkButton(btns, text="Export", command=self.on_export)
        self.btn_reset = ctk.CTkButton(btns, text="Reset", command=self.on_reset)

        # Arrange buttons in two rows
        self.btn_start.grid(row=0, column=0, padx=4, pady=4, sticky="ew")
        self.btn_stop.grid(row=0, column=1, padx=4, pady=4, sticky="ew")
        self.btn_export.grid(row=0, column=2, padx=4, pady=4, sticky="ew")
        self.btn_reset.grid(row=1, column=0, columnspan=3, padx=4, pady=4, sticky="ew")

    def toggle_direction(self):
        if not self.running:
            return
        current = self.project['direction']
        new_dir = "Decreasing" if current == "Increasing" else "Increasing"
        self.project['direction'] = new_dir
        self.direction_var.set(new_dir)
        self.on_direction_select(new_dir)
        messagebox.showinfo("Direction Toggled", f"Direction changed to {new_dir}. Chainage will adjust accordingly.")

    # ---------- Device Status Updates ----------
    def update_gps_status(self):
        """Update GPS status indicator."""
        lat, lon, alt, speed = self.gps_reader.get_position()
        
        if lat is not None and lon is not None:
            self.gps_status = True
            self.gps_status_light.configure(text_color="#43a047")  # Green
            self.gps_status_text.configure(text="Connected")
        else:
            self.gps_status = False
            self.gps_status_light.configure(text_color="#d32f2f")  # Red
            self.gps_status_text.configure(text="No Fix")
    
    def update_camera_status(self):
        """Update camera status indicators."""
        connected_count = 0
        for cam_id, worker in self.cameras.items():
            if worker and worker.is_alive():
                # Check if getting frames
                if worker.latest_frame_ts > 0 and (time.time() - worker.latest_frame_ts) < 2.0:
                    self.camera_status[cam_id] = True
                    connected_count += 1
                else:
                    self.camera_status[cam_id] = False
            else:
                self.camera_status[cam_id] = False
        
        # Update consolidated status
        if connected_count == len(CAM_IDS):
            self.cam_status_light.configure(text_color="#43a047")  # Green
        elif connected_count > 0:
            self.cam_status_light.configure(text_color="#fbc02d")  # Yellow
        else:
            self.cam_status_light.configure(text_color="#d32f2f")  # Red
        
        self.cam_status_text.configure(text=f"{connected_count}/{len(CAM_IDS)} Connected")
    
    def update_laser_status(self):
        """Update laser sensor status indicators."""
        # FIXED: Check self.sensors (persistent) not just sensor_worker (temporary)
        if hasattr(self, 'sensors') and self.sensors:
            connected_count = sum(1 for s in self.sensors if s is not None)
            
            # If sensor_worker is running, also check for valid data
            if self.sensor_worker and self.sensor_worker.is_alive():
                valid_data_count = sum(1 for val in self.latest_raw if np.isfinite(val))
                
                # Build list of active sensors
                active_sensors = [i+1 for i in range(SENSORS) if np.isfinite(self.latest_raw[i])]
                
                if valid_data_count > 0:
                    if valid_data_count >= SENSORS * 0.8:  # 80% or more
                        self.laser_status_light.configure(text_color="#43a047")  # Green
                    else:
                        self.laser_status_light.configure(text_color="#fbc02d")  # Yellow
                    
                    # Show which sensors are active
                    if valid_data_count <= 5:
                        sensor_list = ", ".join([f"S{s}" for s in active_sensors])
                        self.laser_status_text.configure(text=f"{valid_data_count}/{SENSORS} Active: {sensor_list}")
                    else:
                        self.laser_status_text.configure(text=f"{valid_data_count}/{SENSORS} Active")
                else:
                    self.laser_status_light.configure(text_color="#d32f2f")  # Red
                    self.laser_status_text.configure(text="No active sensors")
            else:
                # Sensors initialized but worker not running yet
                if connected_count > 0:
                    self.laser_status_light.configure(text_color="#43a047")  # Green
                    self.laser_status_text.configure(text=f"{connected_count}/{SENSORS} Initialized")
                else:
                    self.laser_status_light.configure(text_color="#d32f2f")  # Red
                    self.laser_status_text.configure(text="No sensors connected")
        else:
            self.laser_status_light.configure(text_color="#d32f2f")  # Red
            self.laser_status_text.configure(text="Not initialized")
    
    def update_all_device_status(self):
        """Update all device status indicators."""
        try:
            self.update_gps_status()
            self.update_camera_status()
            self.update_laser_status()
        except Exception as e:
            logger.error(f"Error updating device status: {e}")
        
        # Schedule next update
        self.after(2000, self.update_all_device_status)  # Update every 2 seconds

    # ---------- Enable/Disable controls ----------
    def set_controls_state(self, enabled: bool):
        state = "normal" if enabled else "disabled"
        for btn in [self.btn_start, self.btn_export, self.btn_stop, self.btn_reset, self.btn_toggle_dir]:
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

        # === CALIBRATION: Capture current laser values as baseline ===
        raw = read_laser_values(self.sensors)
        self.calib_values = []
        calibrated_sensors = []
        uncalibrated = []

        for i in range(SENSORS):
            if np.isfinite(raw[i]):
                self.calib_values.append(raw[i])
                calibrated_sensors.append(i + 1)
            else:
                self.calib_values.append(0.0)
                uncalibrated.append(i + 1)

        logger.info("✓ Calibration captured at project setup")
        logger.info(f"   Calibrated sensors: {calibrated_sensors}")
        logger.info(f"   Baseline offsets: {[f'{v:.2f}' for v in self.calib_values]}")

        # === Build project paths ===
        base = filedialog.askdirectory(title="Choose base folder for project")
        if not base: return
        self.build_project_paths(base, survey, direction, lane)

        # === Set project dict ===
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

        # === UI Updates ===
        self.lbl_proj_status.configure(text=f"Specs set → {survey} | {direction} {lane}\nRun folder: {self.run_dir}")
        self.current_chainage_km = init_chain
        self.update_chainage_label(0.0)
        self.table1_rows.clear()
        self.table2_rows.clear()
        self.measure_accum_km = 0.0
        self.window_buffer = []
        self.graph_data = [deque(maxlen=PLOT_MAX_POINTS) for _ in range(SENSORS)]
        self.rebuild_graphs()
        self.set_left_state(False)
        self.set_controls_state(True)
        self.set_selection_state(False)
        self.close_writers()

        # === Show calibration result ===
        msg = f"Project Configured & Sensors Calibrated\n\n"
        msg += f"Calibrated: {len(calibrated_sensors)}/{SENSORS} sensors\n"
        if calibrated_sensors:
            msg += f"Sensors: {', '.join([f'S{s}' for s in calibrated_sensors])}\n"
        if uncalibrated:
            msg += f"Uncalibrated: {', '.join([f'S{s}' for s in uncalibrated])}\n"
        msg += f"\nAll future measurements will be relative to current position."
        messagebox.showinfo("Project Ready", msg)

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
        # Stop without exporting
        if self.running:
            self.running = False
            self.paused_event.set()
            if self.sensor_worker:
                try:
                    self.sensor_worker.stop()
                    self.sensor_worker.join(timeout=1.0)
                except Exception: pass
                self.sensor_worker = None
            self.close_writers()
        
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
        self.graph_data = [deque(maxlen=PLOT_MAX_POINTS) for _ in range(SENSORS)]  # CHANGED
        self.rebuild_graphs()
        self.set_controls_state(False)
        self.set_selection_state(True)
        self.set_left_state(True)
        self.last_lat = self.last_lon = self.last_alt = None
        self.last_speed = 0.0
        self.last_distance_m = 0.0
        self.table1_rows.clear()
        self.table2_rows.clear()
        self.measure_accum_km = 0.0
        self.window_buffer = []
        # === RESET CALIBRATION ===
        self.calib_values = [0.0] * SENSORS
        logger.info("System reset - calibration values cleared")

    # ---------- Controls ----------
    def on_start(self):
        if not self.controls_enabled or self.project is None:
            messagebox.showwarning("Start", "Set Project Specifications first.")
            return

        # Optional: Warn if no GPS fix, but do NOT block
        lat, lon, alt, speed = self.gps_reader.get_position()
        if lat is None or lon is None:
            fix_info = self.gps_reader.get_fix_info()
            logger.warning(f"GPS Fix Status: {fix_info['fix_type']}, Satellites: {fix_info['satellites']}")
            # Proceed anyway — DMI provides distance

        self.running = True
        self.paused_event.clear()
        self.set_status_color("green")
        self.lbl_status.configure(text="Status: running")
        self.set_selection_state(False)

        # Start sensor worker
        if self.sensor_worker is None:
            self.dmi_simulator.reset()
            logger.info("DMI distance counter reset to zero")

            self.sensor_worker = SensorWorker(
                self.ui_queue, self.paused_event, self.get_calibration_value,
                self.on_ten_m_tick, self.get_sensor_selection, self.set_latest_raw,
                self.gps_reader, self.dmi_simulator, sensors=self.sensors
            )
            self.sensor_worker.start()

        # Open video writers
        if not self.video_writers:
            self.open_writers()

        messagebox.showinfo("Started", "Data collection started.\nSensors are calibrated from project setup.")

    def on_stop(self, silent=False):
        """Stop data collection and pause sensors without exporting."""
        if not self.running: 
            return
        
        self.running = False
        self.paused_event.set()
        self.set_status_color("red")
        self.lbl_status.configure(text="Status: stopped")
        
        # Stop sensor worker
        if self.sensor_worker:
            try:
                self.sensor_worker.stop()
                self.sensor_worker.join(timeout=1.0)
            except Exception as e:
                logger.error(f"Error stopping sensor worker: {e}")
            self.sensor_worker = None
        
        # Close video writers
        self.close_writers()
        self.set_selection_state(True)
        
        if not silent: 
            messagebox.showinfo("Stopped", "Data collection stopped.\nUse Export button to save data.")

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
            left_iri   = (((np.nanmean(np.abs([S3])))/630)^(0.893))*100
            right_iri  = (((np.nanmean(np.abs([S4])))/630)^(0.893))*100
            avg_iri    = np.nanmean(np.abs([S3, S4]))
            avg_texture= np.nanmean(np.abs(masked))
            row1 = [
                ts, self.project["nh"], f"{start_chain:.3f}", f"{end_chain:.3f}", self.project["direction"], self.project["lane"],
                self._fmt_or_none(last_speed, 0),
                self._fmt_or_none(left_iri, 2), self._fmt_or_none(right_iri, 2), self._fmt_or_none(avg_iri, 2),
                self._fmt_or_none(left_rut, 2), self._fmt_or_none(right_rut, 2), self._fmt_or_none(avg_rut, 2),
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
            "Left IRI (m/km)","Right IRI (m/km)","Average IRI (m/km)",
            "Left Rut (mm)","Right Rut (mm)","Average Rut (mm)",
            "Average Texture (mm)",
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
        rut_cols = [c for c in t1_cols if c not in ("Left IRI (m/km)","Right IRI (m/km)","Average IRI (m/km)","Average Texture (mm)")]
        iri_cols = [c for c in t1_cols if c not in ("Left Rut (mm)","Right Rut (mm)","Average Rut (mm)","Average Texture (mm)")]
        tex_cols = [c for c in t1_cols if c not in ("Left IRI (m/km)","Right IRI (m/km)","Average IRI (m/km)","Left Rut (mm)","Right Rut (mm)","Average Rut (mm)")]
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
        left_iri   = (((np.nanmean(np.abs([S3])))/630)^(0.893))*100
        right_iri  = (((np.nanmean(np.abs([S4])))/630)^(0.893))*100
        avg_iri    = np.nanmean(np.abs([S3, S4]))
        avg_texture= np.nanmean(np.abs(masked))
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
        
        # Also update GPS display even when not running
        if self.project and not self.running:
            lat, lon, alt, speed = self.gps_reader.get_position()
            if lat is not None and lon is not None:
                self.var_lat.set(f"{lat:.6f}")
                self.var_lon.set(f"{lon:.6f}")
                if alt is not None:
                    self.var_alt.set(f"{alt:.1f} m")
                if speed is not None:
                    self.var_speed.set(f"{speed:.1f} km/h")
        
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

    # ---------- Sensor Selection ----------
    def open_sensor_selection(self):
        win = ctk.CTkToplevel(self)
        win.title("Select Sensors to Export")
        win.geometry("800x550")
        win.transient(self)
        win.grab_set()
        try: win.attributes("-topmost", True)
        except Exception: pass

        ctk.CTkLabel(win, text="Select Sensors to Export", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=(10,5))
        
        # Sensor status info
        status_info = ctk.CTkFrame(win)
        status_info.pack(fill="x", padx=20, pady=(5,10))
        self.sensor_status_label = ctk.CTkLabel(status_info, text="Sensor readings updating...", 
                                                font=ctk.CTkFont(size=10))
        self.sensor_status_label.pack()

        # Create a frame for checkboxes with readings
        sens_frame = ctk.CTkScrollableFrame(win, height=350)
        sens_frame.pack(fill="both", expand=True, padx=20, pady=10)

        # Create sensor checkboxes with live readings (3 columns)
        self.sensor_checkboxes = []
        self.sensor_reading_labels = []
        for i in range(SENSORS):
            row = i // 3
            col = i % 3
            
            # Create frame for each sensor
            sensor_frame = ctk.CTkFrame(sens_frame)
            sensor_frame.grid(row=row, column=col, sticky="w", padx=15, pady=5)
            
            chk = ctk.CTkCheckBox(sensor_frame, text=f"S{i+1}", variable=self.sensor_selected[i], width=40)
            chk.pack(side="left", padx=(0,5))
            
            # Add reading label (shows both raw and calibrated)
            reading_label = ctk.CTkLabel(sensor_frame, text="--", 
                                        font=ctk.CTkFont(size=9), 
                                        width=150,
                                        anchor="w")
            reading_label.pack(side="left")
            
            self.sensor_checkboxes.append(chk)
            self.sensor_reading_labels.append(reading_label)
            

        # Update readings periodically
        def update_sensor_readings():
            if not win.winfo_exists():
                return
            
            valid_count = 0
            for i in range(SENSORS):
                raw_val = self.latest_raw[i]
                if np.isfinite(raw_val):
                    calib_val = raw_val - self.calib_values[i]
                    self.sensor_reading_labels[i].configure(
                                text=f"Raw:{raw_val:.1f} → Cal:{calib_val:.1f}",
                                text_color="#43a047" if np.isfinite(calib_val) else "#d32f2f"
                            )
                    valid_count += 1
                else:
                    self.sensor_reading_labels[i].configure(
                        text="✗ No data",
                        text_color="#d32f2f"
                    )
            
            self.sensor_status_label.configure(
                text=f"Live Readings: {valid_count}/{SENSORS} sensors active | Raw=absolute, Cal=relative"
            )
            
            win.after(200, update_sensor_readings)
        
        update_sensor_readings()

        # Add buttons
        btn_frame = ctk.CTkFrame(win)
        btn_frame.pack(pady=20)

        def select_all():
            for var in self.sensor_selected:
                var.set(True)

        def deselect_all():
            for var in self.sensor_selected:
                var.set(False)
        
        def select_active_only():
            """Select only sensors with valid data."""
            for i in range(SENSORS):
                if np.isfinite(self.latest_raw[i]):
                    self.sensor_selected[i].set(True)
                else:
                    self.sensor_selected[i].set(False)
            messagebox.showinfo("Active Sensors", 
                              f"Selected {sum(1 for i in range(SENSORS) if np.isfinite(self.latest_raw[i]))} active sensors")

        def apply_and_close():
            selected_count = sum(v.get() for v in self.sensor_selected)
            active_count = sum(1 for i in range(SENSORS) if np.isfinite(self.latest_raw[i]))
            self.rebuild_graphs()
            messagebox.showinfo("Sensors Updated", 
                              f"{selected_count} sensors selected for export and plotting.\n"
                              f"({active_count} sensors currently active)\n\n"
                              f"Note: Graphs show CALIBRATED values (relative to start position)")
            win.destroy()

        ctk.CTkButton(btn_frame, text="Select All", command=select_all, width=120).pack(side="left", padx=5)
        ctk.CTkButton(btn_frame, text="Deselect All", command=deselect_all, width=120).pack(side="left", padx=5)
        ctk.CTkButton(btn_frame, text="Select Active Only", command=select_active_only, width=140).pack(side="left", padx=5)
        ctk.CTkButton(btn_frame, text="Apply & Close", command=apply_and_close, width=120).pack(side="left", padx=5)

    # ---------- Calibration ----------
    def get_calibration_value(self): 
        return self.calib_values

    # ---------- Device Status Monitoring ----------
    def start_device_monitoring(self):
        """Start monitoring device connection status."""
        self.update_device_status()
    
    def update_device_status(self):
        """Update visual indicators for all devices."""
        if not hasattr(self, 'project') or self.project is None:
            return
            
        # Update GPS status
        self.update_gps_status()
        
        # Update Camera status
        self.update_camera_status()
        
        # Update Laser status (only if sensor worker is running)
        self.update_laser_status()
        
        # Schedule next update
        self.after(2000, self.update_device_status)
    
    def update_gps_status(self):
        """Update GPS status indicator."""
        if self.gps_reader and self.gps_worker and self.gps_worker.is_alive():
            lat, lon, alt, speed = self.gps_reader.get_position()
            fix_info = self.gps_reader.get_fix_info()
            
            if lat is not None and lon is not None:
                # Connected and has fix
                self.gps_status_light.configure(text_color="#43a047")  # Green
                self.gps_status_text.configure(text=f"Connected - {fix_info['fix_type']}")
            else:
                # Connected but no fix
                self.gps_status_light.configure(text_color="#fbc02d")  # Yellow
                self.gps_status_text.configure(text=f"No Fix ({fix_info['satellites']} sats)")
        else:
            # Not connected
            self.gps_status_light.configure(text_color="#d32f2f")  # Red
            self.gps_status_text.configure(text="Disconnected")
    
    def update_camera_status(self):
        """Update camera status indicator."""
        connected_count = 0
        for cam_id, worker in self.cameras.items():
            if worker.is_alive() and getattr(worker, 'is_connected', False):
                connected_count += 1
        
        total_cams = len(CAM_IDS)
        self.cam_status_text.configure(text=f"{connected_count}/{total_cams} Connected")
        
        if connected_count == total_cams:
            self.cam_status_light.configure(text_color="#43a047")  # Green - all connected
        elif connected_count > 0:
            self.cam_status_light.configure(text_color="#fbc02d")  # Yellow - partial
        else:
            self.cam_status_light.configure(text_color="#d32f2f")  # Red - none connected
    
    def update_laser_status(self):
        """Update laser sensor status indicator."""
        if self.sensor_worker and self.sensor_worker.is_alive():
            # Count valid sensor readings
            valid_sensors = sum(1 for val in self.latest_raw if np.isfinite(val))
            self.laser_status_text.configure(text=f"{valid_sensors}/{SENSORS} Active")
            
            if valid_sensors >= SENSORS * 0.8:  # 80% or more working
                self.laser_status_light.configure(text_color="#43a047")  # Green
            elif valid_sensors >= SENSORS * 0.5:  # 50% or more working
                self.laser_status_light.configure(text_color="#fbc02d")  # Yellow
            else:
                self.laser_status_light.configure(text_color="#d32f2f")  # Red
        else:
            self.laser_status_text.configure(text="Not initialized")
            self.laser_status_light.configure(text_color="#d32f2f")  # Red
    
    def start_gps_updates(self):
        """Start updating GPS information display every 2 seconds."""
        self.update_gps_info()
    
    def update_gps_info(self):
        """Update GPS information in the UI."""
        if not hasattr(self, 'project') or self.project is None:
            return
        
        if self.gps_reader:
            # Get position and fix info
            lat, lon, alt, speed = self.gps_reader.get_position()
            fix_info = self.gps_reader.get_fix_info()
            
            # Update GPS info variables
            self.var_gps_fix.set(fix_info['fix_type'])
            self.var_gps_sats.set(str(fix_info['satellites']))
            hdop_str = f"{fix_info['hdop']:.1f}" if fix_info['hdop'] < 99 else "N/A"
            self.var_gps_hdop.set(hdop_str)
            
            # Update position display if not running (during running, telemetry updates it)
            if not self.running and lat is not None and lon is not None:
                self.var_lat.set(f"{lat:.6f}")
                self.var_lon.set(f"{lon:.6f}")
                if alt is not None:
                    self.var_alt.set(f"{alt:.1f} m")
                if speed is not None:
                    self.var_speed.set(f"{speed:.1f} km/h")
        
        # Schedule next update
        self.after(2000, self.update_gps_info)

    # ---------- Misc ----------
    def new_project(self):
        if messagebox.askyesno("New Project", "This will reset configuration. Continue?"): self.on_reset()

    def open_manual(self): 
        messagebox.showinfo("User Manual", "Hook your manual PDF path in open_manual().")

    def on_close(self):
        """Clean up all resources and stop all threads before closing."""
        logger.info("=" * 60)
        logger.info("SHUTTING DOWN APPLICATION")
        logger.info("=" * 60)
        
        try:
            # 1. Stop data collection if running
            if self.running:
                logger.info("Stopping data collection...")
                self.running = False
                self.paused_event.set()
            
            # 2. Stop sensor worker thread
            if self.sensor_worker:
                logger.info("Stopping sensor worker...")
                try:
                    self.sensor_worker.stop()
                    self.sensor_worker.join(timeout=2.0)
                    if self.sensor_worker.is_alive():
                        logger.warning("Sensor worker did not stop cleanly")
                    else:
                        logger.info("✓ Sensor worker stopped")
                except Exception as e:
                    logger.error(f"Error stopping sensor worker: {e}")
                finally:
                    self.sensor_worker = None
            
            # 3. Stop GPS worker thread
            if self.gps_worker:
                logger.info("Stopping GPS worker...")
                try:
                    self.gps_worker.stop()
                    self.gps_worker.join(timeout=2.0)
                    if self.gps_worker.is_alive():
                        logger.warning("GPS worker did not stop cleanly")
                    else:
                        logger.info("✓ GPS worker stopped")
                except Exception as e:
                    logger.error(f"Error stopping GPS worker: {e}")
                finally:
                    self.gps_worker = None
            
            # 4. Disconnect GPS
            if self.gps_reader:
                logger.info("Disconnecting GPS...")
                try:
                    self.gps_reader.disconnect()
                    logger.info("✓ GPS disconnected")
                except Exception as e:
                    logger.error(f"Error disconnecting GPS: {e}")
            
            # 5. Stop DMI worker thread
            if hasattr(self, 'dmi_worker') and self.dmi_worker:
                logger.info("Stopping DMI worker...")
                try:
                    self.dmi_worker.stop()
                    self.dmi_worker.join(timeout=2.0)
                    if self.dmi_worker.is_alive():
                        logger.warning("DMI worker did not stop cleanly")
                    else:
                        logger.info("✓ DMI worker stopped")
                except Exception as e:
                    logger.error(f"Error stopping DMI worker: {e}")
            
            # 6. Stop camera workers
            logger.info("Stopping camera workers...")
            try:
                self.stop_cameras()
                logger.info("✓ Camera workers stopped")
            except Exception as e:
                logger.error(f"Error stopping cameras: {e}")
            
            # 7. Close video writers
            logger.info("Closing video writers...")
            try:
                self.close_writers()
                logger.info("✓ Video writers closed")
            except Exception as e:
                logger.error(f"Error closing writers: {e}")
            
            # 8. Clean up laser sensors
            if hasattr(self, "sensors") and self.sensors:
                logger.info("Cleaning up laser sensors...")
                try:
                    cleanup_lasers(self.sensors)
                    logger.info("✓ Laser sensors cleaned up")
                except Exception as e:
                    logger.error(f"Laser cleanup failed: {e}")
            
            logger.info("=" * 60)
            logger.info("SHUTDOWN COMPLETE")
            logger.info("=" * 60)
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
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