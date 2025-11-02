"""
NSV Controller v21 - Network Survey Vehicle Data Collection System

Main application for highway road condition surveying using:
- Multi-camera video recording (4 RTSP cameras)
- Laser distance sensors (13x Micro-Epsilon ILD1320)
- GPS tracking and chainage calculation
- Real-time data visualization and Excel export

Author: Road Survey Team
Version: 2.1

Changelog v21:
- Enhanced calibration window with connection status indicators
- Visual sensor status (green=connected, red=disconnected)
- COM port display for each sensor (shows initialized/detected ports)
- Sensor connection checkboxes
- Improved real-time sensor reading display
- Added CalibrationReader thread for continuous sensor monitoring
- Sensors auto-initialize on app startup for immediate calibration access
- **New: "Re-Scan Sensors" button** - Re-scan and detect sensors manually
- **New: COM port auto-detection** - scan_com_ports() and detect_sensors() functions
- pyserial integration for COM port scanning (optional dependency)
- Fixed: Calibration window shows live readings immediately
- **CRITICAL FIX: Block size changed from 1 to 100** - Ensures fresh sensor data
- Adopted proven TransferData() strategy from working MEDAQLib example
- Improved error logging with detailed messages from GetError(1024)
- Better data validation and availability checks
"""

# ============================================================================
# IMPORTS
# ============================================================================

# Standard library imports
import os
import time
import math
import threading
import queue
import random
import logging
from datetime import datetime
from collections import deque
from math import radians, sin, cos, sqrt, atan2

# GUI framework
import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox

# Data processing and visualization
import numpy as np
import cv2
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from PIL import Image

# Hardware interface
from MEDAQLib import MEDAQLib, ME_SENSOR, ERR_CODE

# Serial port detection
try:
    import serial.tools.list_ports
    SERIAL_AVAILABLE = True
except Exception:
    SERIAL_AVAILABLE = False
    logger_temp = logging.getLogger(__name__)

# Optional Excel support (required for data export)
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

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# APPLICATION CONFIGURATION CONSTANTS
# ============================================================================

# UI Layout
LEFT_WIDTH = 300              # Width of left control panel in pixels
WINDOW_GEOM = "1680x960"      # Default window size

# Camera configuration
CAM_IDS = [0, 1, 2, 3]        # Camera identifiers
CAM_NAMES = {0: "Front", 1: "Left", 2: "Right", 3: "Back"}  # Camera position names

# RTSP camera URLs
# TODO: Update with your actual camera IP addresses and credentials
CAM_URLS = [
    "rtsp://192.168.1.103:554/stream1",  # Front camera
    "rtsp://192.168.1.102:554/stream1",  # Left camera
    "rtsp://192.168.1.100:554/stream1",  # Right camera
    "rtsp://192.168.1.101:554/stream0?username=admin&password=E10ADC3949BA59ABBE56E057F20F883E"  # Back camera
    # WARNING: Credentials in plaintext - consider using environment variables
]
# Camera resolution and frame rate settings
PREVIEW_RES = (360, 202)     # Preview resolution (w,h) for UI display
RECORD_RES  = (1280, 720)    # Recording resolution (w,h) for saved video/images
CAM_FPS     = 25              # Camera capture frame rate
PREVIEW_FPS = 15              # Preview display FPS (lower to reduce UI load)

# Data binning configuration
BIN_SIZE_METERS = 10.0        # Distance bin size in meters (10m segments)
BIN_SIZE_KM = 0.01            # Distance bin size in kilometers (0.01 km = 10m)
# Visual styling constants
NEON_GREEN   = "#39ff14"          # Color for graph lines
HEADER_COLOR = (255, 0, 0)        # BGR format: blue header background
TEXT_COLOR   = (255, 255, 255)    # White text for overlays
FONT         = cv2.FONT_HERSHEY_SIMPLEX  # OpenCV font for text rendering
BG_COLOR     = "#0b0f14"          # Dark background for graphs
GRID_COLOR   = "#263238"          # Grid line color for graphs
SPINE_COLOR  = "#607d8b"          # Graph axis spine color

# Video overlay configuration
HEADER_MAX_FRAC = 0.20            # Maximum header height as fraction of frame (20%)
HEADER_LINES = 5                  # Maximum lines per column in header

# Application resources
LOGO_PATH = "cropped-Roadworks-LogoR-768x576-1.jpeg"  # Splash screen logo
MAX_QUEUE_SIZE = 5                # Frame queue size limit (prevents memory overflow)

# ---------------------------
# Laser Sensor Configuration
# ---------------------------
SENSORS = 13  # Total number of Micro-Epsilon ILD1320 laser sensors

# COM port assignments for each sensor
# TODO: Configure actual COM ports for your hardware setup
# Note: Ensure no duplicate ports - each sensor needs a unique COM port
COM_PORTS = [f"COM{i}" for i in range(10, 10 + SENSORS)]  # COM10 to COM22

# Sensor sampling configuration
EXPECTED_BLOCK_SIZE = 100  # Fetch 100 values per block to ensure recent data (like working example)
TOF_HZ = 100  # Sensor sampling rate in Hz (Time-of-Flight measurements)

# ---------------------------
# Sensor Position Mapping
# ---------------------------
# Physical layout of sensors across the vehicle width (left to right)
# Used for calculating road condition metrics:
#   - Sensors S1, S2: Left wheel path (rutting measurement)
#   - Sensors S3, S4: Center sensors (IRI/roughness measurement)
#   - Sensors S5, S6: Right wheel path (rutting measurement)
#   - Sensors S7-S13: Additional texture/profile measurements (if configured)
#
# Metric Calculation:
#   Left Rut:   Average of S1, S2
#   Right Rut:  Average of S5, S6
#   Left IRI:   S3 (left roughness track)
#   Right IRI:  S4 (right roughness track)
#   Texture:    Average of all selected sensors


# ---------------------------
# Helper Functions
# ---------------------------

# Timestamp formatters for logging and file naming
now_local_str = lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")  # Human-readable timestamp
ts_for_path   = lambda: datetime.now().strftime("%Y-%m-%d_%H-%M-%S")  # Filesystem-safe timestamp

def make_testcard(w, h, text="NO CAMERA"):
    """
    Generate a color bar test pattern with overlay text.

    Used as a fallback image when camera feed is unavailable or fails.
    Creates SMPTE-style color bars with centered error message.

    Args:
        w (int): Width of the test card in pixels
        h (int): Height of the test card in pixels
        text (str): Error message to display on the test card

    Returns:
        numpy.ndarray: BGR image array of shape (h, w, 3)
    """
    img = np.zeros((h, w, 3), dtype=np.uint8)
    bars = [(255,255,255),(255,255,0),(0,255,255),(0,255,0),(255,0,255),(255,0,0),(0,0,255)]
    bw = max(1, w // len(bars))
    for i, c in enumerate(bars):
        img[:, i*bw:(i+1)*bw] = c
    cv2.putText(img, text, (10, h//2), FONT, 0.9, (0,0,0), 3, cv2.LINE_AA)
    cv2.putText(img, text, (10, h//2), FONT, 0.9, (255,255,255), 1, cv2.LINE_AA)
    return img

def draw_header_to_size(frame, ctx, size):
    """
    Overlay telemetry header onto video frame with project and GPS information.

    Creates a blue header bar at the top of the frame containing three columns:
    - Left: Project metadata (ID, location, highway numbers, section, direction)
    - Center: GPS data (lat/lon/alt, distance, chainage, speed, LRP)
    - Right: Timestamp (date and time)

    Args:
        frame: Input image (numpy BGR array or PIL Image). Can be None for blank frame.
        ctx (dict): Context dictionary with telemetry values. Expected keys:
            'survey', 'survey_location', 'nh', 'oldnh', 'section_code', 'direction',
            'lat', 'lon', 'alt', 'distance', 'chainage', 'speed', 'lrp', 'date', 'time'
        size (tuple): Target (width, height) in pixels for the output frame

    Returns:
        numpy.ndarray: BGR frame with header overlay at specified size
    """
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
    """
    Calculate the great-circle distance between two GPS coordinates.

    Uses the Haversine formula to compute the shortest distance over the earth's
    surface between two points specified by latitude and longitude.

    Args:
        lat1 (float): Latitude of first point in decimal degrees
        lon1 (float): Longitude of first point in decimal degrees
        lat2 (float): Latitude of second point in decimal degrees
        lon2 (float): Longitude of second point in decimal degrees

    Returns:
        float: Distance between the two points in meters

    Note:
        Assumes Earth is a perfect sphere with radius 6,371 km.
        Accuracy degrades for very short distances (<1m) or antipodal points.
    """
    R = 6371e3  # Earth radius in meters
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


def read_laser_values(sensors):
    """
    Read scaled distance measurements from all Micro-Epsilon ILD1320 laser sensors.

    Uses proven TransferData() approach to fetch blocks of recent measurements.
    This ensures we get the most recent sensor reading, not stale buffered data.

    Strategy (based on working MEDAQLib example):
    1. Check DataAvail() >= EXPECTED_BLOCK_SIZE (100 samples)
    2. Use TransferData(100) to fetch recent block
    3. Extract last value from block (most recent reading)
    4. Verify with GetLastError() after each operation

    Args:
        sensors (list): List of MEDAQLib sensor instances (length = SENSORS)

    Returns:
        list: Scaled distance values (floats) from each sensor.
              NaN indicates sensor not initialized or read error.

    Note:
        Block size of 100 ensures we get fresh data from sensor buffer.
        Failed reads are logged with detailed error messages from GetError(1024).
    """
    values = [float('nan')] * len(sensors)

    for i, sensor in enumerate(sensors):
        if sensor is None:
            continue  # Sensor not initialized

        try:
            # Check if sufficient data is available in buffer
            currently_available = sensor.DataAvail()

            if sensor.GetLastError() == ERR_CODE.ERR_NOERROR and currently_available >= EXPECTED_BLOCK_SIZE:
                # Fetch block of recent data from MEDAQLib's internal buffer
                transferred_data = sensor.TransferData(EXPECTED_BLOCK_SIZE)

                if sensor.GetLastError() == ERR_CODE.ERR_NOERROR:
                    # Extract scaled values and count
                    scaled_data = transferred_data[1]  # Scaled values array
                    nr_values_transferred = transferred_data[2]  # Number of values

                    if nr_values_transferred > 0:
                        # Use the last value in block (most recent reading)
                        values[i] = scaled_data[-1]
                        logger.debug(f"Sensor S{i+1}: {values[i]:.3f} ({nr_values_transferred} values in block)")
                    else:
                        error_msg = sensor.GetError(1024)
                        logger.warning(f"Sensor S{i+1}: No data transferred - {error_msg}")
                else:
                    error_msg = sensor.GetError(1024)
                    logger.warning(f"Sensor S{i+1}: Transfer error - {error_msg}")
            else:
                # Insufficient data or error
                if sensor.GetLastError() != ERR_CODE.ERR_NOERROR:
                    error_msg = sensor.GetError(1024)
                    logger.debug(f"Sensor S{i+1}: Data check error - {error_msg}")
                else:
                    logger.debug(f"Sensor S{i+1}: Insufficient data ({currently_available}/{EXPECTED_BLOCK_SIZE})")

        except Exception as e:
            logger.error(f"Sensor S{i+1}: Exception during read - {str(e)}")

    return values

def initialize_lasers():
    """
    Initialize all Micro-Epsilon ILD1320 laser sensors on configured COM ports.

    Creates sensor instances, configures RS232 communication parameters,
    opens connections, and enables logging for each sensor.

    Returns:
        list: Sensor instances (MEDAQLib objects). None entries indicate failed initialization.

    Side Effects:
        - Creates log files: MEDAQLib-log-S1.txt through MEDAQLib-log-S13.txt
        - Logs initialization status (success/failure) for each sensor

    Configuration:
        - Interface: RS232 serial communication
        - COM ports: Defined in COM_PORTS constant
        - Automatic mode: Enabled (parameter value = 3)
        - Logging: Enabled for diagnostics
    """
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
    """
    Safely close and release all laser sensor connections.

    Called during shutdown to properly close COM ports and free sensor resources.
    Handles errors gracefully to ensure all sensors are cleaned up even if some fail.

    Args:
        sensors (list): List of MEDAQLib sensor instances to clean up

    Side Effects:
        - Closes RS232 connections for all sensors
        - Releases MEDAQLib resources
        - Logs cleanup status for each sensor
    """
    for i, sensor in enumerate(sensors):
        if sensor is not None:
            try:
                sensor.CloseSensor()
                sensor.ReleaseSensorInstance()
                logger.info(f"Sensor {i+1} closed")
            except Exception as e:
                logger.error(f"Sensor {i+1} cleanup failed: {str(e)}")

# ============================================================================
# COM Port Detection and Sensor Discovery
# ============================================================================

def scan_com_ports():
    """
    Scan for available COM ports on the system.

    Returns:
        list: List of available COM port names (e.g., ['COM3', 'COM10', 'COM12'])
    """
    available_ports = []

    if not SERIAL_AVAILABLE:
        logger.warning("pyserial not available - cannot scan COM ports")
        logger.info("Install with: pip install pyserial")
        return available_ports

    try:
        ports = serial.tools.list_ports.comports()
        available_ports = [port.device for port in ports]
        logger.info(f"Found {len(available_ports)} COM ports: {available_ports}")
    except Exception as e:
        logger.error(f"Error scanning COM ports: {e}")

    return available_ports

def detect_sensors(port_list=None, max_sensors=SENSORS):
    """
    Detect connected Micro-Epsilon ILD1320 sensors on specified COM ports.

    Attempts to initialize each sensor and check if it responds. Returns a mapping
    of sensor indices to COM ports for successfully detected sensors.

    Args:
        port_list (list): List of COM port names to scan. If None, scans all available ports.
        max_sensors (int): Maximum number of sensors to detect

    Returns:
        dict: Dictionary mapping sensor index to (COM_port, sensor_instance)
              Example: {0: ('COM10', sensor_obj), 1: ('COM12', sensor_obj)}
    """
    detected_sensors = {}

    # Get list of ports to scan
    if port_list is None:
        port_list = scan_com_ports()

    if not port_list:
        logger.warning("No COM ports available for sensor detection")
        return detected_sensors

    logger.info(f"Scanning {len(port_list)} COM ports for sensors...")

    sensor_index = 0
    for port in port_list:
        if sensor_index >= max_sensors:
            break

        try:
            # Create sensor instance
            sensor = MEDAQLib.CreateSensorInstance(ME_SENSOR.SENSOR_ILD1320)
            if sensor.iSensor == 0:
                logger.debug(f"Port {port}: Failed to create sensor instance")
                continue

            # Configure RS232 interface
            sensor.SetParameterString("IP_Interface", "RS232")
            sensor.SetParameterString("IP_Port", port)
            sensor.SetParameterInt("IP_AutomaticMode", 3)

            # Try to open sensor
            sensor.OpenSensor()

            # Check if sensor opened successfully
            if sensor.GetLastError() == ERR_CODE.ERR_NOERROR:
                # Verify sensor is responding by trying to read
                time.sleep(0.1)  # Give sensor time to initialize

                if sensor.DataAvail() >= 0:
                    detected_sensors[sensor_index] = (port, sensor)
                    logger.info(f"✓ Sensor S{sensor_index+1} detected on {port}")
                    sensor_index += 1
                else:
                    # Sensor opened but not responding
                    error_msg = sensor.GetError(1024)
                    logger.debug(f"Port {port}: Sensor not responding - {error_msg}")
                    sensor.CloseSensor()
                    sensor.ReleaseSensorInstance()
            else:
                # Failed to open
                error_msg = sensor.GetError(1024)
                logger.debug(f"Port {port}: Failed to open - {error_msg}")
                sensor.CloseSensor()
                sensor.ReleaseSensorInstance()

        except Exception as e:
            logger.debug(f"Port {port}: Detection error - {str(e)}")
            continue

    logger.info(f"Detection complete: {len(detected_sensors)} sensor(s) found")
    return detected_sensors

# ============================================================================
# Camera Worker Thread
# ============================================================================
# Handles real-time camera capture, frame processing, and video recording
# Each camera runs in its own thread for parallel processing
# ============================================================================

class CameraWorker(threading.Thread):
    """
    Background thread for capturing, processing, and recording video from one camera.

    Manages dual-stream processing:
    - Preview stream: Lower resolution (360x202) at 15 FPS for UI display
    - Record stream: Full resolution (1280x720) at 25 FPS with telemetry overlay

    Features:
    - Automatic reconnection on camera failures
    - Low-latency RTSP configuration
    - Frame rate limiting to prevent queue overflow
    - Telemetry overlay on recorded frames

    Attributes:
        cam_id (int): Camera identifier (0-3)
        cam_url (str): RTSP URL for the camera
        latest_preview_frame: Most recent preview frame (RGB)
        latest_record_frame: Most recent full-res frame with overlay (BGR)
    """
    def __init__(self, cam_id, out_queue, get_writer, get_overlay_ctx,
                 preview_size=PREVIEW_RES, record_size=RECORD_RES):
        """
        Initialize camera worker thread.

        Args:
            cam_id (int): Camera index (0-3)
            out_queue (queue.Queue): Queue for preview frames to UI
            get_writer (callable): Function to get video writer for this camera
            get_overlay_ctx (callable): Function to get telemetry context for overlay
            preview_size (tuple): (width, height) for preview frames
            record_size (tuple): (width, height) for recorded frames
        """
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
        """
        Main camera capture loop.

        Process flow:
        1. Capture frame from RTSP stream
        2. Create dual streams (preview + record)
        3. Add telemetry overlay to record stream
        4. Send preview to UI queue (rate-limited)
        5. Write record stream to video file
        6. Maintain target frame rate
        """
        # Initial camera connection
        if not self._open_cap():
            logger.error(f"Camera {self.cam_id} initial open failed")

        while not self.stop_event.is_set():
            # Retry connection if camera was lost
            if self.cap is None:
                time.sleep(1)
                self._open_cap()
                continue

            start_time = time.time()

            # Capture frame from camera
            ok, frame = self.cap.read()

            # Handle capture failures
            if not ok or frame is None or frame.size == 0:
                self.consecutive_fails += 1
                logger.warning(f"Camera {self.cam_id} read failed ({self.consecutive_fails})")

                # After 5 consecutive failures, show permanent error testcard
                if self.consecutive_fails > 5:
                    frame = make_testcard(
                        self.record_size[0], self.record_size[1],
                        f"{CAM_NAMES.get(self.cam_id, f'CAM {self.cam_id}')} PERMANENT ERROR"
                    )
                else:
                    # Release and retry connection
                    self.cap.release()
                    self.cap = None
                    time.sleep(0.5)
                    continue
            else:
                # Reset failure counter on successful read
                self.consecutive_fails = 0

            # Get current telemetry for overlay
            ctx = self.get_overlay_ctx()

            # Create dual-stream processing:
            # 1. Preview: Low-res for UI (360x202)
            preview = cv2.resize(frame, self.preview_size)

            # 2. Record: Full-res with telemetry header overlay (1280x720)
            record = draw_header_to_size(frame, ctx, self.record_size)

            # Convert preview to RGB for tkinter display
            preview_rgb = cv2.cvtColor(preview, cv2.COLOR_BGR2RGB)

            # Store latest frames for image capture (thread-safe access)
            self.latest_preview_frame = preview_rgb
            self.latest_record_frame = record
            self.latest_frame_ts = time.time()

            # Send preview to UI queue (rate-limited to PREVIEW_FPS)
            if start_time - self.last_frame_time >= self.frame_interval:
                if self.out_queue.qsize() < MAX_QUEUE_SIZE:
                    self.out_queue.put((self.cam_id, preview_rgb))
                    self.last_frame_time = start_time
                else:
                    # Drop frame if queue is full (prevents memory overflow)
                    logger.warning(f"Camera {self.cam_id} queue full, dropping frame")

            # Write full-res frame to video file (if recording is active)
            writer = self.get_writer(self.cam_id)
            if writer is not None:
                writer.write(record)

            # Sleep to maintain target frame rate (CAM_FPS = 25)
            elapsed = time.time() - start_time
            sleep_time = max(0, 1.0 / CAM_FPS - elapsed)
            time.sleep(sleep_time)

        # Cleanup on thread exit
        if self.cap is not None:
            self.cap.release()
            logger.info(f"Camera {self.cam_id} released")

    def stop(self):
        self.stop_event.set()

# ============================================================================
# SIMULATED INPUT FUNCTIONS - FOR TESTING ONLY
# ============================================================================
# TODO: CRITICAL - Replace these functions with real GPS hardware driver
# Current implementation uses simulated data for development/testing purposes
# Required: Integrate actual GPS module (e.g., NMEA parser for serial GPS)
# ============================================================================

# Simulated GPS starting position (Hyderabad coordinates)
sim_lat = 17.385  # Latitude in decimal degrees
sim_lon = 78.486  # Longitude in decimal degrees
sim_t = time.time()  # Track time for simulation

def sim_speed_kmph(t):
    """
    Simulate vehicle speed with sinusoidal variation.

    Args:
        t: Current timestamp

    Returns:
        float: Simulated speed in km/h (base 45 km/h with ±10 km/h variation)

    TODO: Remove when real speed sensor is integrated
    """
    return max(0.0, 45 + 10*math.sin(2*math.pi*t/20.0) + random.uniform(-3,3))

def sim_gps():
    """
    Simulate GPS coordinates based on simulated speed and movement.

    Returns:
        tuple: (latitude, longitude, altitude) in decimal degrees and meters

    TODO: REPLACE WITH REAL GPS DRIVER
    Required implementation:
    - Read from GPS hardware (serial/USB)
    - Parse NMEA sentences (GGA, RMC)
    - Return actual lat/lon/alt values
    """
    global sim_lat, sim_lon, sim_t

    t = time.time()
    dt = t - sim_t
    sim_t = t

    # Calculate simulated movement
    speed_kmph = sim_speed_kmph(t)
    speed_mps = speed_kmph / 3.6  # Convert km/h to m/s
    delta_m = speed_mps * dt  # Distance traveled in meters

    # Simulate occasional direction reversals
    dir_s = 1 if math.sin(t/30) > 0 else -1

    # Convert meters to degrees (approximate: 1 degree lat ≈ 111.12 km)
    delta_lat = (delta_m * dir_s) / 111120

    # Add movement and random noise
    sim_lat += delta_lat
    sim_lat += random.uniform(-1e-6, 1e-6)  # GPS jitter
    sim_lon += random.uniform(-1e-6, 1e-6)  # GPS jitter

    # Simulate altitude with small variations
    alt = 505 + random.uniform(-1, 1)

    return sim_lat, sim_lon, alt

def sim_tof_values():
    """
    Simulate Time-of-Flight sensor readings (for testing without hardware).

    Returns:
        list: 13 simulated sensor values centered around 800 with Gaussian noise

    TODO: Remove when using real laser sensors (read_laser_values already implemented)
    """
    return [800 + random.gauss(0, 4) for _ in range(SENSORS)]

# ============================================================================
# Sensor Worker Thread
# ============================================================================
# Manages laser sensor data acquisition, GPS tracking, and distance binning
# Runs at 100 Hz for high-frequency road surface profiling
# ============================================================================

class SensorWorker(threading.Thread):
    """
    Background thread for sensor data acquisition and processing.

    Coordinates multiple data sources:
    - 13 laser sensors reading at 100 Hz
    - GPS position and speed (currently simulated - TODO: integrate real GPS)
    - Distance-based data binning (10-meter segments)

    Responsibilities:
    - Read and calibrate laser sensor values
    - Track vehicle position and calculate speed from GPS
    - Accumulate sensor data into 10-meter bins
    - Trigger callbacks when bins complete
    - Update UI with real-time telemetry

    Attributes:
        distance_m (float): Total distance traveled in current run
        bin_buffers (list): Accumulated sensor readings for current 10m bin
        sensors (list): Initialized laser sensor instances
    """
    def __init__(self, ui_queue, paused_event, calib_getter, ten_m_callback, sensor_selected_getter, raw_setter):
        """
        Initialize sensor worker thread.

        Args:
            ui_queue (queue.Queue): Queue for sending telemetry updates to UI
            paused_event (threading.Event): Event to pause data collection
            calib_getter (callable): Returns calibration offsets for sensors
            ten_m_callback (callable): Called when 10-meter bin completes
            sensor_selected_getter (callable): Returns list of enabled sensors
            raw_setter (callable): Updates latest raw sensor values in UI
        """
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
        """
        Main sensor worker loop - runs at 100 Hz.

        Process flow:
        1. Read GPS and laser sensors
        2. Apply calibration offsets
        3. Accumulate data in 10-meter bins
        4. Calculate speed from GPS movement
        5. Trigger callbacks when bins complete
        """
        while not self.stop_event.is_set():
            t = time.time()

            # Skip processing if paused, but maintain timing
            if self.paused_event.is_set():
                self.prev_t = t
                time.sleep(0.01)
                continue

            # Acquire sensor data
            lat, lon, alt = sim_gps()  # TODO: Replace with real GPS driver
            raw = read_laser_values(self.sensors)  # Read all 13 laser sensors

            # Update UI with raw values (for calibration window)
            try:
                self.raw_setter(raw)
            except Exception:
                pass  # UI may not be ready yet

            # Apply calibration offsets to get calibrated values
            calib_vals = self.calib_getter()
            vals = [raw[i] - calib_vals[i] if np.isfinite(raw[i]) else float('nan') for i in range(SENSORS)]

            # Accumulate sensor readings in current bin
            for i, v in enumerate(vals):
                self.bin_buffers[i].append(v)

            # Track bin start position (GPS coordinates at bin start)
            if self.bin_start_lat is None:
                self.bin_start_lat = lat
                self.bin_start_lon = lon

            # Calculate speed from GPS movement (only after second reading)
            if self.prev_lat is not None:
                # Compute distance traveled since last reading using Haversine
                dist_delta = haversine(self.prev_lat, self.prev_lon, lat, lon)
                dt = t - self.prev_t
                speed_mps = dist_delta / dt if dt > 0 else 0.0

                # Update cumulative distance and speed (convert m/s to km/h)
                self.distance_m += dist_delta
                self.last_speed = speed_mps * 3.6

            # Store current position for next iteration
            self.prev_lat = lat
            self.prev_lon = lon
            self.prev_t = t

            # Send real-time telemetry to UI
            self.ui_queue.put({
                "type": "telemetry",
                "speed_kmph": self.last_speed,
                "distance_m": self.distance_m,
                "lat": lat, "lon": lon, "alt": alt,
            })

            # Check if we've completed a 10-meter bin
            if self.distance_m >= self.next_ten_m_edge:
                # Calculate average sensor values for this bin
                avgs = [float(np.nanmean(buf)) if len(buf) else float("nan") for buf in self.bin_buffers]

                # Reset buffers for next bin
                self.bin_buffers = [[] for _ in range(SENSORS)]

                # Store bin start coordinates
                start_lat = self.bin_start_lat
                start_lon = self.bin_start_lon

                # Notify UI and main app of completed bin
                self.ui_queue.put({
                    "type": "ten_m",
                    "avg_s": avgs,
                    "speed_kmph": self.last_speed,
                    "lat": lat, "lon": lon, "alt": alt,
                    "start_lat": start_lat, "start_lon": start_lon
                })

                # Trigger data processing callback
                self.ten_m_callback(avgs, self.last_speed, (lat, lon, alt), (start_lat, start_lon, alt))

                # Set next bin edge and update start position
                self.next_ten_m_edge += 10.0
                self.bin_start_lat = lat
                self.bin_start_lon = lon

            # Sleep to maintain 100 Hz sampling rate
            time.sleep(max(0.0, 1.0/TOF_HZ - (time.time()-t)))

    def stop(self):
        self.stop_event.set()
        cleanup_lasers(self.sensors)
        self.sensors = [None] * SENSORS

# ============================================================================
# Calibration Reader Thread
# ============================================================================
# Lightweight background thread that continuously reads sensors for calibration
# Runs independently from the main SensorWorker to enable calibration before
# starting a data collection run
# ============================================================================

class CalibrationReader(threading.Thread):
    """
    Background thread for reading sensors during calibration.

    This thread runs continuously to provide live sensor readings in the
    calibration window, even when the main data collection is not running.

    Features:
    - Initializes sensors on startup
    - Reads all 13 sensors at ~10 Hz (lighter load than main worker)
    - Updates raw sensor values for calibration display
    - Runs independently of main SensorWorker
    - Can re-detect and update sensors via detect button
    """
    def __init__(self, raw_setter):
        """
        Initialize calibration reader thread and sensors.

        Args:
            raw_setter (callable): Function to update latest raw sensor values
        """
        super().__init__(daemon=True)
        self.raw_setter = raw_setter
        self.stop_event = threading.Event()
        self.sensors = None
        self.sensor_port_map = {}  # Maps sensor index to COM port

        # Initialize sensors immediately for calibration
        logger.info("Initializing sensors for calibration...")
        self.sensors = initialize_lasers()

        # Count successfully initialized sensors
        connected_count = sum(1 for s in self.sensors if s is not None)
        logger.info(f"Calibration reader: {connected_count}/{SENSORS} sensors initialized")

        # Map sensors to their COM ports
        for i, sensor in enumerate(self.sensors):
            if sensor is not None and i < len(COM_PORTS):
                self.sensor_port_map[i] = COM_PORTS[i]

    def update_sensors(self, detected_sensors_dict):
        """
        Update sensor instances from detection results.

        Used when "Detect Sensors" button is clicked to re-scan and update.

        Args:
            detected_sensors_dict: Dict mapping sensor index to (port, sensor_instance)
        """
        # Clean up old sensors
        if self.sensors:
            for sensor in self.sensors:
                if sensor is not None:
                    try:
                        sensor.CloseSensor()
                        sensor.ReleaseSensorInstance()
                    except:
                        pass

        # Initialize new sensor list
        self.sensors = [None] * SENSORS
        self.sensor_port_map = {}

        # Populate with detected sensors
        for idx, (port, sensor_obj) in detected_sensors_dict.items():
            if idx < SENSORS:
                self.sensors[idx] = sensor_obj
                self.sensor_port_map[idx] = port

        connected_count = sum(1 for s in self.sensors if s is not None)
        logger.info(f"CalibrationReader: {connected_count} sensor(s) active after detection")

    def run(self):
        """
        Main calibration reader loop - runs at ~10 Hz.

        Continuously reads sensors and updates raw values for calibration display.
        """
        while not self.stop_event.is_set():
            try:
                # Read current sensor values
                raw = read_laser_values(self.sensors)

                # Update main app with latest readings
                try:
                    self.raw_setter(raw)
                except Exception as e:
                    logger.debug(f"CalibrationReader: raw_setter error: {e}")

                # Sleep to maintain ~10 Hz update rate (lighter than main 100 Hz)
                time.sleep(0.1)

            except Exception as e:
                logger.error(f"CalibrationReader error: {e}")
                time.sleep(1)  # Back off on errors

    def stop(self):
        """Stop the calibration reader and cleanup sensors."""
        logger.info("Stopping calibration reader...")
        self.stop_event.set()

        # Clean up sensors
        if self.sensors:
            cleanup_lasers(self.sensors)
            self.sensors = None

# ============================================================================
# MAIN APPLICATION CLASS
# ============================================================================
# Network Survey Vehicle (NSV) Controller
#
# Main GUI application for road condition surveying with:
# - 4 RTSP cameras (Front, Left, Right, Back)
# - 13 laser distance sensors for rutting/IRI/texture measurement
# - GPS tracking and chainage calculation
# - Real-time data visualization and export
#
# Architecture:
# - Multi-threaded: 4 camera threads + 1 sensor thread + main UI thread
# - Event-driven UI updates via queues
# - Distance-based binning (10m segments) with configurable measurement sections
# ============================================================================

class NSVApp(ctk.CTk):
    """
    Main application window for NSV road survey data collection system.

    Manages the complete survey workflow:
    1. Project setup (highway, lane, direction, chainage)
    2. Sensor calibration and selection
    3. Real-time data collection and display
    4. Video recording with telemetry overlay
    5. Excel export of road condition metrics

    Key Features:
    - Dual-resolution video: Preview (360x202@15fps) + Record (1280x720@25fps)
    - Real-time graphs for selected sensors
    - Distance-based binning with configurable measurement sections
    - Automatic folder organization by project/lane/direction
    """
    def __init__(self):
        """Initialize the NSV Controller application and setup UI."""
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

        # Sensor detection and calibration
        self.detected_sensors = {}  # Maps sensor index to (COM_port, sensor_instance)
        self.detected_ports = {}    # Maps sensor index to COM_port string
        self.calibration_reader = None
        self.start_calibration_reader()

        self.build_menu()
        self.build_layout()
        self.left.configure(width=LEFT_WIDTH)
        self.left.grid_propagate(False)

        self.set_controls_state(False)
        self.set_selection_state(True)

        for idx in CAM_IDS: self.start_camera(idx)
        self.after(int(1000 / PREVIEW_FPS), self.poll_cam_frames)  # Match preview FPS
        self.after(60, self.poll_ui_queue)

    # ========================================================================
    # GUI CONSTRUCTION METHODS
    # ========================================================================

    def build_menu(self):
        """Build the application menu bar with File, Calibrate, and Help menus."""
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

    # ========================================================================
    # UI STATE MANAGEMENT
    # ========================================================================

    def set_controls_state(self, enabled: bool):
        """Enable or disable control buttons (Start, Pause, Stop, etc.)."""
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

    # ========================================================================
    # CAMERA MANAGEMENT
    # ========================================================================

    def start_camera(self, cam_id):
        """Start camera worker thread for the specified camera ID."""
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

    # ========================================================================
    # PROJECT CONFIGURATION
    # ========================================================================

    def set_project_specs(self):
        """
        Validate and save project specifications, create folder structure.

        Collects all project parameters from UI, validates inputs, prompts for
        base directory, and creates the folder hierarchy for data storage.
        """
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

    # ========================================================================
    # CONTROL FLOW - START/PAUSE/STOP/RESET
    # ========================================================================

    def on_reset(self):
        """Reset the application to initial state (before project setup)."""
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

    def on_start(self):
        """
        Start data collection run.

        Initializes sensor worker thread, opens video writers, and begins
        recording sensor data and video streams.
        """
        if not self.controls_enabled or self.project is None:
            messagebox.showwarning("Start", "Set Project Specifications first.")
            return

        # Stop calibration reader before starting main sensor worker
        # (can't have both reading sensors simultaneously)
        if self.calibration_reader:
            logger.info("Stopping calibration reader to start main data collection")
            try:
                self.calibration_reader.stop()
                self.calibration_reader.join(timeout=2.0)
            except Exception as e:
                logger.error(f"Error stopping calibration reader: {e}")
            self.calibration_reader = None

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

        # Restart calibration reader after stopping main data collection
        logger.info("Restarting calibration reader after stopping run")
        self.start_calibration_reader()

        self.close_writers()
        self.set_selection_state(True)
        if not silent: messagebox.showinfo("Stopped", "Run stopped and data exported.")

    # ========================================================================
    # DATA EXPORT
    # ========================================================================

    def on_export(self):
        """
        Export collected data to Excel files.

        Creates 4 Excel files:
        - Rutting data (left/right/average rut depths)
        - IRI data (roughness index)
        - Texture data (surface texture measurements)
        - GPS data (coordinates, altitude, speed per 10m bin)

        Handles partial bins (< 10m) at end of run.
        """
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
            # Calculate metrics for partial bin (remaining distance < 10m)
            sel = np.array(self.get_sensor_selection(), dtype=bool)
            masked = np.array(avgs, dtype=float)
            masked[~sel] = np.nan  # Mask unselected sensors
            S1,S2,S3,S4,S5,S6 = masked[:6]  # Extract first 6 sensors

            # Calculate road condition metrics (see sensor mapping at top of file)
            left_rut   = np.nanmean(np.abs([S1, S2]))  # Left wheel path rutting
            right_rut  = np.nanmean(np.abs([S5, S6]))  # Right wheel path rutting
            avg_rut    = np.nanmean(np.abs([S1, S2, S5, S6]))  # Average rutting
            left_iri   = np.nanmean(np.abs([S3]))  # Left track roughness
            right_iri  = np.nanmean(np.abs([S4]))  # Right track roughness
            avg_iri    = np.nanmean(np.abs([S3, S4]))  # Average IRI
            avg_texture= np.nanmean(np.abs(masked))  # Overall surface texture
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
        """
        Process completed 10-meter data bin.

        Called when SensorWorker completes a 10m bin. Updates chainage,
        captures images, accumulates data for measurement windows, and
        flushes windows when measurement section is complete.

        Args:
            avgs: Average sensor values for the 10m bin
            speed_kmph: Average speed during the bin
            end_gps: (lat, lon, alt) at end of bin
            start_gps: (lat, lon, alt) at start of bin
        """
        if self.project is None or not self.running:
            return

        # Determine direction sign for chainage calculation
        # Increasing: chainage goes up (+1), Decreasing: chainage goes down (-1)
        dir_sign = 1 if self.project["direction"] == "Increasing" else -1

        # Calculate chainage for this bin
        start_chain = self.current_chainage_km
        end_chain = start_chain + dir_sign * BIN_SIZE_KM  # ±0.01 km (10m)

        # Record timestamp and GPS data
        ts = now_local_str()
        lat, lon, alt = end_gps
        start_lat, start_lon, _ = start_gps

        # Update telemetry state
        self.last_lat, self.last_lon, self.last_alt = lat, lon, alt
        self.last_speed = speed_kmph
        self.last_distance_m += BIN_SIZE_METERS

        # Add GPS data row to Table 2 (GPS coordinates per 10m bin)
        self.table2_rows.append([
            ts, self.project["nh"], f"{start_chain:.3f}", f"{end_chain:.3f}",
            self.project["direction"], self.project["lane"],
            f"{lat:.6f}", f"{lon:.6f}", f"{alt:.1f}", f"{speed_kmph:.1f}"
        ])

        # Capture timestamped images from all selected cameras
        self.capture_images(
            chainage_label=self.format_chainage_label(start_chain),
            lat=lat, lon=lon, alt=alt
        )

        # Accumulate bin data for measurement window averaging
        self.measure_accum_km += BIN_SIZE_KM
        self.window_buffer.append({
            "S": avgs,
            "speed": speed_kmph,
            "lat": lat,
            "lon": lon,
            "start_lat": start_lat,
            "start_lon": start_lon
        })

        # Check if measurement window is complete
        # (e.g., if measurement section = 1.0 km, flush after 100 bins of 10m each)
        if self.measure_accum_km + 1e-9 >= self.project["meas_km"]:  # Add epsilon to avoid float precision issues
            self.flush_measurement_window(
                final_ts=ts,
                end_lat=lat,
                end_lon=lon,
                dir_sign=dir_sign,
                end_chain=end_chain
            )

        # Update current chainage position
        self.current_chainage_km = end_chain
        self.update_chainage_label(dir_sign * BIN_SIZE_KM)

        # Update real-time graphs
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
        S1,S2,S3,S4,S5,S6 = masked[:6]  # Extract first 6 sensors, ignore S7-S13 for primary metrics

        # Calculate road condition metrics using absolute values (non-negative output)
        # Rutting: Deviation in wheel paths (S1, S2 = left; S5, S6 = right)
        left_rut   = np.nanmean(np.abs([S1, S2]))
        right_rut  = np.nanmean(np.abs([S5, S6]))
        avg_rut    = np.nanmean(np.abs([S1, S2, S5, S6]))

        # IRI (International Roughness Index): Road roughness (S3 = left track; S4 = right track)
        left_iri   = np.nanmean(np.abs([S3]))
        right_iri  = np.nanmean(np.abs([S4]))
        avg_iri    = np.nanmean(np.abs([S3, S4]))

        # Texture: Overall road surface texture using all selected sensors
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

    # ========================================================================
    # SENSOR CALIBRATION
    # ========================================================================

    def open_calibrate(self):
        """
        Open enhanced calibration dialog with connection status indicators.

        New features in v21:
        - Visual connection status (green=connected, red=disconnected)
        - COM port display for each sensor
        - Connection checkboxes to mark sensors as active
        - Live raw readings and calibrated values
        - Structured table layout for better visibility
        """
        win = ctk.CTkToplevel(self)
        win.title("Sensor Calibration & Connection Status")
        win.geometry("1000x700")
        win.transient(self)
        win.grab_set()
        try:
            win.attributes("-topmost", True)
        except Exception:
            pass

        # Main title
        title_frame = ctk.CTkFrame(win)
        title_frame.pack(fill="x", padx=10, pady=10)
        ctk.CTkLabel(
            title_frame,
            text="Sensor Calibration & Connection Status",
            font=ctk.CTkFont(size=18, weight="bold")
        ).pack()
        ctk.CTkLabel(
            title_frame,
            text="Sensors auto-initialized on startup | Use 'Detect Sensors' to re-scan | Green = Live readings",
            font=ctk.CTkFont(size=11),
            text_color="gray"
        ).pack()

        # Scrollable frame for sensor table
        scroll_frame = ctk.CTkScrollableFrame(win, height=450)
        scroll_frame.pack(fill="both", expand=True, padx=10, pady=5)

        # Table header
        header_frame = ctk.CTkFrame(scroll_frame)
        header_frame.pack(fill="x", pady=(0, 5))

        headers = ["Status", "Sensor", "COM Port", "Live RAW", "Calibrated", "Connected"]
        widths = [60, 60, 80, 100, 100, 90]

        for col, (header, width) in enumerate(zip(headers, widths)):
            ctk.CTkLabel(
                header_frame,
                text=header,
                font=ctk.CTkFont(size=12, weight="bold"),
                width=width
            ).grid(row=0, column=col, padx=5, pady=5)

        # Sensor rows - store UI elements for updates
        self.cal_status_indicators = []
        self.cal_com_port_labels = []
        self.cal_raw_labels = []
        self.cal_calibrated_labels = []
        self.sensor_connected_vars = []
        self.sensor_connected_checkboxes = []

        for i in range(SENSORS):
            row_frame = ctk.CTkFrame(scroll_frame)
            row_frame.pack(fill="x", pady=2)

            # Column 0: Status indicator (colored label)
            status_label = ctk.CTkLabel(
                row_frame,
                text="●",
                font=ctk.CTkFont(size=20),
                text_color="red",  # Default to red (disconnected)
                width=widths[0]
            )
            status_label.grid(row=0, column=0, padx=5, pady=5)
            self.cal_status_indicators.append(status_label)

            # Column 1: Sensor ID
            ctk.CTkLabel(
                row_frame,
                text=f"S{i+1}",
                font=ctk.CTkFont(size=12, weight="bold"),
                width=widths[1]
            ).grid(row=0, column=1, padx=5, pady=5)

            # Column 2: COM Port (will be updated after detection)
            port_label = ctk.CTkLabel(
                row_frame,
                text="--",  # Initially unknown until detection
                font=ctk.CTkFont(size=11),
                width=widths[2]
            )
            port_label.grid(row=0, column=2, padx=5, pady=5)
            self.cal_com_port_labels.append(port_label)

            # Column 3: Live RAW reading
            raw_label = ctk.CTkLabel(
                row_frame,
                text="--",
                font=ctk.CTkFont(size=11),
                width=widths[3]
            )
            raw_label.grid(row=0, column=3, padx=5, pady=5)
            self.cal_raw_labels.append(raw_label)

            # Column 4: Calibrated value
            cal_label = ctk.CTkLabel(
                row_frame,
                text="--",
                font=ctk.CTkFont(size=11),
                width=widths[4]
            )
            cal_label.grid(row=0, column=4, padx=5, pady=5)
            self.cal_calibrated_labels.append(cal_label)

            # Column 5: Connected checkbox
            connected_var = ctk.BooleanVar(value=self.sensor_selected[i].get())
            connected_chk = ctk.CTkCheckBox(
                row_frame,
                text="",
                variable=connected_var,
                width=widths[5]
            )
            connected_chk.grid(row=0, column=5, padx=5, pady=5)
            self.sensor_connected_vars.append(connected_var)
            self.sensor_connected_checkboxes.append(connected_chk)

        # Initialize COM port labels from detected ports
        for i in range(SENSORS):
            if i in self.detected_ports:
                self.cal_com_port_labels[i].configure(text=self.detected_ports[i])
            else:
                self.cal_com_port_labels[i].configure(text="--")

        # Update function for live readings
        def tick():
            for i in range(SENSORS):
                raw_val = self.latest_raw[i]
                cal_val = raw_val - self.calib_values[i] if np.isfinite(raw_val) else float('nan')

                # Update raw value display
                if np.isfinite(raw_val):
                    self.cal_raw_labels[i].configure(text=f"{raw_val:.2f}")
                    # Sensor is connected if we're getting valid readings
                    self.cal_status_indicators[i].configure(text_color="green")
                else:
                    self.cal_raw_labels[i].configure(text="--")
                    # Sensor disconnected or no data
                    self.cal_status_indicators[i].configure(text_color="red")

                # Update calibrated value display
                if np.isfinite(cal_val):
                    self.cal_calibrated_labels[i].configure(text=f"{cal_val:.2f}")
                else:
                    self.cal_calibrated_labels[i].configure(text="--")

            # Schedule next update
            if win.winfo_exists():
                win.after(100, tick)

        # Start live update loop
        tick()

        # Button frame
        button_frame = ctk.CTkFrame(win)
        button_frame.pack(fill="x", padx=10, pady=10)

        def detect_sensors_clicked():
            """Scan for and detect connected sensors."""
            # Show detection in progress
            detect_btn.configure(text="Detecting...", state="disabled")
            win.update()

            # Run detection
            count = self.detect_and_update_sensors()

            # Update COM port labels with detected ports
            for i in range(SENSORS):
                if i in self.detected_ports:
                    self.cal_com_port_labels[i].configure(text=self.detected_ports[i])
                else:
                    self.cal_com_port_labels[i].configure(text="--")

            # Show results
            detect_btn.configure(text=f"Re-Scan Sensors ({count} found)", state="normal")

            if count > 0:
                messagebox.showinfo(
                    "Sensor Detection Complete",
                    f"Successfully detected {count} sensor(s).\n\n"
                    f"Detected sensors will show green status when readings are received."
                )
            else:
                messagebox.showwarning(
                    "No Sensors Detected",
                    "No sensors were detected.\n\n"
                    "Please check:\n"
                    "- Sensors are powered on\n"
                    "- USB/Serial connections are secure\n"
                    "- COM port drivers are installed"
                )

        def capture_offsets():
            """Capture current sensor readings as calibration offsets."""
            self.calib_values = [
                self.latest_raw[i] if np.isfinite(self.latest_raw[i]) else 0.0
                for i in range(SENSORS)
            ]
            messagebox.showinfo(
                "Calibration Complete",
                "Calibration offsets captured from current readings.\n\n"
                "Calibrated values should now be near 0.0 for all connected sensors."
            )

        def apply_and_close():
            """Apply connection selections and close window."""
            # Update sensor selection based on connection checkboxes
            for i in range(SENSORS):
                self.sensor_selected[i].set(self.sensor_connected_vars[i].get())

            win.destroy()
            self.rebuild_graphs()

        # Re-Scan Sensors button
        detect_btn = ctk.CTkButton(
            button_frame,
            text="Re-Scan Sensors",
            command=detect_sensors_clicked,
            height=35,
            font=ctk.CTkFont(size=13, weight="bold"),
            fg_color="#2196F3"  # Blue to stand out
        )
        detect_btn.pack(side="left", padx=5, expand=True, fill="x")

        # Capture Calibration button
        ctk.CTkButton(
            button_frame,
            text="Capture Calibration Offsets",
            command=capture_offsets,
            height=35,
            font=ctk.CTkFont(size=13)
        ).pack(side="left", padx=5, expand=True, fill="x")

        # Apply & Close button
        ctk.CTkButton(
            button_frame,
            text="Apply & Close",
            command=apply_and_close,
            height=35,
            font=ctk.CTkFont(size=13)
        ).pack(side="left", padx=5, expand=True, fill="x")

    def get_calibration_value(self): return self.calib_values

    # ========================================================================
    # CALIBRATION READER MANAGEMENT
    # ========================================================================

    def start_calibration_reader(self):
        """
        Start the calibration reader thread for live sensor readings.

        This automatically initializes sensors and allows the calibration window
        to show live sensor data even when the main data collection is not running.
        """
        if self.calibration_reader is not None:
            return  # Already running

        try:
            # Create and start calibration reader (auto-initializes sensors)
            self.calibration_reader = CalibrationReader(self.set_latest_raw)
            self.calibration_reader.start()

            # Update detected ports from initialized sensors
            if self.calibration_reader.sensor_port_map:
                self.detected_ports = self.calibration_reader.sensor_port_map.copy()

            logger.info("Calibration reader started successfully")
        except Exception as e:
            logger.error(f"Failed to start calibration reader: {e}")
            self.calibration_reader = None

    def detect_and_update_sensors(self):
        """
        Scan for connected sensors and update calibration reader.

        Returns:
            int: Number of sensors detected
        """
        try:
            # Run sensor detection
            logger.info("Starting sensor detection...")
            detected = detect_sensors()

            # Update detected sensor mappings
            self.detected_sensors = detected
            self.detected_ports = {idx: port for idx, (port, _) in detected.items()}

            # Update calibration reader with detected sensors
            if self.calibration_reader:
                self.calibration_reader.update_sensors(detected)

            logger.info(f"Detected {len(detected)} sensor(s)")
            return len(detected)

        except Exception as e:
            logger.error(f"Sensor detection failed: {e}")
            return 0

    def set_latest_raw(self, raw_vals):
        """
        Update the latest raw sensor values.

        Called by CalibrationReader or SensorWorker to update real-time readings.

        Args:
            raw_vals (list): List of raw sensor values
        """
        self.latest_raw = list(raw_vals)

    # ========================================================================
    # MISC METHODS
    # ========================================================================

    def new_project(self):
        if messagebox.askyesno("New Project", "This will reset configuration. Continue?"): self.on_reset()

    def open_manual(self): messagebox.showinfo("User Manual", "Hook your manual PDF path in open_manual().")

    def on_close(self):
        """Clean up resources and close application."""
        try:
            # Stop calibration reader
            if self.calibration_reader:
                try:
                    self.calibration_reader.stop()
                    self.calibration_reader.join(timeout=2.0)
                except Exception as e:
                    logger.error(f"Error stopping calibration reader: {e}")
                self.calibration_reader = None

            # Stop sensor worker
            if self.sensor_worker:
                try:
                    self.sensor_worker.stop()
                    self.sensor_worker.join(timeout=1.0)
                except Exception:
                    pass
                self.sensor_worker = None

            # Close video writers and cameras
            self.close_writers()
            self.stop_cameras()
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
