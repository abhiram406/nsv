# line 100 added a helper function for chainage distance format
# line 53 chainging the bin_size_ meter constant to 5 meters from 10 meters and  to update all the parameters every 5 meters
# line 54 bin_size_km to 0.005 to update all the parameters every 5 meters
# line 1368 updating sensor worker tick logic for every 5 meters
# line 122 - 203 modified draw_header_to_size method: 
    #increased column width to accomodate more text
    #increased header height
    #adjusted chainage units
    #retained old method until testing
# converted all distances from m to km. Moved upto 2 decimal places

import os, time, threading, queue
from datetime import datetime
import logging

import numpy as np
import cv2
from collections import deque
from MEDAQLib import MEDAQLib, ME_SENSOR, ERR_CODE

# Serial communication for GPS and DMI
try:
    import serial
    SERIAL_OK = True
except ImportError:
    SERIAL_OK = False
    logging.warning("pyserial not installed. GPS and DMI functionality will not work. Install with: pip install pyserial")

# ---------------------------
# Logging Setup
# ---------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ---------------------------
# Layout / Runtime constants
# ---------------------------
# ---------------------------
# Layout / Runtime constants
# ---------------------------
LEFT_WIDTH = 300
WINDOW_GEOM = "1680x960"

CAM_IDS = [0, 1, 2, 3]
CAM_NAMES = {0: "Front", 1: "Left", 2: "Right", 3: "Back"}
CAM_FOLDERS = {0: "Front_camera", 1: "Left_camera", 2: "Right_camera", 3: "Pavement_camera1"}
CAM_URLS = [
    "rtsp://192.168.1.101:554/stream0?username=admin&password=E10ADC3949BA59ABBE56E057F20F883E",
    "rtsp://admin:admin123@192.168.1.102:554/stream1",
    "rtsp://user:admin123@192.168.1.100:554/stream1",
    "rtsp://admin:admin123@192.168.1.103:554/stream1"
]
PREVIEW_RES = (360, 202)
RECORD_RES = (1280, 720)
CAM_FPS = 25
PREVIEW_FPS = 15
TOF_HZ = 100
BIN_SIZE_METERS = 5.0
BIN_SIZE_KM = 0.005
NEON_GREEN = "#39ff14"
HEADER_COLOR = (255, 0, 0)
TEXT_COLOR = (255, 255, 255)
FONT = cv2.FONT_HERSHEY_SIMPLEX
BG_COLOR = "#0b0f14"
GRID_COLOR = "#263238"
SPINE_COLOR = "#607d8b"
HEADER_MAX_FRAC = 0.35
HEADER_LINES = 6
LOGO_PATH = "cropped-Roadworks-LogoR-768x576-1.jpeg"
MAX_QUEUE_SIZE = 5  # ✅ Per camera queue size (total = 5 × 4 = 20 frames)

# Constants for laser sensors
SENSORS = 6
#LASER_COM_PORTS = [f"COM{i}" for i in [10, 6, 7, 9, 8, 11]]
LASER_COM_PORTS = [f"COM{i}" for i in [10,12,7,5,13,14]]
EXPECTED_BLOCK_SIZE = 1000

PLOT_MAX_POINTS = 10

# GPS Configuration (for position only)
GPS_COM_PORT = "COM30"
GPS_BAUDRATE = 57600

# DMI Encoder Configuration (for distance and speed measurement)
DMI_COM_PORT = "COM4"          # ⚠️ CHANGE TO YOUR ARDUINO PORT
DMI_BAUDRATE = 115200
DMI_WHEEL_CIRCUMFERENCE_MM = 2220  # ⚠️ MEASURE YOUR WHEEL (roll test)
DMI_PULSES_PER_REV = 2500      # From encoder datasheet
MIN_SPEED_THRESHOLD_KMH = 0.5  # Minimum speed to register movement

# ---------------------------
# Helpers
# ---------------------------
now_local_str = lambda: datetime.now().strftime("%Y-%m-%d %H:%M:%S")
ts_for_path = lambda: datetime.now().strftime("%Y-%m-%d_%H-%M-%S")


def is_valid_reading(val):
    """Check if a sensor reading is valid (not NaN, inf, or extremely large)."""
    if not np.isfinite(val):
        return False
    if abs(val) > 1e100:  # Catch extremely large numbers like 1e360
        return False
    return True
def format_absolute_chainage(init_chain_km, current_chain_km):
    """
    Always return absolute chainage like: 102.000 Km
    """
    if init_chain_km is None or current_chain_km is None:
        return "--"
    return f"{current_chain_km:.2f}"



def make_testcard(w, h, text="NO CAMERA"):
    img = np.zeros((h, w, 3), dtype=np.uint8)
    bars = [(255, 255, 255), (255, 255, 0), (0, 255, 255), (0, 255, 0), (255, 0, 255), (255, 0, 0), (0, 0, 255)]
    bw = max(1, w // len(bars))
    for i, c in enumerate(bars):
        img[:, i * bw:(i + 1) * bw] = c
    cv2.putText(img, text, (10, h // 2), FONT, 0.9, (0, 0, 0), 3, cv2.LINE_AA)
    cv2.putText(img, text, (10, h // 2), FONT, 0.9, (255, 255, 255), 1, cv2.LINE_AA)
    return img

def draw_header_to_size(frame, ctx, size):
    w, h = size

    # Normalize frame
    if frame is None or frame.size == 0:
        frame = np.zeros((h, w, 3), dtype=np.uint8)
    else:
        frame = cv2.resize(frame, (w, h))

    # Font scaling by resolution
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
    pad_top = max(0, int(4 * (h / 480)))
    pad_bottom = max(10, int(8 * (h / 480)))

    needed_h = pad_top + HEADER_LINES * line_h + pad_bottom
    max_h = int(h * HEADER_MAX_FRAC)
    header_h = min(needed_h, max_h)

    # Draw header background
    cv2.rectangle(frame, (0, 0), (w, header_h), HEADER_COLOR, -1)

    # Header content
    col1 = [
        f"Project ID: {ctx.get('survey', '--')}",
        f"Survey Loc: {ctx.get('survey_location', '--')}",
        f"NH Number: {ctx.get('nh', '--')}",
        f"Old NH Num: {ctx.get('oldnh', '--')}",
        f"Section: {ctx.get('section_code', '--')}",
        f"Direction: {ctx.get('direction', '--')}"
    ]
    col2 = [
        f"Latitude: {ctx.get('lat', '--')}",
        f"Longitude: {ctx.get('lon', '--')}",
        f"Altitude: {ctx.get('alt', '--')} m",
        f"Dist: {ctx.get('distance', '--')} Km, Chainage: {ctx.get('chainage', '---')} Km, Speed: {ctx.get('speed', '--')} Km/h",
        f"LRP: {ctx.get('lrp', '--')} Km"
    ]
    col3 = [
        f"Date: {ctx.get('date', '--')}",
        f"Time: {ctx.get('time', '--')}"
    ]

    cols = [col1, col2, col3]

    # -------- COLUMN SPACING LOGIC --------
    col_gap = max(20, int(150 * (h / 480)))
    usable_w = w - 2 * pad_x - 2 * col_gap
    col_w = usable_w // 3
    # -------------------------------------

    for ci, lines in enumerate(cols):
        x = pad_x + ci * (col_w + col_gap)
        y = pad_top

        for line in lines:
            y += line_h
            cv2.putText(
                frame,
                line,
                (x, y),
                FONT,
                font_scale,
                TEXT_COLOR,
                thickness,
                cv2.LINE_AA
            )

    return frame


def draw_header_to_size_old(frame, ctx, size):
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

    cv2.rectangle(frame, (0, 0), (w, header_h), HEADER_COLOR, -1)

    col1 = [
        f"Project ID: {ctx.get('survey', '--')}",
        f"Survey Loc: {ctx.get('survey_location', '--')}",
        f"NH Number: {ctx.get('nh', '--')}",
        f"Old NH Num: {ctx.get('oldnh', '--')}",
        f"Section: {ctx.get('section_code', '--')}",
        f"Direction: {ctx.get('direction', '--')}"
    ]
    col2 = [
        f"Latitude: {ctx.get('lat', '--')}",
        f"Longitude: {ctx.get('lon', '--')}",
        f"Altitude: {ctx.get('alt', '--')} m",
        f"Dist: {ctx.get('distance', '--')} m | Chain: {ctx.get('chainage', '--')} Km | Speed: {ctx.get('speed', '--')} km/h",
        f"LRP: {ctx.get('lrp', '--')} Km"
    ]
    col3 = [
        f"Date: {ctx.get('date', '--')}",
        f"Time: {ctx.get('time', '--')}"
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
                    values[i] = np.array(np.nanmedian(transferred_data[1]))
        except Exception as e:
            logger.debug(f"Sensor {i + 1} read exception: {str(e)}")
    return values


def initialize_lasers_with_timeout(retry_count=2):
    """
    Initialize lasers with per-port timeout and retry mechanism.

    Args:
        retry_count: Number of retry attempts for failed sensors (default: 2)

    Returns:
        tuple: (sensors list, initialization_report dict)
    """
    sensors = [None] * SENSORS
    initialized_count = 0
    timeout_sec = 3.0

    # Track initialization results
    init_report = {
        'total': len(LASER_COM_PORTS),
        'successful': 0,
        'failed': 0,
        'details': {}
    }

    def try_init(port, idx, result_queue, attempt=1):
        sensor = None
        try:
            sensor = MEDAQLib.CreateSensorInstance(ME_SENSOR.SENSOR_ILD1320)
            if sensor.iSensor == 0:
                result_queue.put((idx, None, f"Create failed (attempt {attempt})", port))
                return

            sensor.SetParameterString("IP_Interface", "RS232")
            sensor.SetParameterString("IP_Port", port)
            sensor.SetParameterInt("IP_AutomaticMode", 3)
            sensor.SetParameterInt("IP_EnableLogging", 0)
            sensor.SetParameterInt("IP_Timeout", int(timeout_sec * 1000))

            sensor.OpenSensor()
            err = sensor.GetLastError()
            if err != ERR_CODE.ERR_NOERROR:
                error_msg = f"Open failed: {err} (attempt {attempt})"
                logger.warning(f"Sensor {idx + 1} on {port}: {error_msg}")
                result_queue.put((idx, None, error_msg, port))
                sensor.CloseSensor()
                sensor.ReleaseSensorInstance()
                return

            result_queue.put((idx, sensor, f"OK (attempt {attempt})", port))
            logger.info(f"✓ Sensor {idx + 1} initialized on {port} (attempt {attempt})")

        except Exception as e:
            result_queue.put((idx, None, f"Exception: {str(e)} (attempt {attempt})", port))
            if sensor:
                try:
                    sensor.CloseSensor()
                except:
                    pass
                try:
                    sensor.ReleaseSensorInstance()
                except:
                    pass

    # Initial attempt for all sensors
    logger.info("=" * 60)
    logger.info("LASER SENSOR INITIALIZATION - ATTEMPT 1")
    logger.info("=" * 60)

    result_queue = queue.Queue()
    threads = []

    for i, port in enumerate(LASER_COM_PORTS):
        if i >= SENSORS:
            break
        t = threading.Thread(target=try_init, args=(port, i, result_queue, 1), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join(timeout=timeout_sec + 1.0)

    # Collect results from first attempt
    failed_sensors = []
    while not result_queue.empty():
        idx, sensor, msg, port = result_queue.get()
        if sensor is not None:
            sensors[idx] = sensor
            initialized_count += 1
            init_report['details'][idx] = {'status': 'SUCCESS', 'port': port, 'message': msg}
        else:
            failed_sensors.append((idx, port))
            init_report['details'][idx] = {'status': 'FAILED', 'port': port, 'message': msg}
            logger.warning(f"✗ Sensor {idx + 1} ({port}) failed: {msg}")

    # Retry failed sensors
    for retry in range(1, retry_count + 1):
        if not failed_sensors:
            break

        logger.info("=" * 60)
        logger.info(f"LASER SENSOR INITIALIZATION - RETRY {retry}")
        logger.info(f"Retrying {len(failed_sensors)} failed sensors...")
        logger.info("=" * 60)

        threads = []
        retry_queue = queue.Queue()

        for idx, port in failed_sensors:
            logger.info(f"Retrying Sensor {idx + 1} on {port}...")
            t = threading.Thread(target=try_init, args=(port, idx, retry_queue, retry + 1), daemon=True)
            t.start()
            threads.append(t)

        for t in threads:
            t.join(timeout=timeout_sec + 1.0)

        # Check retry results
        new_failed = []
        while not retry_queue.empty():
            idx, sensor, msg, port = retry_queue.get()
            if sensor is not None:
                sensors[idx] = sensor
                initialized_count += 1
                init_report['details'][idx] = {'status': 'SUCCESS', 'port': port, 'message': msg}
                logger.info(f"✓ Sensor {idx + 1} recovered on retry {retry}!")
            else:
                new_failed.append((idx, port))
                init_report['details'][idx] = {'status': 'FAILED', 'port': port, 'message': msg}

        failed_sensors = new_failed

    # Final report
    init_report['successful'] = initialized_count
    init_report['failed'] = len(LASER_COM_PORTS) - initialized_count

    logger.info("=" * 60)
    logger.info("LASER SENSOR INITIALIZATION COMPLETE")
    logger.info(f"✓ Successful: {init_report['successful']}/{init_report['total']}")
    logger.info(f"✗ Failed: {init_report['failed']}/{init_report['total']}")

    if init_report['failed'] > 0:
        logger.warning("Failed sensors:")
        for idx, details in init_report['details'].items():
            if details['status'] == 'FAILED':
                logger.warning(f"  Sensor {idx + 1} ({details['port']}): {details['message']}")

    logger.info("=" * 60)

    return sensors, init_report


def cleanup_lasers(sensors):
    """Close and release all laser sensor instances."""
    for i, sensor in enumerate(sensors):
        if sensor is not None:
            try:
                sensor.CloseSensor()
                sensor.ReleaseSensorInstance()
                logger.info(f"Sensor {i + 1} closed")
            except Exception as e:
                logger.error(f"Sensor {i + 1} cleanup failed: {str(e)}")



# ---------------------------
# GPS Reader (Emlid Reach M2) - Position Only
# ---------------------------
class GPSReader:
    """Reads NMEA sentences from Emlid Reach M2 GPS receiver over serial.
    Used for position (lat/lon/alt) only. Distance and speed come from DMI encoder."""

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
        """Get current GPS position (lat, lon, alt). Speed NOT used - comes from DMI."""
        with self.lock:
            # Don't log warnings if serial is closed (during shutdown)
            if time.time() - self.last_update > 2.0:
                if self.serial and self.serial.is_open:
                    logger.warning("GPS data stale")
                return (None, None, None)

            if self.fix_quality == 0:
                logger.warning("No GPS fix")
                return (None, None, None)

            lat = self.lat if self.lat is not None else None
            lon = self.lon if self.lon is not None else None
            alt = self.alt if self.alt is not None else None

            return (lat, lon, alt)

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
                'fix_quality': self.fix_quality,
                'satellites': self.num_satellites,
                'hdop': self.hdop
            }


# ---------------------------
# GPS Worker Thread
# ---------------------------
class GPSWorker(threading.Thread):
    """Background thread that continuously reads GPS data for position."""

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
# DMI Encoder Tracker - Distance and Speed Measurement
# ---------------------------
class DMIEncoderTracker:
    """Tracks distance and speed using wheel encoder (replaces GPS distance tracking).
    
    Reads pulse counts from Arduino-connected rotary encoder.
    Calculates distance based on wheel circumference and pulse resolution.
    Calculates speed using pulse rate over time with smoothing.
    """

    def __init__(self, port=DMI_COM_PORT, baudrate=DMI_BAUDRATE,
                 wheel_circumference_mm=DMI_WHEEL_CIRCUMFERENCE_MM,
                 pulses_per_rev=DMI_PULSES_PER_REV):
        self.port = port
        self.baudrate = baudrate
        self.wheel_circumference_mm = wheel_circumference_mm
        self.pulses_per_rev = pulses_per_rev
        
        # Quadrature encoding gives 4x resolution
        self.total_pulses_per_rev = pulses_per_rev * 4
        
        # Calculate mm per pulse
        self.mm_per_pulse = wheel_circumference_mm / self.total_pulses_per_rev
        
        self.serial = None
        self.total_distance_km = 0.0
        self.total_pulses = 0
        self.last_pulse_count = 0
        self.last_timestamp_ms = 0
        
        # Speed calculation
        self.speed_kmh = 0.0
        self.speed_buffer = deque(maxlen=5)  # Smooth over 10 samples
        self.direction = 0  # -1 = reverse, 0 = stopped, 1 = forward
        
        # Statistics
        self.pulse_updates = 0
        self.distance_updates = 0
        
        self.lock = threading.Lock()
        self.connected = False

    def connect(self):
        """Open serial connection to DMI encoder (Arduino)."""
        if not SERIAL_OK:
            logger.error("pyserial not installed. Cannot connect to DMI encoder.")
            return False
        
        try:
            self.serial = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=0.1,  # 100ms timeout for responsive reads (vs test script's 1s)
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE
            )
            
            # Wait for Arduino initialization message
            time.sleep(2.0)
            
            # Clear any buffered data
            self.serial.reset_input_buffer()
            
            self.connected = True
            logger.info(f"DMI Encoder connected on {self.port} at {self.baudrate} baud")
            logger.info(f"  Wheel circumference: {self.wheel_circumference_mm} mm")
            logger.info(f"  Pulses per revolution: {self.pulses_per_rev} (x4 = {self.total_pulses_per_rev})")
            logger.info(f"  Resolution: {self.mm_per_pulse:.3f} mm/pulse")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to DMI encoder on {self.port}: {e}")
            self.connected = False
            return False

    def disconnect(self):
        """Close serial connection."""
        if self.serial and self.serial.is_open:
            try:
                self.serial.close()
                self.connected = False
                logger.info("DMI encoder disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting DMI encoder: {e}")

    def update(self):
    
        if not self.serial or not self.serial.is_open:
            return
        
        try:
            if self.serial.in_waiting > 0:
                line = self.serial.readline().decode('ascii', errors='ignore').strip()
                
                if line.startswith('DMI_READY') or not line:
                    return
                
                parts = line.split(',')
                if len(parts) >= 2:
                    try:
                        timestamp_ms = int(parts[0])
                        pulse_count = int(parts[1])
                        
                        with self.lock:
                            pulse_delta = pulse_count - self.last_pulse_count
                            time_delta_s = (timestamp_ms - self.last_timestamp_ms) / 1000.0
                            
                            # Handle first reading or Arduino restart
                            if self.last_timestamp_ms == 0 or time_delta_s < 0 or time_delta_s > 2.0:
                                self.last_pulse_count = pulse_count
                                self.last_timestamp_ms = timestamp_ms
                                return
                            
                            # ✅ FIX 1: Reject bad time deltas
                            if time_delta_s < 0.008 or time_delta_s > 0.050:
                                logger.debug(f"DMI time delta out of range: {time_delta_s*1000:.1f}ms")
                                return
                            
                            # Update counters and distance
                            self.last_pulse_count = pulse_count
                            self.last_timestamp_ms = timestamp_ms
                            self.total_pulses += abs(pulse_delta)
                            self.pulse_updates += 1
                            
                            if pulse_delta != 0:
                                distance_delta_mm = pulse_delta * self.mm_per_pulse
                                distance_delta_m = distance_delta_mm / 1000.0
                                self.total_distance_km += distance_delta_m
                                self.distance_updates += 1
                                
                                # Update direction
                                if pulse_delta > 0:
                                    self.direction = 1
                                elif pulse_delta < 0:
                                    self.direction = -1
                            
                            # ✅ NEW ADAPTIVE SPEED CALCULATION
                            if pulse_delta != 0:
                                # Calculate raw speed
                                distance_delta_mm = abs(pulse_delta) * self.mm_per_pulse
                                distance_delta_m = distance_delta_mm / 1000.0
                                speed_m_s = distance_delta_m / time_delta_s
                                speed_kmh_raw = speed_m_s * 3.6
                                
                                # ✅ FIX 2: Aggressive spike rejection (before filtering)
                                if speed_kmh_raw > 120:  # Reject unrealistic speeds
                                    logger.warning(f"DMI spike rejected: {speed_kmh_raw:.1f} km/h")
                                    return
                                
                                # ✅ FIX 3: Reject sudden impossible changes
                                if hasattr(self, 'last_accepted_speed'):
                                    speed_change = abs(speed_kmh_raw - self.last_accepted_speed)
                                    max_change_per_update = 15  # Max 15 km/h change in 10ms
                                    
                                    if speed_change > max_change_per_update:
                                        logger.warning(f"DMI sudden change rejected: "
                                                    f"{self.last_accepted_speed:.1f} → {speed_kmh_raw:.1f}")
                                        return
                                
                                self.last_accepted_speed = speed_kmh_raw
                                
                                # ✅ FIX 4: ADAPTIVE FILTERING
                                # Use exponential weighted moving average (EWMA) instead of buffer
                                # This gives fast response with smooth display
                                
                                if self.speed_kmh == 0:
                                    # First reading or restart from zero
                                    self.speed_kmh = speed_kmh_raw
                                else:
                                    # Detect if we're accelerating/braking vs steady state
                                    speed_diff = abs(speed_kmh_raw - self.speed_kmh)
                                    
                                    if speed_diff > 5:
                                        # Rapid change detected (acceleration/braking)
                                        # Use fast response (alpha = 0.5)
                                        alpha = 0.5  # 50% new, 50% old → fast response
                                    else:
                                        # Steady state - use more smoothing
                                        # Use slow response (alpha = 0.2)
                                        alpha = 0.2  # 20% new, 80% old → smooth
                                    
                                    # EWMA formula: new = alpha × raw + (1-alpha) × old
                                    self.speed_kmh = alpha * speed_kmh_raw + (1 - alpha) * self.speed_kmh
                                
                                # ✅ Optional: Keep small buffer for additional smoothing
                                # But use it only for display, not for calculation
                                self.speed_buffer.append(self.speed_kmh)
                            
                            # ✅ FIX 5: FAST decay to zero when stopped
                            elif time_delta_s > 0.1:  # No pulses for 100ms
                                if self.speed_kmh > 0.5:
                                    # Decay quickly to zero (not instant to avoid jitter)
                                    self.speed_kmh *= 0.3  # 70% reduction each update
                                else:
                                    # Close enough to zero
                                    self.speed_kmh = 0.0
                                
                                self.speed_buffer.clear()
                                self.direction = 0
                    
                    except (ValueError, IndexError) as e:
                        logger.debug(f"Failed to parse DMI data '{line}': {e}")
        
        except Exception as e:
            logger.warning(f"DMI encoder update error: {e}")

    def get_distance_km(self):
        """Get total distance traveled in meters."""
        with self.lock:
            return self.total_distance_km/1000

    def get_speed_kmh(self):
        """Get current speed in km/h."""
        with self.lock:
            return self.speed_kmh

    def get_direction(self):
        """Get current direction: -1 = reverse, 0 = stopped, 1 = forward."""
        with self.lock:
            return self.direction

    def reset(self):
        """Reset distance counter."""
        with self.lock:
            self.total_distance_km = 0.0
            self.total_pulses = 0
            self.last_pulse_count = 0
            self.last_timestamp_ms = 0
            self.speed_kmh = 0.0
            self.speed_buffer.clear()
            self.direction = 0
            self.pulse_updates = 0
            self.distance_updates = 0
            logger.info("DMI encoder reset")
            
            # Send reset command to Arduino if connected
            if self.serial and self.serial.is_open:
                try:
                    self.serial.write(b'R\n')
                    time.sleep(0.1)
                    self.serial.reset_input_buffer()
                except Exception as e:
                    logger.warning(f"Failed to send reset to DMI encoder: {e}")

    def is_connected(self):
        """Check if DMI encoder is connected."""
        return self.connected and self.serial and self.serial.is_open

    def get_statistics(self):
        """Get tracking statistics."""
        with self.lock:
            # Calculate update rate based on pulse updates over time
            if self.pulse_updates > 0 and self.last_timestamp_ms > 0:
                elapsed_s = self.last_timestamp_ms / 1000.0
                update_rate = self.pulse_updates / max(0.1, elapsed_s) if elapsed_s > 0 else 0
            else:
                update_rate = 0
            
            direction_text = {-1: "Reverse", 0: "Stopped", 1: "Forward"}.get(self.direction, "Unknown")
            
            return {
                'total_distance_km': self.total_distance_km,
                'total_pulses': self.total_pulses,
                'current_speed_kmh': self.speed_kmh,
                'direction': self.direction,
                'direction_text': direction_text,
                'pulse_updates': self.pulse_updates,
                'distance_updates': self.distance_updates,
                'update_rate_hz': update_rate,
                'connected': self.connected
            }


# ---------------------------
# DMI Encoder Worker Thread
# ---------------------------
class DMIEncoderWorker(threading.Thread):
    """Background thread that continuously updates DMI encoder data."""

    def __init__(self, dmi_encoder_tracker):
        super().__init__(daemon=True)
        self.dmi_encoder_tracker = dmi_encoder_tracker
        self.stop_event = threading.Event()

    def run(self):
        logger.info("DMI encoder worker thread started")
        while not self.stop_event.is_set():
            try:
                # Check if serial data is available before updating
                if self.dmi_encoder_tracker.serial and self.dmi_encoder_tracker.serial.in_waiting > 0:
                    self.dmi_encoder_tracker.update()
                else:
                    # Tiny sleep when no data to prevent 100% CPU usage
                    time.sleep(0.001)  # 1ms - nearly instant but prevents CPU spinning
            except Exception as e:
                if not self.stop_event.is_set():
                    logger.error(f"DMI encoder worker error: {e}")
        logger.info("DMI encoder worker thread stopped")

    def stop(self):
        self.stop_event.set()


# ---------------------------
# Camera Worker
# ---------------------------
# ---------------------------
# Camera Worker - FIXED VERSION
# ---------------------------
class CameraWorker(threading.Thread):
    """
    Fixed Camera Worker with accurate frame rate recording.
    
    Key fixes:
    1. Frame-count based recording (not wall-clock)
    2. Minimal buffer flushing
    3. Frame duplication when falling behind
    4. Proper recording state management
    """
    
    def __init__(self, cam_id, out_queue, get_writer, get_overlay_ctx,
                 preview_size, record_size, cam_fps=25, preview_fps=15,
                 max_queue_size=5, cam_url=None, cam_names=None):
        super().__init__(daemon=True)
        self.cam_id = cam_id
        self.cam_url = cam_url
        self.out_queue = out_queue
        self.get_writer = get_writer
        self.get_overlay_ctx = get_overlay_ctx
        self.preview_size = preview_size
        self.record_size = record_size
        self.cam_fps = cam_fps
        self.preview_fps = preview_fps
        self.max_queue_size = max_queue_size
        self.cam_names = cam_names or {}
        
        self.stop_event = threading.Event()
        self.cap = None
        self.latest_preview_frame = None
        self.latest_record_frame = None
        self.latest_frame_ts = 0.0
        self.last_frame_time = 0.0
        self.frame_interval = 1.0 / preview_fps
        self.consecutive_fails = 0
        self.is_connected = False
        self.connection_attempts = 0
        self.max_connection_attempts = 3
        self.reconnect_interval = 5.0
        self.last_reconnect_attempt = 0
        
        # Frame history for synchronized capture
        self.frame_history = deque(maxlen=10)
        self.frame_history_lock = threading.Lock()
        
        # ===== FIXED: Video recording state =====
        self.frames_written = 0
        self.recording_start_time = 0.0
        self.write_start_time = 0.0
        self.last_good_frame = None
        self.last_good_frame_time = 0.0
        
        # Frame staleness tracking
        self.max_frame_age_seconds = 0.3  # Reduced from 0.5
        self.frame_age_history = deque(maxlen=100)
        self.stale_frame_count = 0
        self.total_frame_count = 0
        
        # Statistics
        self._frames_duplicated = 0
        self._last_stats_time = 0

    def reset_recording_state(self):
        """Reset recording state - call this when starting a new recording."""
        self.frames_written = 0
        self.recording_start_time = 0.0
        self.write_start_time = 0.0
        self.last_good_frame = None
        self.last_good_frame_time = 0.0
        self._frames_duplicated = 0
        logger.info(f"Camera {self.cam_id}: Recording state reset")

    def _try_open(self, backend_flag):
        """Try to open camera with specific backend."""
        try:
            import os
            if backend_flag == cv2.CAP_FFMPEG:
                os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
                    "rtsp_transport;tcp|"
                    "fflags;nobuffer|"
                    "probesize;32768|"
                    "analyzeduration;100000|"
                    "stimeout;5000000"
                )

            cap = cv2.VideoCapture(self.cam_url, backend_flag)
            cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 3000)
            cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 2000)

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
                cam_name = self.cam_names.get(self.cam_id, f"CAM{self.cam_id}")
                logger.info(f"Camera {self.cam_id} ({cam_name}) connected with backend {backend}")
                return True

        self.is_connected = False
        return False

    def _make_testcard(self, text="NO CAMERA"):
        """Create a test card image when camera is disconnected."""
        w, h = self.record_size
        img = np.zeros((h, w, 3), dtype=np.uint8)
        bars = [(255, 255, 255), (255, 255, 0), (0, 255, 255), (0, 255, 0), 
                (255, 0, 255), (255, 0, 0), (0, 0, 255)]
        bw = max(1, w // len(bars))
        for i, c in enumerate(bars):
            img[:, i * bw:(i + 1) * bw] = c
        font = cv2.FONT_HERSHEY_SIMPLEX
        cv2.putText(img, text, (10, h // 2), font, 0.9, (0, 0, 0), 3, cv2.LINE_AA)
        cv2.putText(img, text, (10, h // 2), font, 0.9, (255, 255, 255), 1, cv2.LINE_AA)
        return img

    def run(self):
        self._open_cap()

        while not self.stop_event.is_set():
            # Handle disconnected state
            if self.cap is None or not self.is_connected:
                cam_name = self.cam_names.get(self.cam_id, f"CAM {self.cam_id}")
                testcard = self._make_testcard(f"{cam_name} DISCONNECTED")
                ctx = self.get_overlay_ctx()
                # Note: You'll need to import/define draw_header_to_size
                testcard_overlay = draw_header_to_size(testcard, ctx, self.record_size)
                #testcard_overlay = testcard  # Simplified for this example
                
                preview = cv2.resize(testcard_overlay, self.preview_size)
                preview_rgb = cv2.cvtColor(preview, cv2.COLOR_BGR2RGB)

                self.latest_preview_frame = preview_rgb
                self.latest_record_frame = testcard_overlay
                self.latest_frame_ts = time.time()

                if self.out_queue.qsize() < self.max_queue_size:
                    self.out_queue.put((self.cam_id, preview_rgb))

                time.sleep(1)
                self._open_cap()
                continue

            loop_start_time = time.time()
            
            # ===== FIXED: Minimal buffer flush =====
            # Only flush if it's been a while since last frame (buffer might be stale)
            time_since_last_frame = loop_start_time - self.latest_frame_ts
            if time_since_last_frame > 0.15:  # 150ms threshold
                self.cap.grab()  # Flush just 1 frame
            
            # ===== FIXED: Simple frame read =====
            ret, frame = self.cap.read()
            ok = ret and frame is not None and frame.size > 0
            
            if not ok:
                # One retry after short delay
                time.sleep(0.008)
                ret, frame = self.cap.read()
                ok = ret and frame is not None and frame.size > 0
            
            read_duration = time.time() - loop_start_time

            # Handle read failure
            if not ok or frame is None:
                self.consecutive_fails += 1
                if self.consecutive_fails > 10:
                    logger.error(f"Camera {self.cam_id} too many failures, reconnecting...")
                    self.cap.release()
                    self.cap = None
                    self.is_connected = False
                    self.consecutive_fails = 0
                    time.sleep(0.5)
                    continue
                time.sleep(0.02)
                continue
            
            self.consecutive_fails = 0
            frame_capture_time = time.time()

            # Process frame
            ctx = self.get_overlay_ctx()
            preview = cv2.resize(frame, self.preview_size)
            # Note: Replace with your draw_header_to_size function
            record = draw_header_to_size(frame, ctx, self.record_size)
            #record = cv2.resize(frame, self.record_size)  # Simplified
            preview_rgb = cv2.cvtColor(preview, cv2.COLOR_BGR2RGB)
            
            # Store frame in history for synchronized capture
            with self.frame_history_lock:
                self.frame_history.append({
                    'frame': record.copy(),
                    'timestamp': frame_capture_time
                })
            
            # Update latest frame references
            self.latest_preview_frame = preview_rgb
            self.latest_record_frame = record
            self.latest_frame_ts = frame_capture_time

            # Send to preview queue (rate limited)
            if loop_start_time - self.last_frame_time >= self.frame_interval:
                if self.out_queue.qsize() < self.max_queue_size:
                    self.out_queue.put((self.cam_id, preview_rgb))
                    self.last_frame_time = loop_start_time

            # ===== FIXED: Frame-accurate video recording =====
            writer = self.get_writer(self.cam_id)
            if writer is not None:
                current_time = time.time()
                
                # Initialize recording state on first frame
                if self.recording_start_time == 0.0:
                    self.recording_start_time = current_time
                    self.write_start_time = current_time
                    self.frames_written = 0
                    self.last_good_frame = record.copy()
                    self.last_good_frame_time = current_time
                    self._frames_duplicated = 0
                    logger.info(f"Camera {self.cam_id}: Recording started")
                
                # Calculate expected frame count based on elapsed time
                elapsed = current_time - self.recording_start_time
                expected_frames = int(elapsed * self.cam_fps)
                frames_behind = expected_frames - self.frames_written
                
                if frames_behind > 0:
                    # Determine which frame to write
                    frame_age = current_time - self.latest_frame_ts
                    
                    if frame_age < self.max_frame_age_seconds:
                        # Current frame is fresh - use it
                        frame_to_write = record
                        self.last_good_frame = record.copy()
                        self.last_good_frame_time = current_time
                    elif self.last_good_frame is not None:
                        # Current frame is stale - use last good frame
                        frame_to_write = self.last_good_frame
                        self._frames_duplicated += 1
                    else:
                        # No good frame available
                        frame_to_write = None
                    
                    if frame_to_write is not None:
                        # Write frames to catch up (max 3 at a time to prevent runaway)
                        frames_to_write = min(frames_behind, 3)
                        
                        for _ in range(frames_to_write):
                            writer.write(frame_to_write)
                            self.frames_written += 1
                        
                        # Log statistics every second
                        if current_time - self._last_stats_time >= 1.0:
                            actual_fps = self.frames_written / elapsed if elapsed > 0 else 0
                            dup_pct = (self._frames_duplicated / max(1, self.frames_written)) * 100
                            logger.info(f"🎬 Camera {self.cam_id}: {self.frames_written} frames in {elapsed:.1f}s "
                                       f"= {actual_fps:.1f} FPS (target: {self.cam_fps}, duplicated: {dup_pct:.1f}%)")
                            self._last_stats_time = current_time

            # Maintain target loop timing
            elapsed = time.time() - loop_start_time
            target_loop_time = 1.0 / self.cam_fps
            sleep_time = max(0.001, target_loop_time - elapsed)
            time.sleep(sleep_time)

        # Cleanup
        if self.cap is not None:
            self.cap.release()
            logger.info(f"Camera {self.cam_id} released")
            
        # Log final recording stats
        if self.frames_written > 0:
            elapsed = time.time() - self.recording_start_time
            actual_fps = self.frames_written / elapsed if elapsed > 0 else 0
            logger.info(f"Camera {self.cam_id}: Recording finished - {self.frames_written} frames "
                       f"in {elapsed:.1f}s = {actual_fps:.1f} FPS")

    def get_frame_closest_to_time(self, target_time, max_age=0.5):
        """Get frame closest to target timestamp for synchronized capture."""
        with self.frame_history_lock:
            if not self.frame_history:
                return None, None
            
            best_frame = None
            best_diff = float('inf')
            
            for frame_data in self.frame_history:
                time_diff = abs(frame_data['timestamp'] - target_time)
                if time_diff < best_diff:
                    best_diff = time_diff
                    best_frame = frame_data
            
            if best_frame and best_diff < max_age:
                return best_frame['frame'].copy(), best_diff
            return None, None

    def stop(self):
        """Signal the worker to stop."""
        self.stop_event.set()

    def get_recording_stats(self):
        """Get current recording statistics."""
        elapsed = time.time() - self.recording_start_time if self.recording_start_time > 0 else 0
        actual_fps = self.frames_written / elapsed if elapsed > 0 else 0
        
        return {
            'frames_written': self.frames_written,
            'elapsed_seconds': elapsed,
            'actual_fps': actual_fps,
            'target_fps': self.cam_fps,
            'frames_duplicated': self._frames_duplicated,
            'duplication_rate': (self._frames_duplicated / max(1, self.frames_written)) * 100
        }


# ---------------------------
# Sensor Worker
# ---------------------------
class SensorWorker(threading.Thread):
    def __init__(self, ui_queue, paused_event, calib_getter, ten_m_callback,
                 sensor_selected_getter, raw_setter, gps_reader, dmi_encoder_tracker,
                 sensors=None):
        super().__init__(daemon=True)
        self.ui_queue = ui_queue
        self.paused_event = paused_event
        self.calib_getter = calib_getter
        self.ten_m_callback = ten_m_callback
        self.sensor_selected_getter = sensor_selected_getter
        self.raw_setter = raw_setter
        self.gps_reader = gps_reader
        self.dmi_encoder_tracker = dmi_encoder_tracker
        self.stop_event = threading.Event()
        self.prev_distance_km = 0.0
        self.bin_start_lat = None
        self.bin_start_lon = None
        self.last_valid_raw = [float('nan')] * SENSORS  # Store last valid readings

        if sensors is not None:
            self.sensors = sensors
            self._owns_sensors = False
        else:
            self.sensors, _ = initialize_lasers_with_timeout()  # Ignore report in worker
            self._owns_sensors = True

        self.reset()

    def reset(self):
        self.last_t = time.time()
        self.next_five_m_edge = 0.005
        self.bin_buffers = [[] for _ in range(SENSORS)]
        self.bin_start_lat = None
        self.bin_start_lon = None
        self.prev_distance_km = 0.0

    def run(self):
        logger.info("Sensor worker thread started")
        
        while not self.stop_event.is_set():
            t = time.time()
            if self.paused_event.is_set():
                time.sleep(0.01)
                continue

            # Get position from GPS (lat/lon/alt)
            lat, lon, alt = self.gps_reader.get_position()
            
            # Get distance and speed from DMI encoder
            current_distance_km = self.dmi_encoder_tracker.get_distance_km()
            current_speed_kmh = self.dmi_encoder_tracker.get_speed_kmh()

            if lat is None or lon is None:
                logger.warning("No GPS fix available - using last valid position or continuing without position")
                # Continue with distance measurement even without GPS

            raw = read_laser_values(self.sensors)

            # ROBUST HANDLING: Replace invalid readings with last valid reading
            for i in range(SENSORS):
                if is_valid_reading(raw[i]):
                    self.last_valid_raw[i] = raw[i]
                else:
                    # Use last valid reading if available
                    if is_valid_reading(self.last_valid_raw[i]):
                        raw[i] = self.last_valid_raw[i]
                        logger.debug(f"Sensor {i + 1}: Using last valid reading {raw[i]:.2f}")
                    else:
                        raw[i] = float('nan')

            try:
                self.raw_setter(raw)
            except Exception:
                pass

            calib_vals = self.calib_getter()

            vals = [raw[i] - calib_vals[i] if is_valid_reading(raw[i]) else float('nan') for i in range(SENSORS)]

            # CRITICAL: Use absolute values to ensure magnitude (not signed displacement)
            # This ensures rutting and IRI calculations are accurate
            for i, v in enumerate(vals):
                abs_val = abs(v) if is_valid_reading(v) else float('nan')
                self.bin_buffers[i].append(abs_val)

            if self.bin_start_lat is None:
                self.bin_start_lat = lat
                self.bin_start_lon = lon

            # Send only GPS data to queue (DMI is read directly by UI for zero lag)
            self.ui_queue.put({
                "type": "telemetry",
                "lat": lat, "lon": lon, "alt": alt,
            })

            if current_distance_km >= self.next_five_m_edge:
                # ✅ NEW: Record exact timestamp when boundary crossed
                boundary_time = time.time()
                
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
                # ✅ NEW: Pass boundary_time to callback
                self.ten_m_callback(avgs, current_speed_kmh, (lat, lon, alt), 
                                   (start_lat, start_lon, alt), boundary_time)
                self.next_five_m_edge += 0.005
                self.bin_start_lat = lat
                self.bin_start_lon = lon

            #time.sleep(max(0.0, 1.0/TOF_HZ - (time.time()-t)))
            time.sleep(0.01)
        
        logger.info("Sensor worker thread stopped")

    def stop(self):
        self.stop_event.set()
        if getattr(self, "_owns_sensors", False):
            cleanup_lasers(self.sensors)