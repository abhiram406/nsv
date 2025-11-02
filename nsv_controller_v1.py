# nsv_basic_ui.py
# Basic Network Survey Vehicle UI (customtkinter)
# - 3 camera feeds
# - 5 TOF sensors @100 Hz (simulated)
# - DMI speed/distance (simulated)
# - GPS (simulated)
# - Averages TOF data every 10 m and appends a row to the table

import time
import math
import threading
import queue
import random
import cv2
import numpy as np
import customtkinter as ctk
from tkinter import ttk

# -----------------------------
# Config
# -----------------------------
BIN_METERS = 10.0            # distance interval for averaging
TOF_HZ = 100                 # sensor sample rate
CAM_INDEXES = [0, 1, 2]      # change per your machine
USE_CSV_LOG = False
CSV_PATH = "nsv_log.csv"

# -----------------------------
# Utility: Test card for cameras
# -----------------------------
def make_testcard(w=400, h=225, text="NO CAMERA"):
    img = np.zeros((h, w, 3), dtype=np.uint8)
    # color bars
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
# Camera thread
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
                # push testcard periodically
                while not self.stop_event.is_set():
                    frame = make_testcard(self.width, self.height, f"CAM {self.cam_index} NOT FOUND")
                    self.out_queue.put((self.cam_index, frame))
                    time.sleep(0.1)
                return

            # Try to set a reasonable size
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
# Replace functions in SensorWorker with your real reads
# -----------------------------
def sim_speed_kmph(t):
    # make a smooth speed profile between 25–55 km/h
    base = 40 + 12*math.sin(2*math.pi*t/20.0) + 3*random.uniform(-1,1)
    return max(0.0, base)

def sim_gps():
    # return (lat, lon, alt) with light jitter
    lat = 17.385 + random.uniform(-1e-4, 1e-4)  # Hyderabad-ish
    lon = 78.486 + random.uniform(-1e-4, 1e-4)
    alt = 505 + random.uniform(-1.0, 1.0)
    return lat, lon, alt

def sim_tof_values():
    # five channels in mm, with small noise
    base = 80  # pretend the frame is 80 cm above road
    return [base*10 + random.gauss(0, 5) for _ in range(5)]  # values in mm

# -----------------------------
# Sensor worker: samples TOF @100Hz, integrates distance from speed, bins every 10 m
# -----------------------------
class SensorWorker(threading.Thread):
    def __init__(self, ui_queue):
        super().__init__(daemon=True)
        self.ui_queue = ui_queue
        self.stop_event = threading.Event()

        # state
        self.distance_m = 0.0
        self.last_t = time.time()
        self.next_bin_edge = BIN_METERS

        # buffers for bin-averaging
        self.bin_buffers = [[] for _ in range(5)]

        # CSV
        if USE_CSV_LOG:
            import csv
            with open(CSV_PATH, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["timestamp", "distance_m", "speed_kmph",
                            "lat", "lon", "alt", "TOF1_mm", "TOF2_mm", "TOF3_mm", "TOF4_mm", "TOF5_mm"])

    def run(self):
        while not self.stop_event.is_set():
            t = time.time()

            # --- DMI / Speed (replace with your DMI pulse integration) ---
            speed_kmph = sim_speed_kmph(t)
            speed_mps = speed_kmph / 3.6
            dt = t - self.last_t
            self.last_t = t
            self.distance_m += speed_mps * dt

            # --- GPS (replace with real GPS read) ---
            lat, lon, alt = sim_gps()

            # --- TOF read @100 Hz (replace with real reads) ---
            tof = sim_tof_values()  # list of 5 floats (mm)
            for i, v in enumerate(tof):
                self.bin_buffers[i].append(v)

            # push live telemetry to UI (throttled)
            self.ui_queue.put({
                "type": "telemetry",
                "speed_kmph": speed_kmph,
                "distance_m": self.distance_m,
                "lat": lat, "lon": lon, "alt": alt,
                "tof": tof,
            })

            # Check for 10 m bin complete
            if self.distance_m >= self.next_bin_edge:
                avg = [float(np.nanmean(buf)) if len(buf) else float("nan") for buf in self.bin_buffers]
                # clear buffers for next bin
                self.bin_buffers = [[] for _ in range(5)]

                self.ui_queue.put({
                    "type": "bin",
                    "bin_end_m": self.next_bin_edge,
                    "avg_tof_mm": avg
                })
                self.next_bin_edge += BIN_METERS

            # CSV logging (optional)
            if USE_CSV_LOG:
                import csv
                with open(CSV_PATH, "a", newline="") as f:
                    w = csv.writer(f)
                    w.writerow([t, self.distance_m, speed_kmph, lat, lon, alt] + tof)

            # 100 Hz
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
        self.title("NSV – Basic Controller")
        self.geometry("1300x760")

        self.ui_queue = queue.Queue()

        # Layout: Left main, Right sidebar
        self.grid_columnconfigure(0, weight=1)
        self.grid_columnconfigure(1, weight=0)
        self.grid_rowconfigure(0, weight=1)

        self.build_left()
        self.build_right()

        # Launch workers
        self.cam_queue = queue.Queue()
        self.cams = [CameraWorker(i, self.cam_queue) for i in CAM_INDEXES]
        for c in self.cams:
            c.start()

        self.sensor_worker = SensorWorker(self.ui_queue)
        self.sensor_worker.start()

        # Timers
        self.after(30, self.poll_cam_frames)
        self.after(60, self.poll_ui_queue)

        self.protocol("WM_DELETE_WINDOW", self.on_close)

    # -------- Left area --------
    def build_left(self):
        left = ctk.CTkFrame(self, corner_radius=12)
        left.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        left.grid_columnconfigure((0,1,2), weight=1)
        left.grid_rowconfigure(1, weight=1)

        # Camera strip
        cam_bar = ctk.CTkFrame(left)
        cam_bar.grid(row=0, column=0, columnspan=3, sticky="ew", padx=10, pady=(10,5))
        cam_bar.grid_columnconfigure((0,1,2), weight=1)

        self.cam_labels = []
        for i in range(3):
            lbl = ctk.CTkLabel(cam_bar, text=f"Camera {i}", compound="top")
            lbl.grid(row=0, column=i, padx=6, pady=6, sticky="ew")
            self.cam_labels.append(lbl)

        # Table area
        table_frame = ctk.CTkFrame(left)
        table_frame.grid(row=1, column=0, columnspan=3, sticky="nsew", padx=10, pady=(5,10))
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
        side = ctk.CTkFrame(self, width=280, corner_radius=12)
        side.grid(row=0, column=1, sticky="ns", padx=(0,10), pady=10)
        side.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(side, text="Survey Data", font=ctk.CTkFont(size=18, weight="bold")).grid(
            row=0, column=0, padx=10, pady=(12,6), sticky="w"
        )

        # Speed
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

        # Controls
        btn_row = ctk.CTkFrame(side)
        btn_row.grid(row=9, column=0, padx=12, pady=10, sticky="ew")
        btn_row.grid_columnconfigure((0,1), weight=1)
        self.start_btn = ctk.CTkButton(btn_row, text="Start", command=self.on_start)
        self.start_btn.grid(row=0, column=0, padx=6, pady=6, sticky="ew")
        self.stop_btn = ctk.CTkButton(btn_row, text="Stop", command=self.on_stop)
        self.stop_btn.grid(row=0, column=1, padx=6, pady=6, sticky="ew")

        self.status_lbl = ctk.CTkLabel(side, text="Status: running")
        self.status_lbl.grid(row=10, column=0, padx=12, pady=(0,12), sticky="w")

        ctk.CTkLabel(side, text="Averages every 10 m\n(TOF @100 Hz)", justify="left").grid(row=11, column=0, padx=12, pady=(0,12), sticky="w")

    # -------- Polling --------
    def poll_cam_frames(self):
        try:
            while True:
                cam_idx, frame = self.cam_queue.get_nowait()
                # convert to PhotoImage
                rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                from PIL import Image, ImageTk  # pillow used only in UI conversion
                im = Image.fromarray(rgb)
                imgtk = ImageTk.PhotoImage(image=im)
                # store reference on label to avoid GC
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
                    self.speed_bar.set(min(1.0, spd/120.0))  # scale to 0–120
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

    # -------- Controls --------
    def on_start(self):
        self.status_lbl.configure(text="Status: running")

    def on_stop(self):
        self.status_lbl.configure(text="Status: paused")
        # (In a real system you might pause workers; here we just display)

    def on_close(self):
        for c in self.cams:
            c.stop()
        self.sensor_worker.stop()
        self.destroy()

if __name__ == "__main__":
    app = NSVApp()
    app.mainloop()
