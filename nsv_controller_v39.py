# line 729 Fix Camera Overlay (images + videos) for chainage format
# line 1568 Fix Telemetry Panel (Right UI) for chainage format
# line 1600 fix image file names for chainage format
# line 755  removing the decimal places for speed
# line 556 fix dmi status update for speed format
# line 585 changing speed format for dmi statistics popup
# line 1038 , updating the texture for now we have both left and right texture and we take average in main export
# line 1254 , updating the texture for now we have both left and right texture and we take average in flush measurement
# converted all distances from m to km. Moved upto 2 decimal places


import os, time, math, threading, queue
from datetime import datetime

import customtkinter as ctk
import tkinter as tk
from tkinter import filedialog, messagebox

import numpy as np
import cv2
from collections import deque

from NSV import *

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



# ---------------------------
# Main App
# ---------------------------
class NSVApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("blue")
        self.title("NSV Controller - DMI Edition")
        self.geometry(WINDOW_GEOM)

        self.grid_columnconfigure(0, weight=0, minsize=LEFT_WIDTH)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.ui_queue = queue.Queue()
        self.cam_queue = queue.Queue(maxsize=MAX_QUEUE_SIZE * len(CAM_IDS))
        self.cameras = {}
        self.sensor_worker = None

        # Initialize devices in proper order: Lasers → GPS → DMI → Cameras
        logger.info("=" * 60)
        logger.info("INITIALIZING DEVICES (DMI Edition)")
        logger.info("=" * 60)

        # 1. Initialize Lasers FIRST
        logger.info("1/4: Initializing Laser Sensors...")
        try:
            self.sensors, self.sensor_init_report = initialize_lasers_with_timeout()
            connected_lasers = sum(1 for s in self.sensors if s is not None)
            logger.info(f"✓ Lasers initialized: {connected_lasers}/{SENSORS} sensors connected")

            # Store which sensors failed for UI display
            self.failed_sensors = [idx for idx, details in self.sensor_init_report['details'].items()
                                   if details['status'] == 'FAILED']
        except Exception as e:
            logger.error(f"Laser initialization failed: {e}")
            self.sensors = [None] * SENSORS
            self.sensor_init_report = {'total': SENSORS, 'successful': 0, 'failed': SENSORS, 'details': {}}
            self.failed_sensors = list(range(SENSORS))

        # 2. Initialize GPS (position only)
        logger.info("2/4: Initializing GPS (position only)...")
        self.gps_reader = GPSReader(GPS_COM_PORT, GPS_BAUDRATE)
        self.gps_worker = None
        if self.gps_reader.connect():
            self.gps_worker = GPSWorker(self.gps_reader)
            self.gps_worker.start()
            logger.info("✓ GPS initialized and worker started")
        else:
            logger.error("❌ GPS initialization failed")

        # 3. Initialize DMI Encoder (distance and speed measurement)
        logger.info("3/4: Initializing DMI Encoder (distance and speed)...")
        self.dmi_encoder_tracker = DMIEncoderTracker(
            DMI_COM_PORT, DMI_BAUDRATE,
            DMI_WHEEL_CIRCUMFERENCE_MM, DMI_PULSES_PER_REV
        )
        self.dmi_encoder_worker = None
        if self.dmi_encoder_tracker.connect():
            self.dmi_encoder_worker = DMIEncoderWorker(self.dmi_encoder_tracker)
            self.dmi_encoder_worker.start()
            logger.info("✓ DMI encoder initialized and worker started")
        else:
            logger.error("❌ DMI encoder initialization failed")

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
        self.last_distance_km = 0.0
        self.latest_raw = [float('nan')] * SENSORS

        self.camera_status = {cam_id: False for cam_id in CAM_IDS}
        self.laser_status = [False] * SENSORS
        self.gps_status = False

        self.selected_sensor_indices = [i for i, v in enumerate(self.sensor_selected) if v.get()]
        self.graph_data = [deque(maxlen=PLOT_MAX_POINTS) for _ in range(SENSORS)]
        self.last_valid_masked = np.array([0.0] * SENSORS)  # For filling missing export data

        self.build_menu()
        self.build_layout()

        self.left.configure(width=LEFT_WIDTH)
        self.left.grid_propagate(False)

        # Update initial status indicators based on actual initialization
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
        """Update device status indicators based on actual initialization."""
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

        # Update DMI status
        if self.dmi_encoder_worker and self.dmi_encoder_worker.is_alive():
            self.dmi_status_light.configure(text_color="#fbc02d")  # yellow (connected but waiting for data)
            self.dmi_status_text.configure(text="Connected (waiting for data)")
        else:
            self.dmi_status_light.configure(text_color="#d32f2f")  # red
            self.dmi_status_text.configure(text="Not connected")

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
        sensors_menu.add_command(label="DMI Encoder Statistics", command=self.show_dmi_stats)
        menubar.add_cascade(label="Sensors", menu=sensors_menu)
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="User Manual", command=self.open_manual)
        menubar.add_cascade(label="Help", menu=help_menu)
        self.config(menu=menubar)

    # ---------- Layout ----------
    def build_layout(self):
        self.left = ctk.CTkFrame(self, corner_radius=12)
        self.left.grid(row=0, column=0, sticky="nsw", padx=(8, 4), pady=8)
        self.left.grid_columnconfigure(0, weight=0, minsize=160)
        self.left.grid_columnconfigure(1, weight=1)
        self.build_left_panel(self.left)

        self.merged_right = ctk.CTkFrame(self, corner_radius=12)
        self.merged_right.grid(row=0, column=1, sticky="nsew", padx=(4, 8), pady=8)
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
        pad = {"padx": 6, "pady": 4}
        ctk.CTkLabel(f, text="Project Setup", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0,
                                                                                             columnspan=2, sticky="w",
                                                                                             **pad)

        def add_row(r, label, widget):
            ctk.CTkLabel(f, text=label, anchor="e").grid(row=r, column=0, sticky="e", **pad)
            widget.grid(row=r, column=1, sticky="ew", **pad)

        entry_w = 60
        self.ent_survey = ctk.CTkEntry(f, width=entry_w)
        self.ent_survey.insert(0, "Hyderabad to Vijayawada")
        self.ent_survey_location = ctk.CTkEntry(f, width=entry_w)
        self.ent_survey_location.insert(0, "Hyderabad")
        self.ent_client = ctk.CTkEntry(f, width=entry_w)
        self.ent_client.insert(0, "NHAI")
        self.ent_nh = ctk.CTkEntry(f, width=entry_w)
        self.ent_nh.insert(0, "NH9")
        self.ent_oldnh = ctk.CTkEntry(f, width=entry_w)
        self.ent_oldnh.insert(0, "NH9")
        self.ent_section = ctk.CTkEntry(f, width=entry_w)
        self.ent_section.insert(0, "1")
        self.ent_lrp = ctk.CTkEntry(f, width=entry_w)
        self.ent_lrp.insert(0, "00.000")
        add_row(1, "Survey ID", self.ent_survey)
        add_row(2, "Survey Location", self.ent_survey_location)
        add_row(3, "Client Name/ID", self.ent_client)
        add_row(4, "NH Number", self.ent_nh)
        add_row(5, "Old NH Number", self.ent_oldnh)
        add_row(6, "Section Code", self.ent_section)
        add_row(7, "LRP (Km)", self.ent_lrp)

        self.direction_var = ctk.StringVar(value="Increasing")
        self.direction_dd = ctk.CTkOptionMenu(f, values=["Increasing", "Decreasing"], variable=self.direction_var,
                                              command=self.on_direction_select, width=entry_w)
        add_row(8, "Direction", self.direction_dd)

        self.lane_var = ctk.StringVar(value="L1")
        self.lane_dd = ctk.CTkOptionMenu(f, values=[f"L{i}" for i in range(1, 11)], variable=self.lane_var,
                                         width=entry_w)
        add_row(9, "Lane No.", self.lane_dd)

        self.cmb_pave = ctk.CTkComboBox(f, values=["Concrete", "Bituminous"], width=entry_w)
        self.cmb_pave.set("Bituminous")
        add_row(10, "Pavement Type", self.cmb_pave)

        self.ent_chainage = ctk.CTkEntry(f, width=entry_w)
        self.ent_chainage.insert(0, "100")
        self.ent_section_km = ctk.CTkEntry(f, width=entry_w)
        self.ent_section_km.insert(0, "0.1")
        add_row(11, "Initial Chainage (Km)", self.ent_chainage)
        add_row(12, "Measurement \nSection (Km)", self.ent_section_km)

        row0 = 13
        ctk.CTkLabel(f, text="Export Cameras", font=ctk.CTkFont(size=14, weight="bold")).grid(row=row0, column=0,
                                                                                              columnspan=2, sticky="w",
                                                                                              padx=8, pady=(12, 4))
        cam_row = row0 + 1
        self.cam_checkboxes = []
        for i, cam_id in enumerate(CAM_IDS):
            chk = ctk.CTkCheckBox(f, text=CAM_NAMES[cam_id], variable=self.cam_selected[cam_id])
            chk.grid(row=cam_row, column=i % 2, sticky="w", padx=2, pady=2)
            if i % 2 == 1: cam_row += 1
            self.cam_checkboxes.append(chk)

        self.btn_set_specs = ctk.CTkButton(f, text="Set Project Specifications", command=self.set_project_specs)
        self.btn_set_specs.grid(row=cam_row + 1, column=0, columnspan=2, sticky="ew", padx=8, pady=(16, 8))
        self.lbl_proj_status = ctk.CTkLabel(f, text="Project not configured.")
        self.lbl_proj_status.grid(row=cam_row + 2, column=0, columnspan=2, sticky="w", padx=8, pady=(0, 8))

        self.left_widgets = [self.ent_survey, self.ent_survey_location, self.ent_client, self.ent_nh, self.ent_oldnh,
                             self.ent_section,
                             self.ent_lrp, self.direction_dd, self.lane_dd, self.cmb_pave, self.ent_chainage,
                             self.ent_section_km, self.btn_set_specs]

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
            ax = fig.add_subplot(rows, cols, j + 1, facecolor=BG_COLOR)
            ax.set_title(f"S{sens_idx + 1}", fontsize=10, color="white", pad=10)

            # Remove axes as requested
            ax.set_xlabel("")
            ax.set_ylabel("")
            ax.set_xticks([])
            ax.set_yticks([])

            # Add border to separate plots
            for spine in ax.spines.values():
                spine.set_color(SPINE_COLOR)
                spine.set_linewidth(2)

            ax.grid(True, color=GRID_COLOR, linewidth=0.6)
            self.axes.append(ax)
            self.lines.append(ax.plot([], [], NEON_GREEN, linewidth=2)[0])

        self.canvas_mpl = FigureCanvasTkAgg(fig, master=self.graph_container)
        self.canvas_mpl.draw()
        self.canvas_mpl.get_tk_widget().pack(fill="both", expand=True)

    # ---------- Right Panel (Telemetry & Controls) ----------
    def build_right_panel(self, parent):
        f = parent
        pad = {"padx": 10, "pady": 4}

        # Device Status Section
        ctk.CTkLabel(f, text="Device Status", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0,
                                                                                             sticky="w", **pad)

        status_frame = ctk.CTkFrame(f)
        status_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=4)
        status_frame.grid_columnconfigure(1, weight=1)

        # GPS Status (row 0) - Position only
        ctk.CTkLabel(status_frame, text="GPS:").grid(row=0, column=0, sticky="w", padx=(5, 10))
        self.gps_status_light = ctk.CTkLabel(status_frame, text="●", font=ctk.CTkFont(size=20), text_color="#d32f2f")
        self.gps_status_light.grid(row=0, column=1, sticky="w")
        self.gps_status_text = ctk.CTkLabel(status_frame, text="Position only", font=ctk.CTkFont(size=10))
        self.gps_status_text.grid(row=0, column=2, sticky="w", padx=(5, 5))

        # DMI Status (row 1) - Distance and speed
        ctk.CTkLabel(status_frame, text="DMI:").grid(row=1, column=0, sticky="w", padx=(5, 10))
        self.dmi_status_light = ctk.CTkLabel(status_frame, text="●", font=ctk.CTkFont(size=20), text_color="#d32f2f")
        self.dmi_status_light.grid(row=1, column=1, sticky="w")
        self.dmi_status_text = ctk.CTkLabel(status_frame, text="Not connected", font=ctk.CTkFont(size=10))
        self.dmi_status_text.grid(row=1, column=2, sticky="w", padx=(5, 5))

        # Camera Status (row 2)
        ctk.CTkLabel(status_frame, text="Cameras:").grid(row=2, column=0, sticky="w", padx=(5, 10))
        self.cam_status_light = ctk.CTkLabel(status_frame, text="●", font=ctk.CTkFont(size=20), text_color="#d32f2f")
        self.cam_status_light.grid(row=2, column=1, sticky="w")
        self.cam_status_text = ctk.CTkLabel(status_frame, text="0/4 Connected", font=ctk.CTkFont(size=10))
        self.cam_status_text.grid(row=2, column=2, sticky="w", padx=(5, 5))

        # Laser Sensors Status (row 3)
        ctk.CTkLabel(status_frame, text="Lasers:").grid(row=3, column=0, sticky="w", padx=(5, 10))
        self.laser_status_light = ctk.CTkLabel(status_frame, text="●", font=ctk.CTkFont(size=20), text_color="#d32f2f")
        self.laser_status_light.grid(row=3, column=1, sticky="w")
        self.laser_status_text = ctk.CTkLabel(status_frame, text="Not initialized", font=ctk.CTkFont(size=10))
        self.laser_status_text.grid(row=3, column=2, sticky="w", padx=(5, 5))

        # Telemetry Section
        ctk.CTkLabel(f, text="Telemetry", font=ctk.CTkFont(size=14, weight="bold")).grid(row=2, column=0, sticky="w",
                                                                                         **pad)

        chip_frame = ctk.CTkFrame(f, fg_color="transparent")
        chip_frame.grid(row=3, column=0, sticky="w", **pad)
        self.chip = ctk.CTkLabel(chip_frame, text="  ", width=16, height=16, corner_radius=8)
        self.chip.grid(row=0, column=0, padx=(0, 6))
        self.set_status_color("red")
        self.lbl_status = ctk.CTkLabel(chip_frame, text="Status: stopped")
        self.lbl_status.grid(row=0, column=1)

        grid = ctk.CTkFrame(f)
        grid.grid(row=4, column=0, sticky="ew", padx=10, pady=(4, 4))
        for i in range(2): grid.grid_columnconfigure(i, weight=1)
        self.var_speed = ctk.StringVar(value="0.0 km/h")
        self.var_dist = ctk.StringVar(value="0.0 m")
        self.var_chain = ctk.StringVar(value="-- + 0.000 Km")
        self.var_lat = ctk.StringVar(value="--")
        self.var_lon = ctk.StringVar(value="--")
        self.var_alt = ctk.StringVar(value="-- m")
        
        # Updated labels to clarify data sources
        ctk.CTkLabel(grid, text="Speed (DMI):").grid(row=0, column=0, sticky="w", padx=(0, 6))
        ctk.CTkLabel(grid, textvariable=self.var_speed).grid(row=0, column=1, sticky="w")
        ctk.CTkLabel(grid, text="Distance (DMI):").grid(row=1, column=0, sticky="w", padx=(0, 6))
        ctk.CTkLabel(grid, textvariable=self.var_dist).grid(row=1, column=1, sticky="w")
        ctk.CTkLabel(grid, text="Chainage:").grid(row=2, column=0, sticky="w", padx=(0, 6))
        ctk.CTkLabel(grid, textvariable=self.var_chain).grid(row=2, column=1, sticky="w")
        ctk.CTkLabel(grid, text="Latitude (GPS):").grid(row=3, column=0, sticky="w", padx=(0, 6))
        ctk.CTkLabel(grid, textvariable=self.var_lat).grid(row=3, column=1, sticky="w")
        ctk.CTkLabel(grid, text="Longitude (GPS):").grid(row=4, column=0, sticky="w", padx=(0, 6))
        ctk.CTkLabel(grid, textvariable=self.var_lon).grid(row=4, column=1, sticky="w")
        ctk.CTkLabel(grid, text="Altitude (GPS):").grid(row=5, column=0, sticky="w", padx=(0, 6))
        ctk.CTkLabel(grid, textvariable=self.var_alt).grid(row=5, column=1, sticky="w")

        ctk.CTkLabel(f, text="Speed Progress:").grid(row=5, column=0, sticky="w", **pad)
        self.speed_progress = ctk.CTkProgressBar(f, mode='determinate', width=200)
        self.speed_progress.grid(row=6, column=0, sticky="ew", padx=10, pady=4)
        self.speed_progress.set(0)

        self.btn_toggle_dir = ctk.CTkButton(f, text="Toggle Direction", command=self.toggle_direction)
        self.btn_toggle_dir.grid(row=7, column=0, sticky="ew", padx=10, pady=4)

        btns = ctk.CTkFrame(f)
        btns.grid(row=8, column=0, sticky="ew", padx=10, pady=(8, 6))
        btns.grid_rowconfigure((0, 1), weight=1)
        btns.grid_columnconfigure((0, 1, 2), weight=1)

        # Initialize buttons
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
        """Update GPS status indicator - check serial connection first."""
        
        # Check if GPS worker is running
        if not (hasattr(self, 'gps_worker') and self.gps_worker and self.gps_worker.is_alive()):
            self.gps_status = False
            self.gps_status_light.configure(text_color="#d32f2f")  # Red
            self.gps_status_text.configure(text="Worker not running")
            return
        
        # Check if serial is connected
        if not (self.gps_reader.serial and self.gps_reader.serial.is_open):
            self.gps_status = False
            self.gps_status_light.configure(text_color="#d32f2f")  # Red
            self.gps_status_text.configure(text="Disconnected")
            return
        
        # Check if we have a GPS fix
        lat, lon, alt = self.gps_reader.get_position()
        fix_info = self.gps_reader.get_fix_info()

        if lat is not None and lon is not None:
            # Connected with valid position - green
            self.gps_status = True
            self.gps_status_light.configure(text_color="#43a047")  # Green
            
            fix_text = fix_info['fix_type']
            if fix_info['satellites'] > 0:
                fix_text += f" ({fix_info['satellites']} sats)"
            self.gps_status_text.configure(text=fix_text)
        else:
            # Connected but no fix - yellow
            self.gps_status = False
            self.gps_status_light.configure(text_color="#fbc02d")  # Yellow
            self.gps_status_text.configure(text=f"Waiting for fix ({fix_info['satellites']} sats)")

    def update_dmi_status(self):
        """Update DMI encoder status indicator - green when connected and receiving data."""
        if hasattr(self, 'dmi_encoder_tracker') and self.dmi_encoder_tracker:
            if self.dmi_encoder_tracker.is_connected():
                stats = self.dmi_encoder_tracker.get_statistics()
                distance_km = stats['total_distance_km']
                speed_kmh = stats['current_speed_kmh']
                pulse_updates = stats['pulse_updates']
                direction = stats['direction']
                direction_text = stats['direction_text']
                
                # Green if connected and receiving data (pulse updates > 0)
                if pulse_updates > 0:
                    self.dmi_status_light.configure(text_color="#43a047")  # Green - connected and working
                    
                    # Show current status in text with direction
                    if speed_kmh > MIN_SPEED_THRESHOLD_KMH:
                        self.dmi_status_text.configure(
                            text=f"{direction_text} ({distance_km:.2f}Km, {int(round(speed_kmh))} Km/h)"

                        )
                    else:
                        self.dmi_status_text.configure(
                            text=f"Stationary ({distance_km:.2f}Km)"
                        )
                else:
                    # Connected but no data yet - yellow (waiting for first pulses)
                    self.dmi_status_light.configure(text_color="#fbc02d")  # Yellow
                    self.dmi_status_text.configure(text="Connected (waiting for data)")
            else:
                self.dmi_status_light.configure(text_color="#d32f2f")  # Red
                self.dmi_status_text.configure(text="Not connected")
        else:
            self.dmi_status_light.configure(text_color="#d32f2f")  # Red
            self.dmi_status_text.configure(text="Not initialized")

    def show_dmi_stats(self):
        """Show DMI encoder statistics."""
        stats = self.dmi_encoder_tracker.get_statistics()
        fix_info = self.gps_reader.get_fix_info()

        msg = "DMI Encoder Statistics\n\n"
        msg += f"Total Distance: {stats['total_distance_km']:.2f} Km\n"
        msg += f"Total Pulses: {stats['total_pulses']}\n"
        msg += f"Current Speed: {int(round(stats['current_speed_kmh']))} km/h\n"

        msg += f"Direction: {stats['direction_text']}\n"
        msg += f"Pulse Updates: {stats['pulse_updates']}\n"
        msg += f"Distance Updates: {stats['distance_updates']}\n"
        msg += f"Update Rate: {stats['update_rate_hz']:.1f} Hz\n"
        msg += f"Connection: {'Active' if stats['connected'] else 'Disconnected'}\n\n"
        msg += "GPS Fix Information (Position Only)\n"
        msg += f"Fix Type: {fix_info['fix_type']}\n"
        msg += f"Satellites: {fix_info['satellites']}\n"
        msg += f"HDOP: {fix_info['hdop']:.1f}\n"

        messagebox.showinfo("DMI Statistics", msg)

    def update_camera_status(self):
        """Update camera status indicators - check is_connected flag."""
        connected_count = 0
        for cam_id, worker in self.cameras.items():
            if worker and worker.is_alive() and worker.is_connected:
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
        if hasattr(self, 'sensors') and self.sensors:
            connected_count = sum(1 for s in self.sensors if s is not None)

            # If sensor_worker is running, also check for valid data
            if self.sensor_worker and self.sensor_worker.is_alive():
                valid_data_count = sum(1 for val in self.latest_raw if is_valid_reading(val))

                # Build list of active sensors
                active_sensors = [i + 1 for i in range(SENSORS) if is_valid_reading(self.latest_raw[i])]

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
            self.update_dmi_status()
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
            try:
                w.configure(state=state)
            except Exception:
                pass

    # ---------- Cameras ----------
    def start_camera(self, cam_id):
        if cam_id in self.cameras: return
        cw = CameraWorker(
        cam_id=cam_id,
        out_queue=self.cam_queue,
        get_writer=self.get_writer_for,
        get_overlay_ctx=self.get_overlay_context,
        preview_size=PREVIEW_RES,
        record_size=RECORD_RES,
        cam_fps=CAM_FPS,
        preview_fps=PREVIEW_FPS,
        max_queue_size=MAX_QUEUE_SIZE,
        cam_url=CAM_URLS[cam_id],
        cam_names=CAM_NAMES
    )
        self.cameras[cam_id] = cw
        cw.start()

    def stop_cameras(self):
        for cw in self.cameras.values(): cw.stop()
        for cw in self.cameras.values():
            try:
                cw.join(timeout=1.0)
            except Exception:
                pass
        self.cameras.clear()

    def get_writer_for(self, cam_id):
        if not self.running or self.paused_event.is_set() or self.stream_dir is None:
            return None
        if not self.cam_selected[cam_id].get():
            return None
        return self.video_writers.get(cam_id)

    def get_overlay_context(self):
        p = self.project or {}
        lat = f"{self.last_lat:.6f}" if isinstance(self.last_lat, (int, float)) else "--"
        lon = f"{self.last_lon:.6f}" if isinstance(self.last_lon, (int, float)) else "--"
        alt = f"{self.last_alt:.1f}" if isinstance(self.last_alt, (int, float)) else "--"
        dist = f"{self.last_distance_km:.2f}"
        if self.project and self.current_chainage_km is not None:
            chain = format_absolute_chainage(
            p.get('init_chain'),
            self.current_chainage_km)

        else:
            chain = "-- + 0.000 Km"
        dt = datetime.now()
        lrp = p.get('lrp', '--')
        lrp = f"{float(lrp):.3f}" if lrp != '--' else '--'
        return {
            'survey': p.get('survey', '--'),
            'survey_location': p.get('survey_location', '--'),
            'nh': p.get('nh', '--'),
            'oldnh': p.get('oldnh', '--'),
            'section_code': p.get('section_code', '--'),
            'direction': p.get('direction', '--'),
            'lane': p.get('lane', '--'),
            'lat': lat,
            'lon': lon,
            'alt': alt,
            'distance': dist,
            'chainage': chain,
            'speed': f"{int(round(self.last_speed))}",
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
            messagebox.showerror("Invalid Input",
                                 "Initial Chainage, Measurement Section, and LRP must be numbers, and LRP must be non-negative.")
            return
        if not all([survey, survey_location, nh, client, section_code, lrp, direction, lane, pave]):
            messagebox.showerror("Missing Input", "Please fill all fields.")
            return
        if meas_km <= 0:
            messagebox.showerror("Invalid Section", "Measurement Section must be > 0.")
            return

        # Build project paths
        base = filedialog.askdirectory(title="Choose base folder for project")
        if not base: return
        self.build_project_paths(base, survey, direction, lane)

        # Set project dict
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

        # UI Updates
        self.lbl_proj_status.configure(text=f"Specs set {survey} | {direction} {lane}\nRun folder: {self.run_dir}")
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
        self.open_sensor_selection()

    def build_project_paths(self, base, survey, direction, lane):
        proj_root = os.path.join(base, survey)
        lhs, rhs = os.path.join(proj_root, "LHS"), os.path.join(proj_root, "RHS")
        for side_root, prefix in [(lhs, "L"), (rhs, "R")]:
            os.makedirs(side_root, exist_ok=True)
            for i in range(1, 11): os.makedirs(os.path.join(side_root, f"{prefix}{i}"), exist_ok=True)
        side_root = lhs if direction == "Increasing" else rhs
        lane_root = os.path.join(side_root, lane)
        run_folder = f"Run_{ts_for_path()}"
        self.run_dir = os.path.join(lane_root, run_folder)
        self.images_dir = os.path.join(self.run_dir, "Images")
        self.stream_dir = os.path.join(self.run_dir, "Stream")
        self.data_dir = os.path.join(self.run_dir, "Road Condition Data")
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
                except Exception:
                    pass
                self.sensor_worker = None
            self.close_writers()

        self.dmi_encoder_tracker.reset()
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
        self.graph_data = [deque(maxlen=PLOT_MAX_POINTS) for _ in range(SENSORS)]
        self.rebuild_graphs()
        self.set_controls_state(False)
        self.set_selection_state(True)
        self.set_left_state(True)
        self.last_lat = self.last_lon = self.last_alt = None
        self.last_speed = 0.0
        self.last_distance_km = 0.0
        self.table1_rows.clear()
        self.table2_rows.clear()
        self.measure_accum_km = 0.0
        self.window_buffer = []
        logger.info("System reset - DMI encoder reset")

    # ---------- Controls ----------
    def on_start(self):
        if not self.controls_enabled or self.project is None:
            messagebox.showwarning("Start", "Set Project Specifications first.")
            return

        # Check DMI encoder connection
        if not self.dmi_encoder_tracker.is_connected():
            response = messagebox.askyesno(
                "DMI Not Connected",
                "DMI Encoder is not connected.\n"
                "Distance and speed measurement will not work.\n\n"
                "Continue anyway?"
            )
            if not response:
                return

        # Check GPS fix quality (optional but recommended)
        lat, lon, alt = self.gps_reader.get_position()
        fix_info = self.gps_reader.get_fix_info()

        if lat is None or lon is None:
            response = messagebox.askyesno(
                "No GPS Fix",
                f"GPS Status: {fix_info['fix_type']}\n"
                f"Satellites: {fix_info['satellites']}\n"
                f"HDOP: {fix_info['hdop']:.1f}\n\n"
                "No valid GPS position available.\n"
                "Position data will not be recorded.\n\n"
                "Continue anyway?"
            )
            if not response:
                return

        self.running = True
        self.paused_event.clear()
        self.set_status_color("green")
        self.lbl_status.configure(text="Status: running")
        self.set_selection_state(False)
        self.dmi_encoder_tracker.reset()
        logger.info("DMI encoder reset for new run")

        # Start sensor worker
        if self.sensor_worker is None:
            self.sensor_worker = SensorWorker(
                self.ui_queue, self.paused_event, self.get_calibration_value,
                self.on_five_m_tick, self.get_sensor_selection, self.set_latest_raw,
                self.gps_reader, self.dmi_encoder_tracker, sensors=self.sensors
            )
            self.sensor_worker.start()

        # Open video writers
        if not self.video_writers:
            self.open_writers()

        messagebox.showinfo("Started", "Data collection started.\nDistance and speed from DMI encoder.\nPosition from GPS.")

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
            stats = self.dmi_encoder_tracker.get_statistics()
            msg = "Data collection stopped.\n\n"
            msg += f"DMI Distance: {stats['total_distance_km']:.2f} Km\n"
            msg += f"Total Pulses: {stats['total_pulses']}\n"
            msg += f"Update Rate: {stats['update_rate_hz']:.1f} Hz\n\n"
            msg += "Use Export button to save data."
            messagebox.showinfo("Stopped", msg)

    def on_export(self):
        if not self.project or self.run_dir is None:
            messagebox.showinfo("Export", "No configured project/run to export.")
            return
        self.paused_event.set()
        self.set_status_color("yellow")
        self.lbl_status.configure(text="Status: paused (export)")
        if not (PANDAS_OK and OPENPYXL_OK):
            messagebox.showerror("Export",
                                 "pandas and openpyxl are required for Excel export.\nInstall with: pip install pandas openpyxl")
            return
        # Flush partial bin if any
        if self.sensor_worker and self.sensor_worker.bin_buffers[0]:
            avgs = [float(np.nanmean(buf)) if len(buf) else float("nan") for buf in self.sensor_worker.bin_buffers]
            last_speed = self.dmi_encoder_tracker.get_speed_kmh()
            lat, lon, alt = self.last_lat, self.last_lon, self.last_alt
            start_lat = self.sensor_worker.bin_start_lat
            start_lon = self.sensor_worker.bin_start_lon
            current_distance_km = self.dmi_encoder_tracker.get_distance_km()
            traveled_m = current_distance_km - (self.sensor_worker.next_five_m_edge - BIN_SIZE_KM)
            partial_km = traveled_m / 1000
            dir_sign = 1 if self.project["direction"] == "Increasing" else -1
            start_chain = self.current_chainage_km
            end_chain = start_chain + dir_sign * partial_km
            ts = now_local_str()
            self.table2_rows.append(
                [ts, self.project["nh"], f"{start_chain:.3f}", f"{end_chain:.3f}", self.project["direction"],
                 self.project["lane"], f"{start_lat:.6f}", f"{start_lon:.6f}", f"{alt:.1f}", f"{last_speed:.1f}"])
            # For table1 (partial)
            sel = np.array(self.get_sensor_selection(), dtype=bool)
            masked = np.array(avgs, dtype=float)
            masked[~sel] = np.nan
            S1, S2, S3, S4, S5, S6 = masked[:6]
            # Use absolute values so metrics are never negative
            left_rut = np.nanmean(np.abs([S1, S2]))
            right_rut = np.nanmean(np.abs([S5, S6]))
            avg_rut = np.nanmean([left_rut, right_rut])
            left_iri = (((np.nanmean(np.abs([S3]))) / 630) ** (0.893)) * 100
            right_iri = (((np.nanmean(np.abs([S4]))) / 630) ** (0.893)) * 100

            # RMS slope IRI for partial segment
            #segment_length_m = max(partial_km * 1000.0, BIN_SIZE_METERS)

            #left_iri = self.rms_slope_iri([S3], segment_length_m)
            #right_iri = self.rms_slope_iri([S4], segment_length_m)
            avg_iri = np.nanmean(np.abs([left_iri, right_iri]))
            left_texture = np.nanmean(np.abs([S1, S2, S3]))

# Right texture: S4, S5, S6
            right_texture = np.nanmean(np.abs([S4, S5, S6]))

            # Final texture = avg of left & right
            avg_texture = np.nanmean([left_texture, right_texture])

            # Apply texture formula
            left_texture = left_texture * 0.04 + 0.6 if np.isfinite(left_texture) else np.nan
            right_texture = right_texture * 0.04 + 0.6 if np.isfinite(right_texture) else np.nan
            avg_texture = avg_texture * 0.04 + 0.6 if np.isfinite(avg_texture) else np.nan
            row1 = [
                ts, self.project["nh"], f"{start_chain:.3f}", f"{end_chain:.3f}", self.project["direction"],
                self.project["lane"],
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
            self.flush_measurement_window(final_ts=now_local_str(), end_lat=end_lat, end_lon=end_lon, dir_sign=dir_sign,
                                          end_chain=end_chain)
        t1_cols = [
            "Timestamp", "NH Number", "Start Chainage (Km)", "End Chainage (Km)",
            "Direction", "Lane", "Speed (km/h)",
            "Left IRI (m/km)", "Right IRI (m/km)", "Average IRI (m/km)",
            "Left Rut (mm)", "Right Rut (mm)", "Average Rut (mm)",
            "Left Texture (mm)","Right Texture (mm)","Average Texture (mm)",
            "Lat Start", "Long Start", "Lat End", "Long End"
        ]
        t2_cols = [
            "Timestamp", "NH Number", "Start Chainage (Km)", "End Chainage (Km)",
            "Direction", "Lane", "Lat", "Long", "Alt (m)", "Speed (km/h)"
        ]
        df1 = pd.DataFrame(self.table1_rows, columns=t1_cols)

        # Fill empty cells with last valid value for metrics
        for col in ["Left IRI (m/km)", "Right IRI (m/km)", "Average IRI (m/km)",
                    "Left Rut (mm)", "Right Rut (mm)", "Average Rut (mm)", "Left Texture (mm)",
                    "Right Texture (mm)","Average Texture (mm)"]:
            if col in df1.columns:
                df1[col].replace('', np.nan, inplace=True)
                df1[col].fillna(method='ffill', inplace=True)

        df2 = pd.DataFrame(self.table2_rows, columns=t2_cols)

        # Fill empty cells for GPS data
        for col in ["Lat", "Long", "Alt (m)", "Speed (km/h)"]:
            if col in df2.columns:
                df2[col].replace('', np.nan, inplace=True)
                df2[col].fillna(method='ffill', inplace=True)
        
        survey = self.project['survey']
        lane = self.project['lane']
        startc = f"{self.project['init_chain']:.3f}"
        rut_name = f"{survey}_{lane}_{startc}_Rutting.xlsx"
        iri_name = f"{survey}_{lane}_{startc}_RoughnessIRI.xlsx"
        tex_name = f"{survey}_{lane}_{startc}_Texture.xlsx"
        gps_name = f"{survey}_{lane}_{startc}_GPS.xlsx"
        rut_cols = [c for c in t1_cols if
                    c not in ("Left IRI (m/km)", "Right IRI (m/km)", "Average IRI (m/km)", "Left Texture (mm)","Right Texture (mm)","Average Texture (mm)")]
        iri_cols = [c for c in t1_cols if
                    c not in ("Left Rut (mm)", "Right Rut (mm)", "Average Rut (mm)", "Left Texture (mm)", "Right Texture (mm)","Average Texture (mm)")]
        tex_cols = [c for c in t1_cols if
                    c not in ("Left IRI (m/km)", "Right IRI (m/km)", "Average IRI (m/km)", "Left Rut (mm)",
                              "Right Rut (mm)", "Average Rut (mm)")]
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            df1[rut_cols].to_excel(os.path.join(self.data_dir, rut_name), index=False)
            df1[iri_cols].to_excel(os.path.join(self.data_dir, iri_name), index=False)
            df1[tex_cols].to_excel(os.path.join(self.data_dir, tex_name), index=False)
            df2.to_excel(os.path.join(self.data_dir, gps_name), index=False)
            
            # Show DMI statistics
            stats = self.dmi_encoder_tracker.get_statistics()
            msg = f"Data exported to:\n{self.data_dir}\n\n"
            msg += f"Rows T1: {len(df1)} | T2: {len(df2)}\n\n"
            msg += "DMI Encoder Statistics:\n"
            msg += f"Total Distance: {stats['total_distance_km']:.2f} Km\n"
            msg += f"Total Pulses: {stats['total_pulses']}\n"
            msg += f"Update Rate: {stats['update_rate_hz']:.1f} Hz\n"
            msg += f"Connection: {'Active' if stats['connected'] else 'Disconnected'}"
            messagebox.showinfo("Export Complete", msg)

        except Exception as e:
            messagebox.showerror("Export failed", str(e))

    # ---------- Graph + data helpers ----------
    def update_graphs(self, avgs):
        for j, sens_idx in enumerate(self.selected_sensor_indices):
            # Only append valid data points
            if is_valid_reading(avgs[sens_idx]):
                self.graph_data[sens_idx].append(avgs[sens_idx])
            elif len(self.graph_data[sens_idx]) > 0:
                # Use last value if current is invalid
                self.graph_data[sens_idx].append(self.graph_data[sens_idx][-1])

            xs = range(1, len(self.graph_data[sens_idx]) + 1)
            self.lines[j].set_data(list(xs), list(self.graph_data[sens_idx]))
            self.axes[j].relim()
            self.axes[j].autoscale_view()
        if hasattr(self, 'canvas_mpl'):
            self.canvas_mpl.draw_idle()

    def on_five_m_tick(self, avgs, speed_kmph, end_gps, start_gps, boundary_time):
        """
        Called every 5 meters.
        
        ✅ NEW PARAMETER: boundary_time - exact timestamp when boundary was crossed
        """
        if self.project is None or not self.running: return
        dir_sign = 1 if self.project["direction"] == "Increasing" else -1
        start_chain = self.current_chainage_km
        end_chain = start_chain + dir_sign * BIN_SIZE_KM
        ts = now_local_str()
        lat, lon, alt = end_gps
        start_lat, start_lon, _ = start_gps
        self.last_lat, self.last_lon, self.last_alt = lat, lon, alt
        self.last_speed = speed_kmph
        self.last_distance_km = self.dmi_encoder_tracker.get_distance_km()
        self.table2_rows.append(
            [ts, self.project["nh"], f"{start_chain:.3f}", f"{end_chain:.3f}", self.project["direction"],
             self.project["lane"], f"{lat:.6f}", f"{lon:.6f}", f"{alt:.1f}", f"{speed_kmph:.1f}"])
        # ✅ NEW: Pass boundary timestamp for synchronization
        self.capture_images(
            chainage_label=self.format_chainage_label(start_chain), 
            lat=lat, lon=lon, alt=alt,
            target_time=boundary_time
        )
        self.measure_accum_km += BIN_SIZE_KM
        self.window_buffer.append(
            {"S": avgs, "speed": speed_kmph, "lat": lat, "lon": lon, "start_lat": start_lat, "start_lon": start_lon})
        if self.measure_accum_km + 1e-9 >= self.project["meas_km"]:
            self.flush_measurement_window(final_ts=ts, end_lat=lat, end_lon=lon, dir_sign=dir_sign, end_chain=end_chain)
        self.current_chainage_km = end_chain
        self.update_chainage_label(dir_sign * BIN_SIZE_KM)
        self.update_graphs(avgs)

        # REAL-TIME EXPORT: Write GPS data to Excel immediately
        self.export_incremental_data()

    def get_sensor_selection(self):
        return [v.get() for v in self.sensor_selected]

    def _fmt_or_none(self, x, ndigits):
        if x is None or (isinstance(x, float) and np.isnan(x)): return None
        return round(float(x), ndigits)
    
    def rms_slope_iri(self, values_mm, segment_length_m):
        """
        RMS slope based IRI approximation.

        values_mm: iterable of vertical displacements (mm)
        segment_length_m: longitudinal length represented (meters)

        Returns: IRI in m/km
        """
        vals = np.asarray(values_mm, dtype=float)
        vals = vals[np.isfinite(vals)]

        if vals.size == 0 or segment_length_m <= 0:
            return np.nan

        # RMS of vertical displacement (mm)
        rms_mm = np.sqrt(np.mean(vals ** 2))

        # RMS slope (mm/m)
        rms_slope = rms_mm / segment_length_m

        # Convert to IRI-like metric (m/km)
        return rms_slope * 1000.0

    def flush_measurement_window(self, final_ts, end_lat, end_lon, dir_sign, end_chain):
        if not self.window_buffer: return
        S_matrix = np.array([w["S"] for w in self.window_buffer])  # (n,SENSORS)
        mean_S = np.nanmean(S_matrix, axis=0)
        sel = np.array(self.get_sensor_selection(), dtype=bool)
        masked = np.array(mean_S, dtype=float)
        masked[~sel] = np.nan
        S1, S2, S3, S4, S5, S6 = masked[:6]
        # Use absolute values for rutting, IRI and texture (roughness) so output is non-negative

        # Fill missing values with last valid
        for i in range(len(masked)):
            if not is_valid_reading(masked[i]) and hasattr(self, 'last_valid_masked') and i < len(
                    self.last_valid_masked):
                if is_valid_reading(self.last_valid_masked[i]):
                    masked[i] = self.last_valid_masked[i]
        self.last_valid_masked = masked.copy()

        left_rut = np.nanmean(np.abs([S1, S2]))
        right_rut = np.nanmean(np.abs([S5, S6]))
        avg_rut = np.nanmean(np.abs([left_rut, right_rut]))
        left_iri = (((np.nanmean(np.abs([S3]))) / 630) ** (0.893)) * 100
        right_iri = (((np.nanmean(np.abs([S4]))) / 630) ** (0.893)) * 100

        # Lane-based RMS slope IRI using full window data
        # S3 -> left lane roughness sensor
        # S4 -> right lane roughness sensor

        window_length_m = self.measure_accum_km * 1000.0

        #left_iri = self.rms_slope_iri(S_matrix[:, 2], window_length_m)
        #right_iri = self.rms_slope_iri(S_matrix[:, 3], window_length_m)
        

        avg_iri = np.nanmean(np.abs([left_iri, right_iri]))
        # ---- TEXTURE CALC (LEFT / RIGHT / AVG) ----

        left_texture = np.nanmean(np.abs([S1, S2, S3]))
        right_texture = np.nanmean(np.abs([S4, S5, S6]))
        avg_texture = np.nanmean([left_texture, right_texture])

        left_texture = left_texture * 0.04 + 0.6 if np.isfinite(left_texture) else np.nan
        right_texture = right_texture * 0.04 + 0.6 if np.isfinite(right_texture) else np.nan
        avg_texture = avg_texture * 0.04 + 0.6 if np.isfinite(avg_texture) else np.nan

        speed_mean = float(np.nanmean([w["speed"] for w in self.window_buffer]))
        start_c = end_chain - dir_sign * self.measure_accum_km
        end_c = start_c + dir_sign * self.measure_accum_km
        lat_start = self.window_buffer[0]["start_lat"]
        lon_start = self.window_buffer[0]["start_lon"]
        row1 = [
            final_ts, self.project["nh"], f"{start_c:.3f}", f"{end_c:.3f}", self.project["direction"],
            self.project["lane"],
            self._fmt_or_none(speed_mean, 0),
            self._fmt_or_none(left_iri, 2), self._fmt_or_none(right_iri, 2), self._fmt_or_none(avg_iri, 2),
            self._fmt_or_none(left_rut, 1), self._fmt_or_none(right_rut, 1), self._fmt_or_none(avg_rut, 1),
            self._fmt_or_none(left_texture, 2),self._fmt_or_none(right_texture, 2),self._fmt_or_none(avg_texture, 2),
            f"{lat_start:.6f}", f"{lon_start:.6f}", f"{end_lat:.6f}", f"{end_lon:.6f}"
        ]
        self.table1_rows.append(row1)
        self.measure_accum_km = 0.0
        self.window_buffer = []

        # REAL-TIME EXPORT: Write measurement data to Excel immediately
        self.export_incremental_data()

    def export_incremental_data(self):
        """Export data incrementally to Excel files in real-time."""
        # Safety checks
        if not self.project or self.run_dir is None:
            return

        if not (PANDAS_OK and OPENPYXL_OK):
            logger.debug("pandas/openpyxl not available for incremental export")
            return

        # Don't export if no data yet
        if not self.table1_rows and not self.table2_rows:
            return

        try:
            # Generate file names
            survey = self.project['survey']
            lane = self.project['lane']
            startc = f"{self.project['init_chain']:.3f}"

            rut_name = f"{survey}_{lane}_{startc}_Rutting.xlsx"
            iri_name = f"{survey}_{lane}_{startc}_RoughnessIRI.xlsx"
            tex_name = f"{survey}_{lane}_{startc}_Texture.xlsx"
            gps_name = f"{survey}_{lane}_{startc}_GPS.xlsx"

            # Column definitions (same as on_export)
            t1_cols = [
                "Timestamp", "NH Number", "Start Chainage (Km)", "End Chainage (Km)",
                "Direction", "Lane", "Speed (km/h)",
                "Left IRI (m/km)", "Right IRI (m/km)", "Average IRI (m/km)",
                "Left Rut (mm)", "Right Rut (mm)", "Average Rut (mm)",
                "Left Texture (mm)","Right Texture (mm)","Average Texture (mm)",
                "Lat Start", "Long Start", "Lat End", "Long End"
            ]

            t2_cols = [
                "Timestamp", "NH Number", "Start Chainage (Km)", "End Chainage (Km)",
                "Direction", "Lane", "Lat", "Long", "Alt (m)", "Speed (km/h)"
            ]

            # Create DataFrames from accumulated data
            if self.table1_rows:
                df1 = pd.DataFrame(self.table1_rows, columns=t1_cols)

                # Fill empty cells with last valid value for metrics
                for col in ["Left IRI (m/km)", "Right IRI (m/km)", "Average IRI (m/km)",
                            "Left Rut (mm)", "Right Rut (mm)", "Average Rut (mm)", "Left Texture (mm)",
                            "Right Texture (mm)","Average Texture (mm)"]:
                    if col in df1.columns:
                        df1[col].replace('', np.nan, inplace=True)
                        df1[col].fillna(method='ffill', inplace=True)
            else:
                df1 = pd.DataFrame(columns=t1_cols)

            if self.table2_rows:
                df2 = pd.DataFrame(self.table2_rows, columns=t2_cols)

                # Fill empty cells for GPS data
                for col in ["Lat", "Long", "Alt (m)", "Speed (km/h)"]:
                    if col in df2.columns:
                        df2[col].replace('', np.nan, inplace=True)
                        df2[col].fillna(method='ffill', inplace=True)
            else:
                df2 = pd.DataFrame(columns=t2_cols)

            # Define column subsets for different files
            rut_cols = [c for c in t1_cols if
                        c not in ("Left IRI (m/km)", "Right IRI (m/km)", "Average IRI (m/km)", "Left Texture (mm)","Right Texture (mm)","Average Texture (mm)")]
            iri_cols = [c for c in t1_cols if
                        c not in ("Left Rut (mm)", "Right Rut (mm)", "Average Rut (mm)", "Left Texture (mm)","Right Texture (mm)","Average Texture (mm)")]
            tex_cols = [c for c in t1_cols if
                        c not in ("Left IRI (m/km)", "Right IRI (m/km)", "Average IRI (m/km)", "Left Rut (mm)",
                                  "Right Rut (mm)", "Average Rut (mm)")]

            # Ensure data directory exists
            os.makedirs(self.data_dir, exist_ok=True)

            # Write Excel files (overwrite each time with latest data)
            if not df1.empty:
                df1[rut_cols].to_excel(os.path.join(self.data_dir, rut_name), index=False, engine='openpyxl')
                df1[iri_cols].to_excel(os.path.join(self.data_dir, iri_name), index=False, engine='openpyxl')
                df1[tex_cols].to_excel(os.path.join(self.data_dir, tex_name), index=False, engine='openpyxl')

            if not df2.empty:
                df2.to_excel(os.path.join(self.data_dir, gps_name), index=False, engine='openpyxl')

            logger.debug(f"Real-time export: T1={len(df1)} rows, T2={len(df2)} rows written to {self.data_dir}")

        except PermissionError:
            # File is currently open in Excel - skip this write cycle
            logger.debug("Incremental export skipped - file may be open in Excel")
        except Exception as e:
            logger.warning(f"Incremental export failed: {e}")

    def set_latest_raw(self, raw_vals):
        self.latest_raw = list(raw_vals)

    def capture_images(self, chainage_label, lat=None, lon=None, alt=None, target_time=None):
        """
        Capture images synchronized to 5m boundary crossing.
        
        ✅ NEW PARAMETER: target_time - timestamp when 5m boundary was crossed
        """
        if self.images_dir is None: 
            return
        
        # ✅ NEW: If no target time provided, use current time (backward compatible)
        if target_time is None:
            target_time = time.time()
            logger.warning("capture_images() called without target_time, using current time")
        
        stamp = ts_for_path()
        current_time = time.time()
        
        for cam_id, worker in self.cameras.items():
            if not self.cam_selected.get(cam_id, tk.BooleanVar(value=True)).get(): 
                continue
            
            # ✅ NEW: Get frame closest to boundary crossing time
            record_frame, time_diff = worker.get_frame_closest_to_time(target_time, max_age=0.5)
            
            if record_frame is None:
                # No suitable frame found - use 10-
                logger.warning(f"Camera {cam_id}: No synchronized frame available for {chainage_label}, using testcard")
                
                ctx = self.get_overlay_context()
                testcard = make_testcard(RECORD_RES[0], RECORD_RES[1],
                                        f"{CAM_NAMES.get(cam_id, f'CAM{cam_id}')} - NO SYNC FRAME")
                record_frame = draw_header_to_size(testcard, ctx, RECORD_RES)
            else:
                # ✅ Found synchronized frame - log the offset
                offset_ms = time_diff * 1000
                at_speed = self.last_speed if hasattr(self, 'last_speed') else 0
                distance_offset_m = (at_speed / 3.6) * time_diff if at_speed > 0 else 0
                
                logger.info(f"Camera {cam_id}: Synchronized frame for {chainage_label} "
                          f"(offset={offset_ms:.1f}ms, ~{distance_offset_m:.2f}m at {at_speed:.1f}km/h)")
            
            # Save image
            name = CAM_NAMES.get(cam_id, f"CAM{cam_id}")
            folder = os.path.join(self.images_dir, name)
            os.makedirs(folder, exist_ok=True)
            fname = f"{name}_{chainage_label}_{stamp}.jpg".replace(" ", "_")
            path = os.path.join(folder, fname)
            
            try:
                cv2.imwrite(path, record_frame)
                logger.debug(f"Saved image: {fname}")
            except Exception as e:
                logger.error(f"Failed to save image {fname}: {e}")


    # ---------- Telemetry/UI queue ----------
    # ---------- Telemetry/UI queue ----------
    def poll_cam_frames(self):
        start_time = time.time()
        frame_count = 0
        
        # ✅ FIX: Adaptive processing based on queue load
        queue_size = self.cam_queue.qsize()
        max_queue_size_total = MAX_QUEUE_SIZE * len(CAM_IDS)
        
        # Process more frames when queue is filling up
        if queue_size > max_queue_size_total * 0.5:
            max_frames_per_cycle = 20  # High load mode
        else:
            max_frames_per_cycle = 13  # Normal mode
        
        try:
            while frame_count < max_frames_per_cycle:
                cam_id, frame = self.cam_queue.get_nowait()
                frame_count += 1
                pil_img = Image.fromarray(frame)  # Frame already in RGB
                ctk_img = ctk.CTkImage(light_image=pil_img, dark_image=pil_img, size=PREVIEW_RES)
                try:
                    i = CAM_IDS.index(cam_id)
                except ValueError:
                    i = cam_id
                if i < len(self.cam_labels):
                    lbl = self.cam_labels[i]
                    lbl.configure(image=ctk_img)
                    lbl.image = ctk_img
            
            # ✅ Monitor queue health
            if queue_size > max_queue_size_total * 0.8:
                logger.warning(f"Camera queue near full: {queue_size}/{max_queue_size_total}")
        
        except queue.Empty:
            pass
        
        # Only log if processing took significant time
        elapsed = time.time() - start_time
        if elapsed > 0.1:
            logger.debug(f"Processed {frame_count} frames in {elapsed:.3f}s")
        
        # ✅ FIX: Adaptive polling rate based on queue load
        if queue_size > max_queue_size_total * 0.5:
            # High load: poll faster (40 FPS)
            self.after(25, self.poll_cam_frames)
        else:
            # Normal load: poll at 20 FPS
            self.after(50, self.poll_cam_frames)

    def poll_ui_queue(self):
        # Process queue for GPS data and events
        processed_count = 0
        
        try:
            # Process queue for GPS and events only
            while processed_count < 10:
                msg = self.ui_queue.get_nowait()
                processed_count += 1
                
                if msg["type"] == "telemetry":
                    # Update GPS data from queue
                    self.last_lat = msg['lat']
                    self.last_lon = msg['lon']
                    self.last_alt = msg['alt']
                    if msg['lat'] is not None:
                        self.var_lat.set(f"{msg['lat']:.6f}")
                    if msg['lon'] is not None:
                        self.var_lon.set(f"{msg['lon']:.6f}")
                    if msg['alt'] is not None:
                        self.var_alt.set(f"{msg['alt']:.1f} m")
                elif msg["type"] == "five_m":
                    # Already handled in callback
                    pass
        except queue.Empty:
            pass

        # CRITICAL: Read DMI distance/speed DIRECTLY (no queue) for ZERO lag
        # This matches the test script's direct serial read approach
        if self.running or self.project:
            distance_km = self.dmi_encoder_tracker.get_distance_km()
            speed_kmh = self.dmi_encoder_tracker.get_speed_kmh()
            
            # Update immediately - same as test script's print()
            self.var_dist.set(f"{distance_km:.2f} Km")
            self.var_speed.set(f"{speed_kmh:.0f} km/h")
            self.speed_progress.set(min(speed_kmh / 100.0, 1.0))
            
            # Store for other uses
            self.last_distance_km = distance_km
            self.last_speed = speed_kmh

        # Also update GPS display even when not running
        if self.project and not self.running:
            lat, lon, alt = self.gps_reader.get_position()
            if lat is not None and lon is not None:
                self.var_lat.set(f"{lat:.6f}")
                self.var_lon.set(f"{lon:.6f}")
                if alt is not None:
                    self.var_alt.set(f"{alt:.1f} m")

        # Poll at 5ms (100 Hz) to match test script's tight loop responsiveness
        self.after(10, self.poll_ui_queue)

    # ---------- Writers (video) ----------
    def open_writers(self):
        self.close_writers()
        if self.stream_dir is None or self.project is None: return
        for cam_id, worker in self.cameras.items():
            worker.reset_recording_state()
        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        for cam_id in CAM_IDS:
            if not self.cam_selected[cam_id].get(): continue
            name = CAM_NAMES.get(cam_id, f"CAM{cam_id}")
            folder_name = CAM_FOLDERS.get(cam_id, name)

            # Create subdirectory for this camera
            cam_folder = os.path.join(self.stream_dir, folder_name)
            os.makedirs(cam_folder, exist_ok=True)

            # Save video in camera-specific folder
            out_path = os.path.join(cam_folder,
                                    f"{name}_{self.project['survey']}_{self.project['lane']}_{self.project['init_chain']:.3f}.mp4")
            vw = cv2.VideoWriter(out_path, fourcc, CAM_FPS, RECORD_RES)
            if not vw.isOpened():
                messagebox.showerror("Video", f"Failed to open writer for {name}")
            else:
                self.video_writers[cam_id] = vw
                logger.info(
                    f"Video writer opened: {folder_name}/{name}_{self.project['survey']}_{self.project['lane']}_{self.project['init_chain']:.3f}.mp4")

    def close_writers(self):
        for vw in self.video_writers.values():
            try:
                vw.release()
            except Exception:
                pass
        self.video_writers.clear()

    # ---------- Chainage & labels ----------
    def set_status_color(self, color):
        mapping = {"red": "#d32f2f", "yellow": "#fbc02d", "green": "#43a047"}
        self.chip.configure(fg_color=mapping.get(color, "#d32f2f"))

    def update_chainage_label(self, _delta_km):
        if self.project is None:
            return
        self.var_chain.set(
            format_absolute_chainage(
                self.project['init_chain'],
                self.current_chainage_km
            )
        )


    def format_chainage_label(self, chain_km):
        return f"{chain_km:.3f}Km"


    # ---------- Sensor Selection ----------
    def open_sensor_selection(self):
        win = ctk.CTkToplevel(self)
        win.title("Select Sensors to Export")
        win.geometry("850x650")
        win.transient(self)
        win.grab_set()
        try:
            win.attributes("-topmost", True)
        except Exception:
            pass

        ctk.CTkLabel(win, text="Select Sensors to Export & Calibrate", font=ctk.CTkFont(size=16, weight="bold")).pack(
            pady=(10, 5))

        # Sensor status info
        status_info = ctk.CTkFrame(win)
        status_info.pack(fill="x", padx=20, pady=(5, 10))
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

            chk = ctk.CTkCheckBox(sensor_frame, text=f"S{i + 1}", variable=self.sensor_selected[i], width=40)
            chk.pack(side="left", padx=(0, 5))

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
                if is_valid_reading(raw_val):
                    calib_val = raw_val - self.calib_values[i]
                    self.sensor_reading_labels[i].configure(
                        text=f"Raw:{raw_val:.1f} Cal:{calib_val:.1f}",
                        text_color="#43a047" if is_valid_reading(calib_val) else "#d32f2f"
                    )
                    valid_count += 1
                else:
                    self.sensor_reading_labels[i].configure(
                        text="-- No data",
                        text_color="#d32f2f"
                    )

            self.sensor_status_label.configure(
                text=f"Live Readings: {valid_count}/{SENSORS} sensors active | Raw=absolute, Cal=relative"
            )

            win.after(200, update_sensor_readings)

        update_sensor_readings()

        # CALIBRATION SECTION
        calib_frame = ctk.CTkFrame(win)
        calib_frame.pack(fill="x", padx=20, pady=10)

        ctk.CTkLabel(calib_frame, text="Manual Calibration Offsets", font=ctk.CTkFont(size=14, weight="bold")).pack(
            pady=5)

        # Input fields for calibration
        calib_inputs_frame = ctk.CTkFrame(calib_frame)
        calib_inputs_frame.pack(pady=5)

        self.calib_entries = []
        calib_def = [319.05, 312.67, 307.68, 311.63, 301.12, 315.64]
        for i in range(SENSORS):
            row = i // 3
            col = i % 3
            sensor_calib_frame = ctk.CTkFrame(calib_inputs_frame)
            sensor_calib_frame.grid(row=row, column=col, padx=10, pady=3)
            ctk.CTkLabel(sensor_calib_frame, text=f"S{i + 1}:", width=30).pack(side="left")
            entry = ctk.CTkEntry(sensor_calib_frame, width=80, placeholder_text="0.0")
            entry.insert(0, f"{calib_def[i]:.2f}")
            entry.pack(side="left")
            self.calib_entries.append(entry)

        def calibrate_sensors():
            """Apply calibration with confirmation popup."""
            new_calib = []
            try:
                for i, entry in enumerate(self.calib_entries):
                    val = entry.get().strip()
                    if val:
                        new_calib.append(float(val))
                    else:
                        # If empty, use current reading
                        if is_valid_reading(self.latest_raw[i]):
                            new_calib.append(self.latest_raw[i])
                        else:
                            new_calib.append(0.0)
            except ValueError:
                messagebox.showerror("Invalid Input", "All calibration values must be numbers.")
                return

            # Show confirmation popup
            confirm_msg = "Calibration Offsets to Apply:\n\n"
            for i in range(SENSORS):
                confirm_msg += f"S{i + 1}: {new_calib[i]:.2f}\n"
            confirm_msg += "\nApply these calibration values?"

            if messagebox.askyesno("Confirm Calibration", confirm_msg):
                self.calib_values = new_calib
                logger.info(f"✓ Calibration applied manually")
                logger.info(f"   New offsets: {[f'{v:.2f}' for v in self.calib_values]}")
                messagebox.showinfo("Calibration Applied", "Sensor calibration updated successfully!")
            else:
                logger.info("Calibration cancelled by user")

        def check_sensors_ready():
            """Check if all connected sensors have data available."""
            ready_sensors = []
            not_ready_sensors = []

            for i in range(SENSORS):
                if self.sensors[i] is None:
                    continue  # Skip disconnected sensors

                try:
                    data_avail = self.sensors[i].DataAvail()
                    if data_avail >= EXPECTED_BLOCK_SIZE:
                        ready_sensors.append(i + 1)
                    else:
                        not_ready_sensors.append((i + 1, data_avail))
                except Exception as e:
                    logger.warning(f"Error checking sensor {i + 1}: {e}")
                    not_ready_sensors.append((i + 1, 0))

            return ready_sensors, not_ready_sensors

        def auto_calibrate():
            """Auto-calibrate using current sensor readings with smart waiting."""

            # First check if sensors are ready
            connected_count = sum(1 for s in self.sensors if s is not None)

            if connected_count == 0:
                messagebox.showerror("Auto-Calibration Error",
                                     "No sensors connected!\n\nPlease check sensor connections and restart software.")
                return

            # Check sensor readiness
            ready, not_ready = check_sensors_ready()

            if not_ready:
                # Show waiting dialog
                wait_msg = f"Waiting for sensors to buffer data...\n\n"
                wait_msg += f"Ready: {len(ready)}/{connected_count} sensors\n"
                wait_msg += f"Buffering: {len(not_ready)} sensors\n\n"

                for sensor_num, data_count in not_ready:
                    progress = (data_count / EXPECTED_BLOCK_SIZE) * 100
                    wait_msg += f"S{sensor_num}: {progress:.0f}% ({data_count}/{EXPECTED_BLOCK_SIZE})\n"

                wait_msg += f"\n⏱️ Estimated wait: {10 - (min([d for _, d in not_ready]) / 100):.0f} seconds\n"
                wait_msg += "\nClick OK to wait, or Cancel to skip waiting."

                if messagebox.askyesno("Sensors Buffering Data", wait_msg):
                    # Wait for sensors to buffer data (max 15 seconds)
                    logger.info("Waiting for sensors to buffer data...")
                    wait_start = time.time()
                    max_wait = 15.0

                    while time.time() - wait_start < max_wait:
                        ready, not_ready = check_sensors_ready()
                        if not not_ready:
                            logger.info("All connected sensors ready!")
                            break
                        time.sleep(0.5)

                    if not_ready:
                        logger.warning(f"Timeout waiting for sensors: {[s for s, _ in not_ready]}")

            # Now proceed with calibration
            raw = read_laser_values(self.sensors)
            new_calib = []
            calibrated_sensors = []
            skipped_sensors = []

            for i in range(SENSORS):
                # Check if sensor is connected
                if self.sensors[i] is None:
                    new_calib.append(0.0)
                    skipped_sensors.append((i + 1, "Not connected"))
                elif is_valid_reading(raw[i]):
                    new_calib.append(raw[i])
                    calibrated_sensors.append(i + 1)
                    self.calib_entries[i].delete(0, 'end')
                    self.calib_entries[i].insert(0, f"{raw[i]:.2f}")
                else:
                    new_calib.append(0.0)
                    # Check why no data
                    try:
                        data_avail = self.sensors[i].DataAvail()
                        if data_avail < EXPECTED_BLOCK_SIZE:
                            skipped_sensors.append((i + 1, f"Still buffering ({data_avail}/{EXPECTED_BLOCK_SIZE})"))
                        else:
                            skipped_sensors.append((i + 1, "No valid data (check sensor)"))
                    except:
                        skipped_sensors.append((i + 1, "Error reading sensor"))

            # Show confirmation with clear indication of skipped sensors
            confirm_msg = f"Auto-Calibration Results:\n\n"
            confirm_msg += f"✓ Calibrated: {len(calibrated_sensors)}/{connected_count} sensors\n"

            if calibrated_sensors:
                confirm_msg += f"   Sensors: {', '.join([f'S{s}' for s in calibrated_sensors])}\n\n"
                for i in range(SENSORS):
                    if i + 1 in calibrated_sensors:
                        confirm_msg += f"   S{i + 1}: {new_calib[i]:.2f} mm\n"

            if skipped_sensors:
                confirm_msg += f"\n✗ Skipped: {len(skipped_sensors)} sensors\n"
                for sensor_num, reason in skipped_sensors:
                    confirm_msg += f"   S{sensor_num}: {reason}\n"

                confirm_msg += "\n⚠️ WARNING: Only calibrated sensors will give accurate measurements!"

                # Add recommendation if many sensors skipped
                if len(skipped_sensors) > 2:
                    confirm_msg += "\n\n💡 TIP: Wait 15-20 seconds after startup, then retry."

            confirm_msg += "\n\nApply these calibration values?"

            if messagebox.askyesno("Confirm Auto-Calibration", confirm_msg):
                self.calib_values = new_calib
                logger.info("=" * 60)
                logger.info("AUTO-CALIBRATION APPLIED")
                logger.info(f"✓ Calibrated sensors: {calibrated_sensors}")
                if skipped_sensors:
                    logger.warning(f"✗ Skipped sensors: {[s[0] for s in skipped_sensors]}")
                    for sensor_num, reason in skipped_sensors:
                        logger.warning(f"   S{sensor_num}: {reason}")
                logger.info(f"   New offsets: {[f'{v:.2f}' for v in self.calib_values]}")
                logger.info("=" * 60)

                result_msg = f"Auto-calibration completed!\n\n"
                result_msg += f"✓ Calibrated: {len(calibrated_sensors)}/{connected_count} sensors"
                if skipped_sensors:
                    result_msg += f"\n✗ Skipped: {len(skipped_sensors)} sensors"
                    result_msg += "\n\n💡 Check console log for details."
                    result_msg += "\n💡 If sensors buffering, wait and try again."

                messagebox.showinfo("Calibration Applied", result_msg)
            else:
                logger.info("Auto-calibration cancelled by user")

        calib_btn_frame = ctk.CTkFrame(calib_frame)
        calib_btn_frame.pack(pady=5)
        ctk.CTkButton(calib_btn_frame, text="Auto-Calibrate (Use Current)", command=auto_calibrate, width=180).pack(
            side="left", padx=5)
        ctk.CTkButton(calib_btn_frame, text="Apply Manual Values", command=calibrate_sensors, width=180).pack(
            side="left", padx=5)

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
                if is_valid_reading(self.latest_raw[i]):
                    self.sensor_selected[i].set(True)
                else:
                    self.sensor_selected[i].set(False)
            messagebox.showinfo("Active Sensors",
                                f"Selected {sum(1 for i in range(SENSORS) if is_valid_reading(self.latest_raw[i]))} active sensors")

        def apply_and_close():
            selected_count = sum(v.get() for v in self.sensor_selected)
            active_count = sum(1 for i in range(SENSORS) if is_valid_reading(self.latest_raw[i]))
            self.rebuild_graphs()
            messagebox.showinfo("Sensors Updated",
                                f"{selected_count} sensors selected for export and plotting.\n"
                                f"({active_count} sensors currently active)\n\n"
                                f"Note: Graphs show CALIBRATED values (relative to start position)")
            win.destroy()

        ctk.CTkButton(btn_frame, text="Select All", command=select_all, width=120).pack(side="left", padx=5)
        ctk.CTkButton(btn_frame, text="Deselect All", command=deselect_all, width=120).pack(side="left", padx=5)
        ctk.CTkButton(btn_frame, text="Select Active Only", command=select_active_only, width=140).pack(side="left",
                                                                                                        padx=5)
        ctk.CTkButton(btn_frame, text="Apply & Close", command=apply_and_close, width=120).pack(side="left", padx=5)

    # ---------- Calibration ----------
    def get_calibration_value(self):
        return self.calib_values

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
            if hasattr(self, 'gps_worker') and self.gps_worker:
                logger.info("Stopping GPS worker...")
                try:
                    self.gps_worker.stop()
                    self.gps_worker.join(timeout=2.0)
                    logger.info("✓ GPS worker stopped")
                except Exception as e:
                    logger.error(f"Error stopping GPS worker: {e}")

            # 4. Stop DMI encoder worker thread
            if hasattr(self, 'dmi_encoder_worker') and self.dmi_encoder_worker:
                logger.info("Stopping DMI encoder worker...")
                try:
                    self.dmi_encoder_worker.stop()
                    self.dmi_encoder_worker.join(timeout=2.0)
                    if self.dmi_encoder_worker.is_alive():
                        logger.warning("DMI encoder worker did not stop cleanly")
                    else:
                        logger.info("✓ DMI encoder worker stopped")
                except Exception as e:
                    logger.error(f"Error stopping DMI encoder worker: {e}")

            # 5. Disconnect GPS
            if self.gps_reader:
                logger.info("Disconnecting GPS...")
                try:
                    self.gps_reader.disconnect()
                    logger.info("✓ GPS disconnected")
                except Exception as e:
                    logger.error(f"Error disconnecting GPS: {e}")

            # 6. Disconnect DMI encoder
            if hasattr(self, 'dmi_encoder_tracker') and self.dmi_encoder_tracker:
                logger.info("Disconnecting DMI encoder...")
                try:
                    self.dmi_encoder_tracker.disconnect()
                    logger.info("✓ DMI encoder disconnected")
                except Exception as e:
                    logger.error(f"Error disconnecting DMI encoder: {e}")

            # 7. Stop camera workers
            logger.info("Stopping camera workers...")
            try:
                self.stop_cameras()
                logger.info("✓ Camera workers stopped")
            except Exception as e:
                logger.error(f"Error stopping cameras: {e}")

            # 8. Close video writers
            logger.info("Closing video writers...")
            try:
                self.close_writers()
                logger.info("✓ Video writers closed")
            except Exception as e:
                logger.error(f"Error closing writers: {e}")

            # 9. Clean up laser sensors
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
        ctk.CTkLabel(splash, text="Loading NSV Controller - DMI Edition...", font=ctk.CTkFont(size=16)).pack(expand=True)

    # Center the splash screen
    splash.update_idletasks()
    width = splash.winfo_width()
    height = splash.winfo_height()
    x = (splash.winfo_screenwidth() // 2) - (width // 2)
    y = (splash.winfo_screenheight() // 2) - (height // 2)
    splash.geometry(f"{width}x{height}+{x}+{y}")

    # Schedule closing splash and showing main app
    splash.after(3000, lambda: (splash.destroy(), app.deiconify()))

    app.protocol("WM_DELETE_WINDOW", app.on_close)
              
    app.mainloop()