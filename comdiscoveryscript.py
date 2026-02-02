from serial.tools import list_ports
import json

def discover():
    devices = []

    for p in list_ports.comports():
        devices.append({
            "com_port": p.device,
            "vid": f"0x{p.vid:04x}" if p.vid is not None else None,
            "pid": f"0x{p.pid:04x}" if p.pid is not None else None,
            "serial_number": p.serial_number,
            "manufacturer": p.manufacturer,
            "description": p.description,
            "location": p.location
        })

    return devices


if __name__ == "__main__":
    devices = discover()

    print("\n=== RAW DEVICE LIST ===\n")
    for d in devices:
        print(d)

    # Optional: dump to JSON for easy copy-paste
    with open("device_snapshot.json", "w") as f:
        json.dump(devices, f, indent=2)

    print("\nSaved device_snapshot.json")
