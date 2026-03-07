import os
import socket
import requests
import sys
import time
import subprocess

def check_port(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def verify():
    print("--- Compliance Gate Environment Verification ---")
    
    # 1. Check CSV files
    csv_files = ["AD.csv", "UEM.csv", "EDR.csv", "ASSET.CSV"]
    print("\n[1] Checking data files:")
    for f in csv_files:
        path = os.path.join(os.getcwd(), f)
        if os.path.exists(path):
            size = os.path.getsize(path) / 1024
            print(f"  ✅ {f} found ({size:.1f} KB)")
        else:
            print(f"  ❌ {f} MISSING")

    # 2. Check Backend
    print("\n[2] Checking Backend (Port 8000):")
    if check_port(8000):
        try:
            resp = requests.get("http://localhost:8000/api/v1/machines/summary", timeout=2)
            if resp.status_code == 200:
                data = resp.json()
                total = data.get("data", {}).get("total", 0)
                print(f"  ✅ Backend is ONLINE. Total machines in engine: {total}")
            else:
                print(f"  ⚠️  Backend responded with status {resp.status_code}")
        except Exception as e:
            print(f"  ❌ Backend error: {e}")
    else:
        print("  ❌ Backend is OFFLINE (Port 8000 closed)")

    # 3. Check Workspace
    print("\n[3] Checking Workspace:")
    workspace = os.path.join(os.getcwd(), "workspace")
    if os.path.exists(workspace):
        print("  ✅ Workspace directory exists.")
    else:
        print("  ❌ Workspace directory MISSING.")

    print("\n--- Verification Complete ---")

if __name__ == "__main__":
    verify()
