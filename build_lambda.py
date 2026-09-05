#!/usr/bin/env python3
"""
Cross-platform packaging script for EventGuardian Lambda processor.
Works natively on Windows, macOS, and Linux without external zip utilities.
"""
import os
import shutil
import subprocess
import sys
import zipfile


def build_package():
    root_dir = os.path.dirname(os.path.abspath(__file__))
    build_dir = os.path.join(root_dir, "build")
    lambda_dir = os.path.join(root_dir, "lambda_processor")
    req_file = os.path.join(lambda_dir, "requirements.txt")
    app_file = os.path.join(lambda_dir, "app.py")
    zip_path = os.path.join(root_dir, "lambda_function.zip")

    print("[EventGuardian] Starting packaging...")

    # 1. Clean previous build artifacts
    if os.path.exists(build_dir):
        print(f"[EventGuardian] Cleaning build directory: {build_dir}")
        shutil.rmtree(build_dir)
    if os.path.exists(zip_path):
        print(f"[EventGuardian] Removing existing archive: {zip_path}")
        os.remove(zip_path)

    os.makedirs(build_dir, exist_ok=True)

    # 2. Install dependencies into build directory
    print(f"[EventGuardian] Installing dependencies from {req_file}...")
    cmd = [
        sys.executable, "-m", "pip", "install",
        "-r", req_file,
        "-t", build_dir,
        "--quiet"
    ]
    subprocess.run(cmd, check=True)

    # 3. Copy handler code
    print(f"[EventGuardian] Copying {app_file} -> {build_dir}...")
    shutil.copy(app_file, os.path.join(build_dir, "app.py"))

    # 4. Create ZIP archive
    print(f"[EventGuardian] Creating archive: {zip_path}...")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(build_dir):
            for file in files:
                abs_path = os.path.join(root, file)
                rel_path = os.path.relpath(abs_path, build_dir)
                zipf.write(abs_path, rel_path)

    file_size_mb = os.path.getsize(zip_path) / (1024 * 1024)
    print(f"[EventGuardian] SUCCESS! Lambda package created ({file_size_mb:.2f} MB): {zip_path}")


if __name__ == "__main__":
    build_package()
