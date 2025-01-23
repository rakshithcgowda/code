import subprocess

# Start Extract.py
extract_process = subprocess.Popen(["python", "extracter.py"])

# Start plot.py
plot_process = subprocess.Popen(["python", "plotting.py"])

# Wait for both scripts to finish (optional)
extract_process.wait()
plot_process.wait()