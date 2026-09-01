import subprocess, sys, os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
subprocess.run([PY, 'compute_yur_absorption.py'], capture_output=True)
subprocess.run([PY, 'compute_signals.py'], capture_output=True)
subprocess.run([PY, 'compute_squeeze.py'], capture_output=True)
subprocess.run([PY, 'compute_trap.py'], capture_output=True)
print("refresh ok")
