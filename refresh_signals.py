import subprocess, sys, os
from datetime import datetime
os.chdir(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
for script in ['compute_yur_absorption.py', 'compute_signals.py', 'compute_squeeze.py', 'compute_trap.py']:
    r = subprocess.run([PY, script], capture_output=True, text=True)
    if r.returncode != 0:
        print(f"❌ {script} FAILED:\n{r.stderr[-1500:]}")
print("refresh ok", datetime.now().strftime('%H:%M:%S'))
