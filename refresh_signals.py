import subprocess, sys, os
from datetime import datetime
os.chdir(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
# Порядок ВАЖЕН: tradestats → passive_yur → absorption/signals/squeeze/trap
for script in [
    'compute_passive_yur.py',
    'compute_enrich.py',
    'compute_yur_absorption.py',   # ромбы (читает legal_passive_estimate)
    'compute_signals.py',          # дивергенции/whale/комбо
    'compute_squeeze.py',          # сквизы
    'compute_trap.py',             # ловушки
]:
    r = subprocess.run([PY, script], capture_output=True, text=True)
    if r.returncode != 0:
        print(f"❌ {script} FAILED:\n{r.stderr[-1500:]}")
    else:
        print(f"✓ {script}")
print("refresh ok", datetime.now().strftime('%H:%M:%S'))
