"""[NEW] Автопересчёт производных таблиц (ромбы -> whale/дивергенции/пробои)."""
import subprocess, sys, os
os.chdir(os.path.dirname(os.path.abspath(__file__)))
PY = sys.executable
# Сначала ромбы (нужны для пробоев), затем сигналы
subprocess.run([PY, 'compute_yur_absorption.py'], capture_output=True)
subprocess.run([PY, 'compute_signals.py'], capture_output=True)
print("refresh ok")
