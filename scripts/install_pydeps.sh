#!/usr/bin/env bash
set -euo pipefail
LOG="/var/log/emr_bootstrap_install_pydeps.log"
exec > >(tee -a "$LOG") 2>&1

echo "[INFO] Bootstrap start: $(date -Iseconds) on $(hostname)"

# OS packages (git, unzip, certs)
if command -v dnf >/dev/null 2>&1; then
  PKG_MGR="dnf -y install"
elif command -v yum >/dev/null 2>&1; then
  PKG_MGR="yum -y install"
else
  PKG_MGR=""
  echo "[WARN] No dnf/yum found; skipping OS packages install"
fi

if [[ -n "${PKG_MGR}" ]]; then
  set +e
  ${PKG_MGR} git unzip ca-certificates
  RC=$?
  set -e
  if [[ $RC -ne 0 ]]; then
    echo "[WARN] Failed to install OS packages (network?). Continuing anyway."
  else
    command -v update-ca-trust >/dev/null 2>&1 && sudo update-ca-trust || true
  fi
fi

# Python packages (system-wide)
PY_BIN="/usr/bin/python3"
export PIP_BREAK_SYSTEM_PACKAGES=1

$PY_BIN -m ensurepip || true

PKGS=("$@")
if [[ ${#PKGS[@]} -eq 0 ]]; then
  PKGS=( "pandas==2.2.2" "pyarrow==16.1.0" "pillow==10.4.0" "optree==0.12.1" )
fi

retry() { n=0; max=5; d=10; until "$@"; do n=$((n+1)); [[ $n -ge $max ]] && { echo "[ERR] $*"; exit 1; }; echo "[WARN] retry $n/$max"; sleep $d; d=$((d*2)); done; }

echo "[INFO] Installing system-wide: ${PKGS[*]}"
retry sudo -H env PIP_BREAK_SYSTEM_PACKAGES=1 $PY_BIN -m pip install --no-cache-dir --only-binary=:all: "${PKGS[@]}"

$PY_BIN - <<'PY'
import sys
def ver(m):
    try:
        mod = __import__(m)
        print(f"{m}: {getattr(mod,'__version__','?')}")
    except Exception as e:
        print(f"{m}: ERR {e}")
print("Python:", sys.version)
for m in ("pandas","pyarrow","PIL","optree"):
    ver(m)
PY

echo "[INFO] Bootstrap end: $(date -Iseconds)"