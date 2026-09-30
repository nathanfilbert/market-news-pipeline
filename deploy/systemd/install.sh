#!/usr/bin/env bash
# Install and start the mnp user services (systemctl --user).
#
#   deploy/systemd/install.sh [--port 8000] [--host 127.0.0.1]
#
# Safe to re-run, e.g. after changing the port or moving the checkout.
set -euo pipefail

port=8000
host=127.0.0.1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --port) port="$2"; shift 2 ;;
    --host) host="$2"; shift 2 ;;
    *) echo "unknown option: $1" >&2; exit 2 ;;
  esac
done

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo="$(cd "$here/../.." && pwd)"
uv="$(command -v uv || true)"
if [[ -z "$uv" ]]; then
  echo "uv not found on PATH" >&2
  exit 1
fi

# Install dependencies up front so the first start is quick.
(cd "$repo" && "$uv" sync --frozen)

dest="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
mkdir -p "$dest"
for unit in mnp-worker.service mnp-api.service; do
  sed -e "s|@REPO@|$repo|g" -e "s|@UV@|$uv|g" -e "s|@PORT@|$port|g" -e "s|@HOST@|$host|g" \
    "$here/$unit" > "$dest/$unit"
done

systemctl --user daemon-reload
systemctl --user enable mnp-worker.service mnp-api.service
systemctl --user restart mnp-worker.service mnp-api.service

# Without lingering, user services stop at logout and only start at login.
if [[ "$(loginctl show-user "$USER" --property=Linger --value 2>/dev/null)" != "yes" ]]; then
  if loginctl enable-linger "$USER" 2>/dev/null; then
    echo "enabled lingering for $USER: services start at boot"
  else
    echo "run 'sudo loginctl enable-linger $USER' so the services start at boot" >&2
  fi
fi

echo "installed; dashboard at http://$host:$port/ui"
echo "status: systemctl --user status mnp-worker mnp-api"
echo "logs:   journalctl --user -u mnp-worker -f"
