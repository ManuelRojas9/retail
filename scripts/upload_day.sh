#!/usr/bin/env bash
# Sube al Volume los archivos de UN día, para simular que las fuentes llegan diario.
# Uso: scripts/upload_day.sh 2026-01-01 [perfil-databricks]
set -euo pipefail

DAY="${1:?uso: upload_day.sh YYYY-MM-DD [perfil]}"
PROFILE="${2:-DEFAULT}"
SRC="data"
DST="dbfs:/Volumes/retail/bronze/raw"

cp_one() { databricks fs cp --overwrite -p "$PROFILE" "$1" "$DST/$2"; }

cp_one "$SRC/crm/clientes_${DAY}.csv" "crm/clientes_${DAY}.csv"
databricks fs cp -r --overwrite -p "$PROFILE" "$SRC/erp_cdc/date=${DAY}" "$DST/erp_cdc/date=${DAY}"
databricks fs cp -r --overwrite -p "$PROFILE" "$SRC/app_events/date=${DAY}" "$DST/app_events/date=${DAY}"

echo "Día ${DAY} subido a ${DST}"
