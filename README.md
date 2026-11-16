# retail-bronze

Dataset sintético de retail multi-sistema para practicar la reingeniería de una capa Bronze en Databricks:
CDC, SCD, deduplicación, survivorship, schema evolution, calidad de datos y PII.

## Generar

```bash
python -m venv .venv && .venv/bin/pip install -r generator/requirements.txt
.venv/bin/python generator/generate.py                                  # 20k clientes, 3k pedidos/día, 30 días
.venv/bin/python generator/generate.py --customers 2000 --days 10       # muestra chica
```

## Fuentes (`data/`)

| Carpeta | Formato | Problemas incluidos a propósito |
|---|---|---|
| `crm/clientes_YYYY-MM-DD.csv` | Snapshot completo diario | PII (RFC, teléfono, email, nacimiento); ~6% de clientes duplicados con variantes de nombre, email y teléfono; RFC faltante |
| `erp_cdc/date=*/cdc-*.json` | CDC estilo Debezium (`op` c/u/d, `before`/`after`, `ts_ms`) | ~3% entregado un día tarde; orden aleatorio dentro del archivo; ~1% de eventos duplicados; deletes físicos |
| `app_events/date=*/hour=*/events.jsonl` | JSONL por hora (muchos archivos chicos) | ~2% de llegadas tardías; schema drift a mitad del periodo (`device`, `coupon_code`); `amount` a veces string; ~0.3% de líneas corruptas |
| `truth/` | CSV | Cliente "golden" real y mapeo `crm_id → golden_id` para validar dedup y survivorship |

Las fuentes **no coinciden entre sí**:
- El teléfono cambia solo en el CRM.
- La dirección cambia solo en el ERP.
- El email cambia en los dos sistemas.
- El ERP guarda los nombres en mayúsculas.

Además, un cliente "ballena" concentra el ~10% de los pedidos (*data skew*).

## Ingestor legado

`legacy/CustomerIngestJob.java` es el job Spark "heredado", sin documentación. El ejercicio es:
1. Hacerle ingeniería inversa y documentar sus reglas de deduplicación y survivorship (y sus bugs).
2. Reconstruirlo como Lakeflow Declarative Pipeline (Auto Loader → Bronze, `AUTO CDC`/SCD2 → Silver),
   con expectations y cuarentena, y enmascaramiento de PII en Unity Catalog.
3. Validar el resultado contra `truth/`.
