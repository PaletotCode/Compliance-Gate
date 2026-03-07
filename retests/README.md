# Retests

## Engine Core v1
Para validar a Engine ponta a ponta (docker + ingest + materialize + report):

```bash
python retests/scripts/run_engine_retests.py
```

O script:
1. Sobe `db/redis/api` via Docker Compose.
2. Faz login com bootstrap admin.
3. Executa ingest de machines.
4. Materializa `machines_final.parquet`.
5. Executa report `machines_status_summary`.
6. Valida coerência matemática dos totais.
7. Salva artefatos em `retests/output/`.

### Outputs esperados
- `engine_materialize_<run_id>.json`
- `engine_report_<run_id>.json`
- `engine_report_table_<run_id>.csv`

### Flags úteis
- `KEEP_ENGINE_RETEST_STACK=true` para não derrubar os containers ao final.
