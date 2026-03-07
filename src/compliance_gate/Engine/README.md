# Engine Core v1

Engine Core v1 fornece materialização colunar (Parquet) e execução de reports declarativos para o Compliance Gate.

## Escopo v1
- Materializa `machines_final.parquet` por `tenant_id` e `dataset_version_id`.
- Registra metadados e auditoria de execução (`engine_artifacts`, `engine_runs`).
- Executa reports com templates fixos (sem SQL livre do cliente).
- Expõe API em `/api/v1/engine/*`.

## Estrutura
- `catalog/`: resolução de dataset, paths de artefatos e fontes.
- `config/`: limites e paths da engine.
- `spines/`: contrato da tabela canônica `machines_final`.
- `materialization/`: pipeline de materialização e writer de parquet.
- `reports/`: templates + runner em DuckDB.
- `validation/`: guardrails e preview/explain.
- `interfaces/`: API e CLI.

## Materialização
Endpoint:
- `POST /api/v1/engine/materialize/machines?dataset_version_id=<id>&tenant_id=<tenant>`

Comportamento:
- Resolve o `dataset_version` do tenant.
- Reusa o pipeline do domínio `machines` para reconstruir os registros.
- Classifica e gera a spine canônica.
- Escreve parquet em:
  - `/workspace/artifacts/<tenant_id>/machines/<dataset_version_id>/machines_final.parquet`
- Idempotência por checksum (se arquivo+checksum já batem, não reprocessa).
- Usa advisory lock no Postgres para evitar corrida concorrente.

## Reports
Endpoints:
- `POST /api/v1/engine/reports/preview?dataset_version_id=<id>&tenant_id=<tenant>`
- `POST /api/v1/engine/reports/run?dataset_version_id=<id>&tenant_id=<tenant>`

Body v1:
```json
{
  "template_name": "machines_status_summary",
  "limit": 1000
}
```

Saída do template obrigatório `machines_status_summary`:
- colunas: `key`, `label`, `count`, `type`
- `type` pode ser `status` ou `flag`.

## Metadados de banco
- `engine_artifacts`: path/checksum/row_count/schema do artefato.
- `engine_report_definitions` + `engine_report_versions`: catálogo versionado de templates.
- `engine_runs`: trilha de execução (status, métricas, erro truncado).

## Execução local (CLI)
```bash
python -m compliance_gate.Engine.interfaces.cli materialize <dataset_version_id>
python -m compliance_gate.Engine.interfaces.cli report <dataset_version_id> machines_status_summary
```

## Retestes
Use o script:
```bash
python retests/scripts/run_engine_retests.py
```

Outputs gerados em `retests/output/`:
- `engine_materialize_<run_id>.json`
- `engine_report_<run_id>.json`
- `engine_report_table_<run_id>.csv`
