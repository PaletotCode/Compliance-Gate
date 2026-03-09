# Main View TI v1

## Escopo
- Módulo frontend isolado em `frontend/src/main_view/`.
- Fluxo suportado:
  - Profiles prontos por fonte (`AD`, `UEM`, `EDR`, `ASSET`)
  - Preview/parse
  - Ingest
  - Materialize
  - Tabela final com virtualização e carregamento incremental
  - Summary/Filters
  - Export via `report/run`

## Contrato de segurança
- Autenticação cookie-only (`withCredentials`).
- CSRF automático para métodos state-changing.
- Redirecionamento para login em `401`.
- Sem import de assets/fontes externas em runtime.

## Validação recomendada
Rodar da raiz do repositório:

```bash
bash scripts/verify_all.sh
```

Saídas esperadas:
- Exit code `0`
- `retests/output/FRONTEND_INTEGRATION_FINAL_REPORT.md`
- Logs individuais em `retests/output/*.log`
