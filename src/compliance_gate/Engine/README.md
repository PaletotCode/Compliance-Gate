# Engine Core v1

Engine Core v1 fornece materialização colunar (Parquet) e execução de reports declarativos para o Compliance Gate.

## Escopo v1
- Materializa `machines_final.parquet` por `tenant_id` e `dataset_version_id`.
- Registra metadados e auditoria de execução (`engine_artifacts`, `engine_runs`).
- Executa reports com templates fixos (sem SQL livre do cliente).
- Define metadados declarativos versionáveis para `transformations`, `segments` e `views`.
- Expõe API em `/api/v1/engine/*`.

## Estrutura
- `catalog/`: resolução de dataset, paths de artefatos e fontes.
- `config/`: limites e paths da engine.
- `spines/`: contrato da tabela canônica `machines_final`.
- `materialization/`: pipeline de materialização e writer de parquet.
- `reports/`: templates + runner em DuckDB.
- `expressions/`: AST declarativo + validação de tipos (sem SQL).
- `transformations/`: schemas versionáveis de colunas derivadas.
- `segments/`: schemas versionáveis de filtros salvos.
- `views/`: schemas versionáveis da planilha principal.
- `validation/`: guardrails e preview/explain.
- `errors/`: taxonomia de erros estruturados para UX.
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

## Declarative Engine v1
Escopo implementado neste prompt:
- AST declarativo com nós: `literal`, `column_ref`, `unary_op`, `binary_op`, `logical_op`, `function_call`.
- Funções suportadas v1:
  - `regex_extract(column, pattern, group)`
  - `split_part(column, delimiter, index)`
  - `substring(column, start, length)`
  - `upper`, `lower`, `trim`
  - `date_diff_days(column, now|other_column)`
  - `coalesce(a, b)`
- Versionamento em Postgres:
  - `engine_transformations` + `engine_transformation_versions`
  - `engine_segments` + `engine_segment_versions`
  - `engine_views` + `engine_view_versions`
- Catálogo interno de `machines_final` com tipo por coluna, amostra e estatísticas rápidas:
  - `GET /api/v1/engine/internal/catalog/machines-final`
- Taxonomia de erros estruturados (`code`, `message`, `details`, `hint`):
  - `InvalidExpressionSyntax`
  - `UnknownColumn`
  - `TypeMismatch`
  - `UnsupportedOperatorForType`
  - `RegexCompileError`
  - `ExcessiveComplexity`
  - `GuardrailViolation`

## Declarative Runtime v1
Endpoints públicos:
- `GET /api/v1/engine/catalog/machines?dataset_version_id=<id>`
- `GET /api/v1/engine/segments/templates`
- `POST /api/v1/engine/segments/from-template`
- `POST /api/v1/engine/transformations`
- `PUT /api/v1/engine/transformations/{id}`
- `GET /api/v1/engine/transformations`
- `GET /api/v1/engine/transformations/{id}`
- `POST /api/v1/engine/segments`
- `PUT /api/v1/engine/segments/{id}`
- `GET /api/v1/engine/segments`
- `GET /api/v1/engine/segments/{id}`
- `POST /api/v1/engine/segments/preview?dataset_version_id=<id>`
- `POST /api/v1/engine/views`
- `PUT /api/v1/engine/views/{id}`
- `GET /api/v1/engine/views`
- `GET /api/v1/engine/views/{id}`
- `POST /api/v1/engine/views/preview?dataset_version_id=<id>`
- `POST /api/v1/engine/views/run?dataset_version_id=<id>`

Regras de produto aplicadas:
- Filtros da tabela final vêm de `segments` declarativos (sem filtros fixos hard-coded no frontend).
- Versionamento ativo por entidade (`transformations`, `segments`, `views`).
- `engine_runs` registra `segment_preview`, `view_preview` e `view_run` com métricas de linhas e tempo.
- Execução segura AST -> Polars com allowlist de operadores/funções (sem SQL livre).

## RuleSet v2 (metamodel declarativo de classificação)
Escopo implementado nesta etapa:
- Novas entidades versionáveis e auditáveis:
  - `engine_rule_sets` (definition)
  - `engine_rule_set_versions` (status + payload versionado)
  - `engine_rule_blocks` (`special`, `primary`, `flags`)
  - `engine_rule_entries` (condição + output + prioridade)
- Ciclo de publicação por tenant:
  - `draft` -> `validated` -> `published` -> `archived`
- Ponteiros por definition:
  - `active_version`: versão de trabalho/consumo atual
  - `published_version`: versão oficial publicada
- Rollback:
  - promove versão anterior para `published`
  - arquiva a versão previamente publicada
- Auditoria:
  - eventos `RULESET_CREATE`, `RULESET_UPDATE`, `RULESET_VALIDATE`, `RULESET_PUBLISH`, `RULESET_ROLLBACK`
  - persistidos em `audit_logs` com tenant, actor e versão no `details`

AST v2 (validação sem execução dedicada para classificação nesta etapa):
- Operadores adicionais:
  - binários matemáticos: `+`, `-`, `*`, `/`
- Literais:
  - `null` (`value_type = "null"`)
- Funções novas:
  - `is_null`, `is_not_null`
  - `contains`, `starts_with`, `ends_with`
  - `regex_match`, `regex_extract`
- `date_now`, `date_diff` (unit: `hours|days|weeks|months|years`)
  - `coalesce` com N argumentos (>=2)
- Compatibilidade preservada:
  - funções v1 continuam válidas (`date_diff_days`, `split_part`, `substring`, etc.)

Endpoints públicos (`/api/v1/engine/*`):
- `POST /rulesets`
- `GET /rulesets`
- `GET /rulesets/{ruleset_id}`
- `PUT /rulesets/{ruleset_id}`
- `DELETE /rulesets/{ruleset_id}` (archive lógico)
- `GET /rulesets/{ruleset_id}/versions`
- `POST /rulesets/{ruleset_id}/versions`
- `GET /rulesets/{ruleset_id}/versions/{version}`
- `PUT /rulesets/{ruleset_id}/versions/{version}`
- `POST /rulesets/{ruleset_id}/versions/{version}/validate`
- `POST /rulesets/{ruleset_id}/versions/{version}/publish`
- `POST /rulesets/{ruleset_id}/rollback`

Endpoints internos (`/api/v1/engine/internal/*`):
- `GET /internal/rulesets/active?name=<ruleset_name>`
- `GET /internal/rulesets/published?name=<ruleset_name>`

## RuleSet v2 Runtime (classificação declarativa)
Modos de execução por tenant:
- `legacy`: executa somente `rule.py` (comportamento atual).
- `shadow`: executa legado + declarativo, responde legado e persiste divergências/métricas.
- `declarative`: executa somente RuleSet publicado.

Precedência de classificação:
1. `special` (`bypass`)
2. `primary` (`first-match-wins`)
3. `flags` (`additive`)

Integração na materialização (`POST /api/v1/engine/materialize/machines`):
- Em `legacy`, mantém saída equivalente ao classificador atual.
- Em `shadow/declarative`, compila RuleSet publicado com allowlist (sem SQL livre) e aplica guardrails.
- `engine_runs.metrics_json` recebe:
  - `rows_scanned`
  - `rows_classified`
  - `elapsed_ms`
  - `rule_hits` por regra
  - `divergences` (quando `shadow`)
- Divergências de `shadow` são persistidas em `engine_classification_divergences`.

Guardrails de runtime:
- Timeout hard por execução (`classification_timeout_seconds`).
- Limite de complexidade AST (nós/profundidade) na compilação.
- Limite de linhas (`classification_max_rows`).
- Estimativa de orçamento de memória (`classification_memory_budget_mb`).
- Limite de regras por RuleSet (`classification_max_rules`).
- Erros estruturados (`GuardrailViolation`) com `code/message/details/hint`.

Endpoints operacionais (`/api/v1/engine/*`):
- `GET /classification/mode`
- `PUT /classification/mode`
- `GET /classification/divergences?limit=<n>&dataset_version_id=<id>`
- `GET /classification/metrics?limit=<n>`

## RuleSet v2 Migration (aposentadoria controlada do `rule.py`)
Objetivo operacional:
- migrar regras hardcoded para RuleSet declarativo com prova de paridade por tenant
- executar cutover progressivo com rollback instantâneo via modo de execução

Inventário e baseline:
- Biblioteca oficial de templates legados: `Engine/rulesets/template_library.py`
  - mapeia precedência completa: `special` -> `primary` -> `flags`
  - cobre regras legadas (`GAP`, `AVAILABLE`, `INCONSISTENCY`, `PHANTOM`, `ROGUE`, `MISSING_UEM`, `MISSING_EDR`, `MISSING_ASSET`, `SWAP`, `CLONE`, `OFFLINE`, `COMPLIANT`, `LEGACY`, `PA_MISMATCH`)
- Bootstrap automático por tenant:
  - `POST /classification/migration/bootstrap-baseline`
  - cria/publica RuleSet baseline (`published`) e grava estado de migração no tenant
  - opção `all_tenants=true` (restrita ao tenant padrão de operação)

Estado, paridade e promoção:
- `GET /classification/migration/state`
  - retorna fase atual (`A|B|C|D`), baseline e snapshot de paridade
- `GET /classification/parity-report?dataset_version_id=<id>&run_id=<id>`
  - consolida divergência legado vs declarativo por:
    - dimensão (`primary_status`, `flags`, `mixed`)
    - severidade
    - regra (`rule_key`)
  - calcula `parity_percent` e `parity_ok` (threshold configurável, default `99.9`)
- `PUT /classification/migration/promote-phase`
  - promove fase do tenant em sequência (`A -> B -> C -> D`)
  - bloqueia promoção quando `parity_ok != true` (gate por padrão)

Estratégia de cutover por fase (`mode=declarative`):
1. Fase `A`:
  - mantém `primary` legado
  - aplica apenas `flags` declarativas (com bypass special legado)
2. Fase `B`:
  - aplica `primary + flags` declarativos
  - mantém bypass special legado
3. Fase `C`:
  - aplica `special + primary + flags` declarativos
  - ainda com capacidade de comparação via shadow operacional
4. Fase `D`:
  - declarative-only por tenant

Rollback seguro:
- rollback instantâneo de produção: `PUT /classification/mode` com `mode=legacy`
  - independe da fase atual e restaura resposta legado imediatamente
- rollback de versão declarativa: `POST /rulesets/{ruleset_id}/rollback`
  - re-publica versão anterior do RuleSet

Auditoria de migração:
- `CLASSIFICATION_MIGRATION_BASELINE_BOOTSTRAP`
- `CLASSIFICATION_MIGRATION_PARITY_REPORT`
- `CLASSIFICATION_MIGRATION_PHASE_PROMOTE`
- `CLASSIFICATION_MODE_UPDATE`
- eventos persistidos em `audit_logs` com `tenant`, `actor`, `details`

Playbook (go/no-go):
1. Executar bootstrap baseline no tenant alvo.
2. Rodar materialização em `mode=shadow` nos datasets alvo.
3. Gerar `parity-report` por `dataset_version`.
4. Verificar gate:
  - `parity_percent >= 99.9`
  - sem divergências críticas não explicadas (por severidade/regra)
5. Promover fase (`A` -> `B` -> `C` -> `D`) sem pular etapas.
6. Se qualquer regressão operacional for detectada, retornar imediatamente para `mode=legacy`.

Rollback drill recomendado (antes de D):
1. Deixar tenant em `mode=declarative` fase `C`.
2. Executar carga de teste e registrar métricas.
3. Acionar `PUT /classification/mode` para `legacy`.
4. Reexecutar carga e confirmar restauração do comportamento legado.
5. Validar trilha em `audit_logs` e tempos de recuperação.

## RuleSet v2 Explain (validação avançada + feedback UX)
Objetivo:
- garantir payload estável para o Rule Builder sem quebrar o front
- respostas de erro com `code`, `message`, `details`, `hint`, `node_path`

Endpoints (`/api/v1/engine/*`):
- `POST /validate-ruleset` (alias: `/rulesets/validate-ruleset`)
- `POST /explain-row` (alias: `/rulesets/explain-row`)
- `POST /explain-sample` (alias: `/rulesets/explain-sample`)
- `POST /dry-run-ruleset` (alias: `/rulesets/dry-run-ruleset`)

### Contrato de validação em 3 estágios
`validate-ruleset` retorna:
- `syntax`: estrutura e allowlist de operadores/funções
- `semantics`: tipos e colunas conhecidas
- `viability`: compilação segura, conflitos de output e regras inalcançáveis

Formato:
```json
{
  "is_valid": false,
  "stages": [
    {"stage": "syntax", "ok": true, "issues": [], "warnings": []},
    {"stage": "semantics", "ok": false, "issues": [], "warnings": []},
    {"stage": "viability", "ok": false, "issues": [], "warnings": []}
  ],
  "issues": [
    {
      "code": "UnknownColumn",
      "message": "A coluna informada não existe no catálogo.",
      "details": {"column": "hostnme", "suggestions": ["hostname"]},
      "hint": "Use uma coluna disponível no catálogo do parquet.",
      "node_path": "root.left",
      "stage": "semantics",
      "severity": "error"
    }
  ],
  "warnings": [],
  "summary": {"error_count": 1, "warning_count": 0}
}
```

### Explain determinístico
`explain-row` e `explain-sample` retornam:
- regra que bateu
- condições que falharam
- ordem de avaliação
- motivo da decisão final

Campos principais:
- `final_output` (`primary_status`, `primary_status_label`, `flags`)
- `matched_rules`
- `evaluation_order`
- `rules` (trace por regra)
- `decision_reason`

### Dry-run com shadow
`dry-run-ruleset` executa classificação sem publicar a versão:
- aceita `mode` em `legacy|shadow|declarative`
- em `shadow`, inclui warning `ShadowDivergenceWarning` quando houver divergência
- retorna métricas: `rows_scanned`, `rows_classified`, `elapsed_ms`, `rule_hits`, `divergences`

### Exemplo de erro estruturado
```json
{
  "detail": {
    "code": "InvalidExpressionSyntax",
    "message": "Payload do RuleSet inválido.",
    "details": {
      "reason": "invalid_ruleset_payload",
      "errors": [{"type": "missing", "loc": ["blocks", 0, "entries", 0, "condition"]}]
    },
    "hint": "Revise schema_version, blocks, entries, condition e output.",
    "node_path": "blocks[0].entries[0].condition"
  }
}
```

Persistência runtime:
- `engine_classification_modes`:
  - configura modo ativo por tenant + `ruleset_name`
  - auditado em `audit_logs` (`CLASSIFICATION_MODE_UPDATE`)
- `engine_classification_divergences`:
  - snapshot por máquina de legado vs declarativo no `shadow`
  - vínculo com `engine_runs` para rastreabilidade operacional

Limitações atuais:
- Não há execução de SQL livre pelo cliente.
- Não há endpoint dedicado de explain-plan para views declarativas (somente preview de dados).

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
