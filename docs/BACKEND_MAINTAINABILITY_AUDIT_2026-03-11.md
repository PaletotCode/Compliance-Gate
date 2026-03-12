# Backend Maintainability Audit (2026-03-11)

## Escopo
- Levantamento de manutenibilidade em todo `src/compliance_gate`.
- Foco em centralização, organização de responsabilidades e manutenção futura por times humanos + IA.

## Panorama
- Arquivos Python: 246
- Módulos mais densos:
  - `Engine`: 57 arquivos / 11.168 linhas
  - `infra`: 31 / 3.492
  - `domains`: 60 / 2.207
  - `http`: 11 / 1.841
  - `authentication`: 22 / 1.513

## Hotspots críticos
- `src/compliance_gate/Engine/interfaces/rulesets_api.py`
- `src/compliance_gate/Engine/rulesets/runtime.py`
- `src/compliance_gate/Engine/rulesets/explain.py`
- `src/compliance_gate/Engine/expressions/validator.py`
- `src/compliance_gate/http/routes/datasets.py`
- `src/compliance_gate/Engine/interfaces/declarative_api.py`
- `src/compliance_gate/http/routes/csv_tabs.py`
- `src/compliance_gate/http/routes/workspace_uploads.py`

## Achados principais

### 1) Rotas com lógica de negócio demais
- `datasets.py`, `csv_tabs.py`, `workspace_uploads.py` acumulam validação, regras de negócio, persistência e serialização.
- Recomendação: camada de Application Services para ingest/profile/upload.

### 2) Contratos HTTP inconsistentes
- Mix de `ApiResponse/PaginatedResponse` com retornos `dict` sem `response_model`.
- Recomendação: contrato único para todas rotas públicas.

### 3) Erros estruturados sem tradutor único
- Engine possui boa taxonomia (`code/message/details/hint`), mas status mapping está duplicado por router.
- Recomendação: único mapper de exceções da Engine.

### 4) Overlap de runtime
- Semântica de expressão espalhada entre `expressions`, `runtime/declarative_runtime`, `rulesets/runtime`.
- Recomendação: Execution Core único (AST plan + coercion + guardrails).

### 5) Versionamento repetitivo por entidade
- Padrão semelhante em transformations/segments/views/rulesets com implementações separadas.
- Recomendação: repositório base versionado reutilizável.

### 6) Fronteira legado x novo ainda híbrida
- `domains/machines/service.py` ainda sinaliza fallback legado e caminho “not yet implemented”.
- Recomendação: reduzir fallback para modo controlado por feature flag e convergir para artefato materializado.

### 7) RBAC funcional, porém repetitivo
- Muitos `Depends(require_role(...))` espalhados.
- Recomendação: matriz de políticas central (resource/action/role).

### 8) Fluxo de perfis/drafts distribuído
- Estado de draft/profile repartido entre rotas e stores diferentes.
- Recomendação: serviço transacional único e idempotente para ciclo de configuração.

## O que centralizar primeiro
1. Contracts Layer (resposta, erro, paginação)
2. Application Services (ingest/profile/upload)
3. Execution Core de expressão/AST
4. Versioned Repository Base
5. RBAC policy matrix
6. Boundary cleanup legado -> materializado

## Avaliação do código (backend)
- **Qualidade arquitetural atual**: boa base, mas com acoplamento de borda elevado.
- **Risco de manutenção**: moderado-alto nos hotspots citados.
- **Nota técnica (backend)**: **7.1/10**.

## Observação de execução
- `pytest -q` não executou neste ambiente por ausência de dependência (`sqlalchemy`), limitando evidência de regressão automática nesta sessão.

