# Agents Guide - Compliance Gate Backend

Este arquivo serve como contexto exclusivo para novos agentes (LLMs) ou colaboradores de IA no projeto.

## Contexto e Visão
- **Domínios**: Todas as lógicas são agregadas por domínios (`machines`, `telefonia`, `impressoras`).
- **Arquitetura "Frontend Burro"**: O backend é encarregado de fazer as consolidações, gerar painéis (summary, timelines) e entregar os dados prontos (`responses.py`, `pagination.py`).
- **Header-first (Crucial)**: Colunas importadas via arquivos externos ou integrations não dependem de sua posição/index (ex: "coluna 3", "index 4"). Toda lógica DEVE se orientar pelo nome dos headers (mapeiros registrados em `column_registry.py`).
- **Ambiente On-Premise**: O sistema roda offline, sem APIs de nuvem externas, focando no banco local (PostgreSQL) e no processamento analítico local (DuckDB + Polars).

## O que NÃO fazer
1. **Nunca use index para colunas de dados.** As colunas são flexíveis. Confie nos `ColumnRegistry`.
2. **Sem dados sensíveis nos logs.** Use hashes no log para identificar IDs, mas não informações abertas do arquivo processado em exceções globais.
3. **Não polua a stack.** Qualquer nova dependência precisa de justificativa explícita no plano de implementação e aprovação do usuário. O backend já possui Polars+DuckDB para dados pesados e Postgres/Redis para os transacionais e cache.

## Estrutura do Projeto (`src/compliance_gate/`)
- `domains/`: Agrupamento de regras de negócio. Ex: `machines/`.
- `http/`: Camada de borda, dependências HTTP, manipulação de erros, rotas FastAPI.
- `infra/`: Persistência técnica (db, redis, celery, local storage via polars).
- `shared/`: Utilitários e schemas comuns.

## Testes & Layouts
Temos preferência por layouts rígidos validando o contrato (schemas Pydantic). Quando for criar ou modificar um endpoint, certifique-se de envolver o retorno em `ApiResponse` ou `PaginatedResponse` da pasta `shared.schemas.responses`.
