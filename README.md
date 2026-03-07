# Compliance Gate Backend

## Visão Geral
Este repositório contém o backend do projeto **Compliance Gate**, uma plataforma projetada para entregar dados processados e prontos para consumo por um frontend "burro". A arquitetura é on-premise, guiada por domínios e voltada para a ingestão baseada em cabeçalhos (header-first).

## Stack Tecnológica
- **Linguagem**: Python 3.12+
- **API Framework**: FastAPI + Uvicorn
- **Validação e Settings**: Pydantic v2 + pydantic-settings
- **ORM e Database**: SQLAlchemy 2.0, Alembic, PostgreSQL
- **Data Engine**: Polars + DuckDB (preparado para análises on-premise)
- **Background Jobs**: Celery + Redis
- **Testes e Qualidade**: pytest, ruff, mypy, pre-commit

## Instalação e Execução

### Rodando via Docker (Recomendado)
A maneira mais fácil de iniciar a infraestrutura (PostgreSQL, Redis e a API) é através do Docker Compose:

```bash
docker compose up -d
```
A API estará exposta em `http://localhost:8000`.

### Bootstrap de Admin (AUTH CORE v1)
No primeiro run, é possível bootstrap de admin local por env:

```bash
export AUTH_BOOTSTRAP_ADMIN_USERNAME=admin
export AUTH_BOOTSTRAP_ADMIN_PASSWORD=Admin1234
```

Depois faça login em `POST /api/v1/auth/login` com `username/password`.

## Autenticação (resumo)
- Prefixo: `/api/v1/auth`
- Login local com JWT (`HS256`)
- MFA TOTP (Microsoft Authenticator) com setup via QR code
- Reset de senha sem e-mail/SMS usando `totp_code` ou `recovery_code`
- RBAC mínimo: `TI_ADMIN` e `DIRECTOR`

### Execução Local (Desenvolvimento)
1. Crie e ative um ambiente virtual:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   ```
2. Instale as dependências (com pacotes de desenvolvimento):
   ```bash
   pip install -e ".[dev]"
   ```
3. Configure as variáveis de ambiente baseando-se no `.env.example`:
   ```bash
   cp .env.example .env
   ```
4. Suba a infraestrutura necessária (BD, Redis):
   ```bash
   docker compose up -d db redis
   ```
5. Inicie o servidor FastAPI:
   ```bash
   uvicorn src.compliance_gate.main:app --reload --host 0.0.0.0 --port 8000
   ```

## Comandos Úteis (via Pip / Pyproject)
Após instalar com `.[dev]`:
- Format: `ruff format .`
- Linting: `ruff check .`
- Testes: `pytest`
- Type checking: `mypy .`

## Reteste end-to-end de auth
```bash
python retests/scripts/run_auth_retests.py
```

## Reteste RBAC (auth + isolamento multi-tenant)
```bash
python retests/scripts/run_rbac_retests.py
```

## Engine Core v1
- API: `/api/v1/engine/*`
- Materialização: `POST /api/v1/engine/materialize/machines`
- Report obrigatório: `machines_status_summary`

## Ingest de datasets (machines)
- Endpoint: `POST /api/v1/datasets/machines/ingest`
- Quando `profile_ids` for enviado, cada perfil deve existir no tenant e ter payload ativo; caso contrário a API retorna `400` com mensagem curta de diagnóstico.

Reteste e2e da Engine:
```bash
python retests/scripts/run_engine_retests.py
```
