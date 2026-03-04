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
