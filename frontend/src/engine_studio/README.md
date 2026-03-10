# Engine Studio (Admin Studio)

Bounded context frontend para gestão declarativa da Engine no modo materialized do Admin Studio.

## Estrutura
- `api/`: contratos HTTP tipados para `catalog`, `transformations`, `segments`, `views`, `rulesets` e `diagnostics`.
- `state/`: store Zustand do contexto declarativo, separado do `main_view`.
- `hooks/`: bootstrap/ciclo de vida para materialized (`useEngineStudioBootstrap`).
- `components/`: shell do painel lateral (`EngineStudioDock`), tabs e erro estruturado.
- `panels/`: painéis por domínio (`Catalog`, `Transformations`, `Segments`, `Views`, `RuleSets`, `Diagnostics`).
- `builders/`: builders JSON com foco em manutenção humana/IA e destaque de `node_path`.
- `diagnostics/`: parser de erro robusto para `code/message/details/hint/node_path`.
- `types/`: contratos canônicos usados por UI/store/client.

## Decisões de arquitetura
1. **Store isolada do Main View**: estado declarativo não polui o store legado de ingest.
2. **Tabela final via View declarativa**: no modo materialized para TI_ADMIN, a grade usa `POST /engine/views/run`.
3. **Sem filtros hardcoded**: filtros por status fixo foram removidos do topo da tabela; filtragem vem de View/Segment e filtros por coluna da própria grade.
4. **Erro UX estável**: toda falha backend é normalizada em `EngineErrorPayload` e renderizada no topo do dock.
5. **Builder com node_path**: `JsonPayloadBuilder` destaca nó específico e mostra sugestões (ex.: `UnknownColumn`).

## Fluxo operacional
1. Materialize concluído -> `dataset_version_id` disponível.
2. `useEngineStudioBootstrap` carrega catálogo e entidades declarativas.
3. Se não houver View, cria `Admin Studio Table` automaticamente.
4. Seleção de View alimenta a tabela final com paginação declarativa.
5. RuleSets: validar, explicar e dry-run com payloads JSON diretamente no painel.
