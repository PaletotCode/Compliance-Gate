# Frontend Maintainability Audit - 2026-03-11

## 1) Escopo e metodologia

Escopo analisado:
- `frontend/src` completo (86 arquivos)
- `frontend/scripts` e `frontend/e2e`
- configuracoes (`eslint`, `tsconfig`, `vite`)
- estrutura de pastas em `frontend/`

Metodo:
- Inventario de tamanho/complexidade por arquivo
- Revisao de acoplamento (state + api + UI)
- Busca de duplicacoes (unwrap, parseJson, strings de endpoint, tokens de estilo)
- Busca de codigo legado/nao referenciado
- Execucao de `npm run lint` para evidencia objetiva

Resumo rapido:
- Codigo total mapeado (TS/TSX/CSS): 10.647 linhas
- Hotspots (arquivos maiores):
  - `mainViewStore.ts` (1099)
  - `AuthenticationCanvas.tsx` (938)
  - `engineStudioStore.ts` (865)
  - `RuleSetsPanel.tsx` (662)
  - `MainMenuCanvas.tsx` (475)
  - `engineStudioApi.ts` (447)

## 2) Diagnostico executivo (prioridade)

### P0 - alto risco de manutencao e regressao

1. Monolitos de estado com responsabilidades misturadas
- Arquivos:
  - `frontend/src/main_view/state/mainViewStore.ts`
  - `frontend/src/engine_studio/state/engineStudioStore.ts`
- Problema:
  - Store faz tudo: estado + regras de negocio + IO de API + mensagens de erro + orquestracao de fluxo.
  - Alta chance de regressao em mudancas pequenas.
- Evidencia:
  - Main View: lista de acoes extensa em `mainViewStore.ts:59-95`; implementacao unica em `mainViewStore.ts:420-1099`.
  - Engine Studio: estado + CRUD + preview/run + diagnostics + ruleset lifecycle em `engineStudioStore.ts:108-834`.
- Centralizar:
  - Separar por use-case/service (ex.: `services/pipeline`, `services/profiles`, `services/table`) e manter a store apenas como orquestradora de estado.

2. Persistencia de configuracao de perfil incompleta (causa perda de configuracao em troca de aba)
- Arquivo:
  - `frontend/src/main_view/state/mainViewStore.ts`
- Problema:
  - So existe persistencia de `dataset_version_id` em localStorage.
  - Nao existe persistencia de drafts por fonte antes do `save`.
  - Reabrir fonte recarrega payload do backend e sobrescreve alteracoes locais nao salvas.
- Evidencia:
  - Persistencia atual limitada: `mainViewStore.ts:144-157`.
  - Recarregamento ao abrir aba: `mainViewStore.ts:474-499`.
- Centralizar:
  - Modulo unico de `draftPersistence` por `sourceId` (versao local + status dirty + reconciliacao com perfil salvo).

3. `AuthenticationCanvas` concentrando fluxo, UI, infra e estilos globais
- Arquivo:
  - `frontend/src/auth/ui/AuthenticationCanvas.tsx`
- Problema:
  - Arquivo gigante com subcomponentes internos, fluxo de login/MFA/reset, health probing, notificacoes, modal, estilos globais inline.
  - Dificulta teste, evolucao e correcoes.
- Evidencia:
  - Componente principal de `211` ate `910`.
  - Health probe embutido: `302-340`.
  - Estilo global inline via `<style>`: `885-907`.
  - `catch {}` silencioso: `397`, `440`, `495`.
- Centralizar:
  - Separar em: `auth-flow-controller`, `auth-health`, `auth-presentational-steps`, `auth-theme-tokens`.

4. `MainMenuCanvas` acoplado demais ao estado global e com UI hardcoded
- Arquivo:
  - `frontend/src/main_view/ui/MainMenuCanvas.tsx`
- Problema:
  - Seleciona store inteira (`mainViewStore()`) e concentra regras de TI_ADMIN/DIRECTOR, navegacao de modos e renderizacao completa.
  - Hardcode de identidade de usuario no header.
- Evidencia:
  - Store sem selector: `MainMenuCanvas.tsx:75`.
  - Texto fixo de usuario: `MainMenuCanvas.tsx:455-462`.
- Centralizar:
  - Dividir em container por modo (`home/viewer/materialized`) + header component com dados reais do usuario.

### P1 - custo alto de evolucao

5. API layer sem centralizacao de endpoints e envelope handling
- Arquivos:
  - `frontend/src/engine_studio/api/engineStudioApi.ts`
  - `frontend/src/main_view/api/pipelineApi.ts`
  - `frontend/src/main_view/api/csvTabsApi.ts`
  - `frontend/src/api/endpoints.ts`
- Problema:
  - Endpoints hardcoded em varios arquivos.
  - `unwrap<T>` duplicado em 3 modulos.
  - Mapeamentos de erro espalhados.
- Evidencia:
  - Auth usa registry central em `endpoints.ts`, mas Engine/Main View nao.
  - `unwrap`: `engineStudioApi.ts:32`, `pipelineApi.ts:20`, `csvTabsApi.ts:18`.
- Centralizar:
  - `api/endpoints` por dominio + `api/envelope.ts` + `api/error-adapter.ts` unico.

6. Painel Engine Studio com JSON CRUD repetitivo (parse/validacao/salvar)
- Arquivos:
  - `frontend/src/engine_studio/panels/TransformationsPanel.tsx`
  - `frontend/src/engine_studio/panels/SegmentsPanel.tsx`
  - `frontend/src/engine_studio/panels/ViewsPanel.tsx`
  - `frontend/src/engine_studio/panels/RuleSetsPanel.tsx`
- Problema:
  - Mesmo padrao repetido: form local + `parseJson` + erro local + save/preview.
  - Duplicacao eleva custo para padronizar UX e tratamento de erro.
- Evidencia:
  - `parseJson` repetido nos 4 paineis.
- Centralizar:
  - Hook/factory compartilhado (`useJsonEditorForm`) com parse seguro, dirty-state, validacao e erro padrao.

7. Engine Studio Dock com fan-out de selectors e wiring manual
- Arquivo:
  - `frontend/src/engine_studio/components/EngineStudioDock.tsx`
- Problema:
  - Muitas subscriptions Zustand e muito wiring manual para props.
- Evidencia:
  - Selecao de estado/acoes extensa: `14-74`.
- Centralizar:
  - Seletores compostos com `useShallow` + facade por painel (`useCatalogPanelModel`, etc.).

8. Duplicacao de logica de filtro de tabela
- Arquivos:
  - `frontend/src/main_view/components/table/SourceDataTable.tsx`
  - `frontend/src/main_view/components/table/MachinesVirtualGrid.tsx`
- Problema:
  - Normalizacao de celula, selecao de valores unicos e aplicacao de filtros repetidos.
- Evidencia:
  - `normalizeCellValue` vs `formatCellValue` e filtro local em ambos arquivos.
- Centralizar:
  - `tableFiltering.ts` compartilhado (normalizacao, unique options, predicate builder).

9. Design tokens hardcoded e repetidos
- Arquivos impactados:
  - praticamente todo `main_view`, `auth`, `engine_studio`
- Problema:
  - Cor e classes repetidas (`#00AE9D` e variacoes) em massa.
- Evidencia:
  - 131 ocorrencias de tokens visuais hardcoded encontradas.
- Centralizar:
  - `styles/tokens.css` + helper de classes por variante para reduzir string copy/paste.

10. Conjunto de scripts de reteste com codigo repetido
- Arquivos:
  - `frontend/scripts/auth_flow_check.ts`
  - `frontend/scripts/main_view_profile_check.ts`
  - `frontend/scripts/main_view_full_flow_check.ts`
- Problema:
  - `CookieJar`, `callJson`, parsing e validacoes repetidos.
- Centralizar:
  - `frontend/scripts/lib/http-session.ts` + `frontend/scripts/lib/assertions.ts`.

### P2 - limpeza e consistencia

11. Arquivos legados/nao utilizados
- Arquivos sem referencia de import no `src`:
  - `frontend/src/main_view/components/table/MaterializedDataTable.tsx`
  - `frontend/src/main_view/components/panels/MachinesSummaryFilters.tsx`
  - `frontend/src/main_view/components/panels/StatusMultiSelectPopover.tsx`
- Outros:
  - `frontend/src/assets/react.svg` (nao usado)
  - `frontend/src/main_view/mocks/mockData.ts` tem exportacoes nao usadas (`MOCK_DATA`, `DEFAULT_ACTIVE_MAT_COLS`, `getSourceColumns`)
- Acao:
  - mover para `legacy/` com data de deprecacao ou remover.

12. Estrutura de pastas com ruido
- Encontrado:
  - pasta vazia `frontend/frontend/temp`
  - pasta `frontend/temp` com rascunhos nao-produtivos
- Risco:
  - ambiguidade para novos devs/IA e para tooling.
- Acao:
  - padronizar area de rascunho em um unico local (ex.: `docs/drafts/`) e remover duplicacoes.

13. RBAC frontend pouco declarativo
- Arquivo:
  - `frontend/src/main_view/ui/MainMenuCanvas.tsx`
- Problema:
  - gating pontual por `isTiAdmin`, sem matriz central de permissoes de UI.
- Acao:
  - `auth/permissions.ts` com capacidades (`canEditSegments`, `canRunViews`, etc.).

14. Query infra presente sem uso real
- Arquivos:
  - `frontend/src/app.tsx`
  - `frontend/src/state/queryClient.ts`
- Problema:
  - `QueryClientProvider` ativo, mas sem `useQuery/useMutation` no app.
- Acao:
  - ou migrar API access para React Query de fato, ou simplificar e remover camada por enquanto.

## 3) Evidencia objetiva de qualidade (lint)

Execucao em 2026-03-11:
- comando: `npm run lint`
- status: falhou

Falhas relevantes:
1. Escopo ESLint/TSConfig inconsistente
- Arquivos fora de `src` (`scripts` e `e2e`) quebram parser com `parserOptions.project`.
- Raiz: `tsconfig.app.json` inclui apenas `src`, mas ESLint varre tudo.

2. Problemas concretos de codigo
- `auth/store.ts:143` variavel `error` nao usada.
- `AuthenticationCanvas.tsx:130` `setState` em effect (aviso de padrao fragil).
- `AuthenticationCanvas.tsx:397,440,495` `catch {}` vazio.
- `engine_studio/builders/nodePath.ts:10` escape desnecessario.
- `main_view/api/pipelineApi.ts:153-154` escapes desnecessarios.

3. Aviso de compatibilidade
- `MachinesVirtualGrid.tsx:175` aviso de biblioteca incompativel com React Compiler.

## 4) Arquivos com maior dificuldade de manutencao (levantamento)

### Criticos
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/main_view/state/mainViewStore.ts`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/engine_studio/state/engineStudioStore.ts`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/auth/ui/AuthenticationCanvas.tsx`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/main_view/ui/MainMenuCanvas.tsx`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/engine_studio/panels/RuleSetsPanel.tsx`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/engine_studio/api/engineStudioApi.ts`

### Alto impacto
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/engine_studio/components/EngineStudioDock.tsx`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/engine_studio/panels/ViewsPanel.tsx`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/engine_studio/panels/SegmentsPanel.tsx`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/engine_studio/panels/TransformationsPanel.tsx`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/main_view/components/table/MachinesVirtualGrid.tsx`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/main_view/api/pipelineApi.ts`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/main_view/api/csvTabsApi.ts`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/main_view/api/schemas.ts`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/engine_studio/types/contracts.ts`

### Legado/limpeza
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/main_view/components/table/MaterializedDataTable.tsx`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/main_view/components/panels/MachinesSummaryFilters.tsx`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/main_view/components/panels/StatusMultiSelectPopover.tsx`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/main_view/mocks/mockData.ts`
- `/Users/pedro.torres/Documents/Compliance Gate/frontend/src/assets/react.svg`

## 5) O que centralizar para facilitar configuracao, parametrizacao e manutencao

1. API contracts e endpoints
- Criar registry unico por dominio:
  - `src/api/endpoints/auth.ts`
  - `src/api/endpoints/main-view.ts`
  - `src/api/endpoints/engine.ts`
- Criar normalizador unico:
  - `src/api/envelope.ts` (`unwrapApiData`)
  - `src/api/error.ts` (`toHumanError`)

2. Permissoes de UI (RBAC)
- Criar `src/auth/permissions.ts` com matriz declarativa por role.
- UI passa a checar capacidade (ex.: `canEditProfiles`) em vez de `role === 'TI_ADMIN'` hardcoded.

3. Persistencia de estado local
- Criar `src/main_view/state/persistence/` para:
  - drafts de perfil por source
  - dataset scope atual
  - versao de schema local para migracao de estado

4. Tokens de design e componentes base
- Centralizar tokens no CSS:
  - `src/styles/tokens.css`
- Criar componentes base para formularios (Input, Select, SectionCard), reduzindo repeticao de classes.

5. JSON editor/validator compartilhado
- Criar `src/engine_studio/builders/form-core/` com:
  - parse seguro
  - deteccao de dirty state
  - assinatura de erro padrao
  - helper de payload default

6. Tabela e filtros
- Criar utilitario unico de filtragem em `src/main_view/table/filtering.ts`.
- Reuso em SourceDataTable + MachinesVirtualGrid.

7. Scripts de reteste
- Criar biblioteca comum em `frontend/scripts/lib/` para cookie jar, request, csrf e asserts.

## 6) Organizacao de pastas e nome de arquivos (proposta)

Objetivo: arquitetura legivel para humanos e IA, baixa entropia.

Sugestao de padrao por feature:
- `src/features/auth/`
  - `api/`, `model/`, `ui/`, `hooks/`, `lib/`
- `src/features/main-view/`
  - `api/`, `model/`, `ui/`, `table/`, `panels/`, `lib/`
- `src/features/engine-studio/`
  - `api/`, `model/`, `ui/`, `builders/`, `diagnostics/`, `lib/`

Regras de naming:
- pastas: `kebab-case`
- arquivos de componente: `PascalCase.tsx`
- arquivos de estado/logica: `camelCase.ts`
- sem nomes duplicados de contexto (`frontend/frontend/temp`)

## 7) Overengineering vs underengineering (equilibrio recomendado)

Hoje ha dois extremos simultaneos:
1. Overengineering local:
- componentes/stores muito grandes com muitas responsabilidades.

2. Underengineering estrutural:
- ausencia de modulos centrais para endpoints, erros, permissoes, persistencia de draft.

Direcao correta:
- Componentizar por responsabilidade pequena.
- Centralizar infraestrutura transversal (API, erro, RBAC, persistencia, tokens).

## 8) Plano pratico de evolucao (sem reescrever tudo de uma vez)

Fase 1 - higiene estrutural (rapida)
- Corrigir lint e escopo TS/ESLint.
- Remover/arquivar legado sem uso.
- Consolidar pastas temporarias.
- Centralizar endpoints + unwrap + error adapter.

Fase 2 - desacoplamento de estado
- Quebrar `mainViewStore` em slices/use-cases.
- Quebrar `engineStudioStore` por dominio (`catalog`, `transformations`, `segments`, `views`, `rulesets`, `diagnostics`).

Fase 3 - UX/state resiliente
- Implementar persistencia de draft por aba/source.
- Adicionar dirty-state explicito e resolucao de conflito local vs servidor.

Fase 4 - UI system
- Extrair tokens e primitives de formulario/tabela.
- Reduzir classes inline repetidas.

Fase 5 - observabilidade e qualidade
- Cobertura de testes por fluxo critico de store.
- Checklists automatizados para contratos de API e RBAC frontend.

## 9) Conclusao

O frontend esta funcional, mas ainda com alta dependencia de alguns arquivos monoliticos.

Para manutencao industrial (escala, equipe maior, manutencao por IA e mudancas frequentes), o principal ganho vira de:
- reduzir acoplamento em stores/componentes gigantes,
- centralizar infraestrutura transversal,
- eliminar legado morto,
- consolidar naming e estrutura de pastas,
- introduzir persistencia robusta de drafts e matriz declarativa de permissoes.
