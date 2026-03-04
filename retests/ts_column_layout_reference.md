# TS Column Layout Reference — dashboard_fixed.ts

Gerado pelo Reteste 2A v2. Audita o comportamento exato do arquivo `dashboard_fixed.ts` para detecção de header row, colunas obrigatórias/opcionais, aliases e normalização.

---

## 1 · Função `getIdx()` — Column Finder (linha 374)

```typescript
function getIdx(headerRow: string[], name: string): number {
  const target = name.toUpperCase().trim();
  return headerRow.findIndex(col => {
    const cleanCol = col.toString().trim().toUpperCase().replace(/^\uFEFF/, '');
    return cleanCol === target;
  });
}
```

**Estratégia:**
- Comparação EXATA após `.toUpperCase().trim()` e remoção de BOM (`\uFEFF`)
- Case-insensitive mas sem fuzzy matching
- Sem normalização de acentos no `getIdx` — apenas `toUpperCase()`
- Sem fallback por índice fixo para AD/UEM/EDR/ASSET

---

## 2 · Função `normalize()` — Key Normalizer (linha 2975, inner function de `main()`)

```typescript
const normalize = (s: string) => {
  let out = s.toString().trim().toUpperCase();
  out = out.replace(/\..*$/, '');  // Remove TUDO a partir do primeiro ponto
  return out;
};
```

**⚠️ Diferença crítica vs v1 do script Python:**
- TS usa `.replace(/\..*$/, '')` → remove da **primeira** ocorrência de `.` em diante
- v1 Python: `_strip_ext()` só removia `.SCR2008...` especificamente
- **Impacto:** `SIC_00_4349_01.scr2008.local` → TS vira `SIC_00_4349_01`, Python v1 também chegava lá mas por outro caminho
- v2 Python: `normalize_key()` usa `s[:dot]` onde `dot = s.find('.')` — idêntico

---

## 3 · Função `normalizeAssetHostname()` (linha 344)

```typescript
function normalizeAssetHostname(raw: string): string {
  let s = raw.toString().trim();
  const marker = ".SCR2008";
  const idx = s.toUpperCase().indexOf(marker);
  if (idx !== -1) { s = s.substring(0, idx); }
  else if (s.includes(".")) { s = s.split(".")[0]; }
  return normalizeSicFromExtra(s);  // → extrai "SIC_XX_XXXX_XX" prefix
}
```

Usada APENAS para a `assetSet` — **não** para o join de AD/UEM/EDR.

---

## 4 · AD — Fonte e Layout

| Item | Valor | Evidência TS |
|------|-------|-------------|
| Sheet | `AD` (workbook) | `workbook.getWorksheet("AD")` (linha 2925) |
| Header row | **Linha 0** (fixo) | `hAD = dataAD[0]` (linha 2936) |
| Data inicia | Linha 1 | `for (i=1; i<dataAD.length; i++) upsert(...)` (linha 3095) |
| Col obrigatória (key) | `Computer Name` | `idxAdName = getIdx(hAD, "Computer Name")` (linha 2941) |
| Col opcional | `Last Logon Time` | `idxAdLogon = getIdx(hAD, "Last Logon Time")` (linha 2942) |
| Col opcional | `Password Last Set` | `idxAdPwdSet = getIdx(hAD, "Password Last Set")` (linha 2943) |
| Col opcional | `Operating System` | `idxAdOs = getIdx(hAD, "Operating System")` (linha 2944) |
| Aliases | Nenhum (apenas fallback-less) | — |

**Nota:** `AD.csv` atual NÃO possui `Last Logon Time` nem `Password Last Set`. O TS trata graciosamente (`val(idx)` retorna `""` quando idx=-1). **Não é bloqueante.**

---

## 5 · UEM — Fonte e Layout

| Item | Valor | Evidência TS |
|------|-------|-------------|
| Sheet | `UEM` (workbook) | `workbook.getWorksheet("UEM")` (linha 2926) |
| Header row | **Linha 0** (fixo) | `hUEM = dataUEM[0]` (linha 2937) |
| Data inicia | Linha 1 | `for (i=1; i<dataUEM.length; i++) upsert(...)` (linha 3096) |
| Col key (alias 1) | `Hostname` | `idxUemName = getIdx(hUEM, "Hostname")` (linha 2947) |
| Col key (alias 2) | `Friendly Name` | `if (idxUemName === -1) idxUemName = getIdx(hUEM, "Friendly Name")` (linha 2948) |
| Col | `Username` | linha 2950 |
| Col | `Serial Number` | linha 2951 |
| Col | `Last Seen` | linha 2952 |
| Col | `DM Last Seen` | linha 2953 |
| Col | `OS` | linha 2954 |
| Col | `Model` | linha 2955 |

**Nota:** UEM.csv atual usa `Friendly Name` (não `Hostname`). `getIdx` resolve corretamente.

---

## 6 · EDR — Fonte e Layout

| Item | Valor | Evidência TS |
|------|-------|-------------|
| Sheet | `EDR` (workbook) | `workbook.getWorksheet("EDR")` (linha 2927) |
| Header row | **Linha 0** (fixo) | `hEDR = dataEDR[0]` (linha 2938) |
| Data inicia | Linha 1 | `for (i=1; i<dataEDR.length; i++) upsert(...)` (linha 3097) |
| Col key (alias 1) | `Friendly Name` | `idxEdrName = getIdx(hEDR, "Friendly Name")` (linha 2958) |
| Col key (alias 2) | `Hostname` | `if (idxEdrName === -1) idxEdrName = getIdx(hEDR, "Hostname")` (linha 2959) |
| Col | `Last Logged In User Account` | linha 2964 |
| Col | `Last Seen` | linha 2965 |
| Col | `Last User Account Login` | linha 2966 |
| Col | `Serial Number` | linha 2967 |
| Col | `OS Version` | linha 2968 |
| Col | `Local IP` | linha 2969 |
| Col | `Sensor Tags` | linha 2970 |
| Col | `Chassis` | linha 2971 |

**Nota:** EDR.csv atual usa `Hostname` (não `Friendly Name`). `getIdx` com fallback resolve.

---

## 7 · ASSET — Lookup Set (não adiciona entradas ao MasterMap)

| Item | Valor | Evidência TS |
|------|-------|-------------|
| Função | `loadAssetSet()` | linha 2819 |
| Header row | **Dinâmico** (scan por `NOME DO ATIVO`) | linhas 2843–2854 |
| Fallback | linha 0 se `NOME DO ATIVO` estiver em `data[0]` | linhas 2860–2865 |
| Data inicia | `headerRow + 1` | linha 2874 |
| Col key | `Nome do ativo` → via `normalizeAssetHostname()` | linhas 2878–2880 |
| Retorno | `Set<string>` de hostname keys | linha 2884 |

**ASSET.CSV — estrutura observada:**
```
Linha 0: ,Sicoob Central Rondon,...
Linha 1: (vazio)
Linha 2: "Gerada por Murilo..." 
Linha 3: Total de registros: 202,...
Linha 4: Produto,Local,...,Nome do ativo,Estado do ativo,...   ← HEADER (detectado pelo scan)
Linha 5+: dados reais
```

**Aplicação no MasterMap (linha 3196–3198):**
```typescript
for (const [k, m] of masterMap.entries()) {
  const base = normalizeSicFromExtra(k);
  m.sources.ASSET = assetSet.has(k) || (base ? assetSet.has(base) : false);
}
```
→ ASSET **nunca adiciona** novas entradas. Apenas marca `sources.ASSET=true`.

---

## 8 · `AVAILABLE` / GAP — Somente Entradas Virtuais

**`AVAILABLE` e `GAP` no TS são EXCLUSIVAMENTE para entradas `isVirtualGap=true`.**

```typescript
// linha 3283
for (const m of masterMap.values()) {
  if (m.isVirtualGap) {
    stats.details["AVAILABLE"]++;
    // ... buildAvailableRow(m.name)
    continue;  // ← pula toda a classificação real
  }
  stats.total++;
  let statusKey = "COMPLIANT";  // ← apenas para entradas reais
  ...
}
```

Virtual GAPs são criados na linha 3169 quando há salto numérico na sequência de hostnames. **Não estão presentes nos dados reais dos CSVs atuais.**

---

## 9 · Status Key: `COMPLIANT` (não `OK`)

O TS usa a string `"COMPLIANT"` como status padrão para máquinas saudáveis:

```typescript
// linha 3304
let statusKey = "COMPLIANT";
let statusTxt = "✅ SEGURO";
```

A v1 do script Python usava `"OK"`. A v2 foi corrigida para `"COMPLIANT"`.

---

## 10 · Universo do MasterMap

**O MasterMap é populado APENAS por AD + UEM + EDR:**

```typescript
for (let i = 1; i < dataAD.length; i++) upsert(dataAD[i], "AD", idxAdName);    // linha 3095
for (let i = 1; i < dataUEM.length; i++) upsert(dataUEM[i], "UEM", idxUemName); // linha 3096
for (let i = 1; i < dataEDR.length; i++) upsert(dataEDR[i], "EDR", idxEdrName); // linha 3097
```

ASSET NÃO está nos loops de `upsert()`. Entra apenas como lookup após o join.

---

_Gerado em: {{ run_time }} — dash analysis: dashboard_fixed.ts (4164 linhas)_
