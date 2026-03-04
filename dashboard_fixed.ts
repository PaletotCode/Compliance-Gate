interface MachineData {
  name: string;
  paCode: string;
  sources: { AD: boolean; UEM: boolean; EDR: boolean; ASSET: boolean };
  data: {
    adLogon: string; adPwdSet: string; adOs: string;
    uemUser: string; uemSeen: string; uemDmSeen: string; uemSerial: string; uemOs: string;
    edrUser: string;
    edrSeen: string;
    edrLogin: string;
    edrSerial: string;
    edrOs: string;
    edrChassis: string;
    model: string; ip: string; tags: string;

    uemExtraUserLogado: string;
    uemExtraCheckWin11: string;
    uemExtraChassis: string;

    lastSeenScore: number;
    lastSeenSource: "AD" | "EDR" | "UEM" | "";


  };
  isVirtualGap: boolean;
}

interface PaInfo {
  code: string;
  name: string;
  city: string;
  fullName: string;
}

interface ProcessedRow {
  arr: (string | number | boolean)[];
  colorCategory: string;
  priority: number;
  paColor: string;
  lastSignalFromEdr?: boolean;
  lastSignalFromUem?: boolean;
  isLegacy?: boolean;
  paMismatch?: boolean;


  verifiedSuggested?: boolean;
}



interface Stats {
  total: number;
  compliant: number;
  missingUem: number; missingEdr: number; missingAsset: number;
  phantom: number; rogue: number;
  swap: number; clone: number; legacy: number; offline: number;
  gap: number; available: number;
  details: { [key: string]: number };
}

interface AppConfig {
  filters: {
    compliant: boolean;
    missingUem: boolean;
    missingEdr: boolean;
    missingAsset: boolean;
    phantom: boolean;
    rogue: boolean;
    swap: boolean;
    clone: boolean;
    legacy: boolean;
    offline: boolean;
    gap: boolean;
    available: boolean;
    inconsistency: boolean;
    paMismatch: boolean;
  };

  searchTerm: string;
  paFilter: string;
  optionalCols: {
    model: boolean; ip: boolean; tags: boolean; sources: boolean; chassis: boolean;
    status_usuariologado: boolean;
    status_check_win11: boolean;
    sinal_individual: boolean;
    ultimo_sinal: boolean;
  };



  legacyDefinitions: string[];
  modules: {
    gapLimit: number; cloneEnabled: boolean; staleDays: number;
    printersEnabled: boolean;
  };
}



const CONSTANTS = {
  SHEET_MENU: "DASHBOARD",
  SHEET_OUTPUT: "RELATORIO_FINAL",
  SHEET_PA_DB: "BASE_PA",
  SHEET_PRINTERS: "IMPRESSORAS",


  SHEET_COLABORADORES: "COLABORADORES",
  SHEET_VERIFIED_USERS: "USUARIOS_VERIFICADOS",

  COLORS: {
    bg: "#FFFFFF",
    primary: "#003641",
    accent: "#00AE9D",
    textMain: "#1E293B",
    textMuted: "#64748B",
    cardBg: "#F0FDFA",
    risk: "#DC2626",
    warning: "#D97706",
    success: "#166534",
    inputBg: "#FEF9C3",
  }
};

function sourceIcon(v: boolean): string {
  return v ? "✅" : "❌";
}


// ===== Helpers for safer rendering / Office Scripts quirks =====
const EXCEL_LIMITS = { MAX_ROWS: 1048576, MAX_COLS: 16384 };

function clampNumber(n: number, min: number, max: number): number {
  if (n < min) return min;
  if (n > max) return max;
  return n;
}

/**
 * Delete any table in the workbook with a given name (table names are global).
 * This avoids failures when re-creating output tables.
 */
function deleteTableByNameIfExists(workbook: ExcelScript.Workbook, tableName: string) {
  try {
    const tables = workbook.getTables();
    for (const t of tables) {
      if (t.getName() === tableName) t.delete();
    }
  } catch (e) { }
}

/**
 * Sets a table name safely (table names are global). If the desired name is taken,
 * we delete the old table, then try again. As a last resort we create a unique name.
 */
function safeSetTableName(workbook: ExcelScript.Workbook, table: ExcelScript.Table, desiredName: string) {
  try {
    deleteTableByNameIfExists(workbook, desiredName);
    table.setName(desiredName);
  } catch (e) {
    try {
      table.setName(`${desiredName}_${new Date().getTime()}`);
    } catch (e2) { }
  }
}

/**
 * Clears the output area using indexes (safer than A1-notation when usedRange is huge).
 * startRow0/startCol0 are 0-based indices used by getRangeByIndexes.
 */
function safeClearOutputArea(sheet: ExcelScript.Worksheet, startRow0: number, startCol0: number, minCols: number) {
  try {
    const used = sheet.getUsedRange();
    const usedRow0 = used ? used.getRowIndex() : 0;
    const usedCol0 = used ? used.getColumnIndex() : 0;
    const usedRows = used ? used.getRowCount() : 0;
    const usedCols = used ? used.getColumnCount() : 0;

    const usedLastRow0 = used ? (usedRow0 + usedRows - 1) : startRow0;
    const usedLastCol0 = used ? (usedCol0 + usedCols - 1) : (startCol0 + minCols - 1);

    const clearStartRow0 = clampNumber(startRow0 + 1, 0, EXCEL_LIMITS.MAX_ROWS - 1);
    const clearStartCol0 = clampNumber(startCol0, 0, EXCEL_LIMITS.MAX_COLS - 1);

    const clearLastRow0 = clampNumber(usedLastRow0 + 300, clearStartRow0, EXCEL_LIMITS.MAX_ROWS - 1);
    const clearLastCol0 = clampNumber(Math.max(usedLastCol0 + 5, startCol0 + minCols + 5), clearStartCol0, EXCEL_LIMITS.MAX_COLS - 1);

    const rowCount = (clearLastRow0 - clearStartRow0) + 1;
    const colCount = (clearLastCol0 - clearStartCol0) + 1;

    sheet.getRangeByIndexes(clearStartRow0, clearStartCol0, rowCount, colCount)
      .clear(ExcelScript.ClearApplyTo.all);
  } catch (e) { }
}
// =============================================================


function paintToggleRange(sheet: ExcelScript.Worksheet, address: string) {
  const r = sheet.getRange(address);
  const v = r.getTexts();

  for (let i = 0; i < v.length; i++) {
    for (let j = 0; j < v[i].length; j++) {
      const cell = r.getCell(i, j);
      const t = (v[i][j] || "").toString().trim().toUpperCase();

      const on = (t === "X" || t === "S" || t === "SIM" || t === "YES");
      if (on) {
        cell.getFormat().getFill().setColor("#DCFCE7");
        cell.getFormat().getFont().setColor("#166534");
        cell.getFormat().getFont().setBold(true);
      } else {
        cell.getFormat().getFill().setColor("#FFFFFF");
        cell.getFormat().getFont().setColor(CONSTANTS.COLORS.textMain);
        cell.getFormat().getFont().setBold(false);
      }

      cell.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);
    }
  }
}







function extractUserPaSuffix(userRaw: string): string {
  if (!userRaw) return "";
  let u = userRaw.toString().trim();


  if (u.includes("\\")) u = (u.split("\\").pop() || "").trim();


  const m = u.match(/_(\d{1,2})$/);
  if (!m) return "";
  return m[1].padStart(2, "0");
}


function parseDateScore(dateVal: string): number {
  if (!dateVal) return 0;
  const dateStr = dateVal.toString().trim();
  if (dateStr === "" || dateStr === "N/A") return 0;

  const hasAmPm = /\b(AM|PM)\b/i.test(dateStr);


  if (hasAmPm) {
    const usRegex = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{1,2})(:(\d{1,2}))?\s*(AM|PM)\s*$/i;
    const mUs = dateStr.match(usRegex);

    if (mUs) {
      const month = parseInt(mUs[1], 10);
      const day = parseInt(mUs[2], 10);
      const year = parseInt(mUs[3], 10);

      let hour = parseInt(mUs[4], 10);
      const minute = parseInt(mUs[5], 10);
      const second = mUs[7] ? parseInt(mUs[7], 10) : 0;

      const ampm = (mUs[8] || "").toUpperCase();
      if (ampm === "PM" && hour < 12) hour += 12;
      if (ampm === "AM" && hour === 12) hour = 0;

      return new Date(year, month - 1, day, hour, minute, second).getTime();
    }
  }


  const ptRegex = /^(\d{1,2})\/(\d{1,2})\/(\d{4})\s+(\d{1,2}):(\d{1,2})(:(\d{1,2}))?\s*$/;
  const mPt = dateStr.match(ptRegex);

  if (mPt) {
    return new Date(
      parseInt(mPt[3], 10),
      parseInt(mPt[2], 10) - 1,
      parseInt(mPt[1], 10),
      parseInt(mPt[4], 10),
      parseInt(mPt[5], 10),
      mPt[7] ? parseInt(mPt[7], 10) : 0
    ).getTime();
  }

  return 0;
}



function getDetailedDuration(msScore: number, now: number): string {
  if (msScore <= 0) return "-";
  let diff = now - msScore;
  if (diff < 0) diff = 0;
  const oneHour = 1000 * 60 * 60;
  const oneDay = oneHour * 24;
  return (Math.floor(diff / oneDay) === 0) ? `${Math.floor(diff / oneHour)}h atrás` : `${Math.floor(diff / oneDay)} dias`;
}

function getPaColor(paCode: string): string {
  if (!paCode || paCode === "??") return "#FFFFFF";
  const palette = [
    "#FFADAD", "#FFD6A5", "#FDFFB6", "#CAFFBF", "#9BF6FF",
    "#A0C4FF", "#BDB2FF", "#FFC6FF", "#FF9AA2", "#E2F0CB",
    "#B5EAD7", "#C7CEEA", "#F49AC2", "#84DCC6", "#FDFD96",
    "#8EECF5", "#CFBAF0", "#F1C0E8", "#98F5E1", "#FFDAC1"
  ];
  let hash = 0;
  for (let i = 0; i < paCode.length; i++) { hash = paCode.charCodeAt(i) + ((hash << 5) - hash); }
  const index = Math.abs(hash) % palette.length;
  return palette[index];
}
function getStableTagColor(key: string): string {
  if (!key) return "#FFFFFF";
  const palette = [
    "#FFADAD", "#FFD6A5", "#FDFFB6", "#CAFFBF", "#9BF6FF",
    "#A0C4FF", "#BDB2FF", "#FFC6FF", "#FF9AA2", "#E2F0CB",
    "#B5EAD7", "#C7CEEA", "#F49AC2", "#84DCC6", "#FDFD96",
    "#8EECF5", "#CFBAF0", "#F1C0E8", "#98F5E1", "#FFDAC1"
  ];

  const s = key.toString().trim().toUpperCase();
  let hash = 0;
  for (let i = 0; i < s.length; i++) { hash = s.charCodeAt(i) + ((hash << 5) - hash); }
  const index = Math.abs(hash) % palette.length;
  return palette[index];
}


function normalizeKey(s: string): string {
  if (!s) return "";
  let out = s.toString().trim().toUpperCase();
  out = out.replace(/\..*$/, "");
  return out;
}

function normalizeSicFromExtra(s: string): string {
  const base = normalizeKey(s);
  const m = base.match(/^(SIC_\d+_\d+_\d+)/i);
  return m ? m[1].toUpperCase() : base;
}


function normalizeAssetHostname(raw: string): string {
  if (!raw) return "";
  let s = raw.toString().trim();

  const upper = s.toUpperCase();
  const marker = ".SCR2008";
  const idx = upper.indexOf(marker);

  if (idx !== -1) {
    s = s.substring(0, idx);
  } else if (s.includes(".")) {
    s = s.split(".")[0];
  }

  return normalizeSicFromExtra(s);
}

function parseUserLogado(raw: string): string {
  if (!raw) return "";
  const s = raw.toString().trim();
  const m = s.match(/Usuario\s+logado:\s*(.+)$/i);
  if (!m) return "";
  const after = m[1].trim();

  return after.includes("\\") ? (after.split("\\").pop() || "").trim() : after;
}




function getIdx(headerRow: string[], name: string): number {
  if (!headerRow || headerRow.length === 0) return -1;
  const target = name.toUpperCase().trim();
  return headerRow.findIndex(col => {
    if (!col) return false;

    const cleanCol = col.toString().trim().toUpperCase().replace(/^\uFEFF/, '');
    return cleanCol === target;
  });
}





type VerifiedStatus = "SUGESTAO" | "CONFIRMADO";
interface VerifiedEntry {
  user: string;
  status: VerifiedStatus;
  updatedAt: string;
}

function normalizeMachineKey(raw: string): string {
  if (!raw) return "";
  let out = raw.toString().trim().toUpperCase();
  out = out.replace(/\..*$/, "");
  return out;
}

function stripDiacritics(s: string): string {
  if (!s) return "";

  return s.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
}

function onlyLettersLower(s: string): string {
  if (!s) return "";
  return stripDiacritics(s).toLowerCase().replace(/[^a-z]/g, "");
}

function colNumberToLetter(col1Based: number): string {
  let n = col1Based;
  let s = "";
  while (n > 0) {
    const mod = (n - 1) % 26;
    s = String.fromCharCode(65 + mod) + s;
    n = Math.floor((n - 1) / 26);
  }
  return s;
}

function loadCollaborators(workbook: ExcelScript.Workbook): { names: string[]; listFormula: string } {
  const sheet = workbook.getWorksheet(CONSTANTS.SHEET_COLABORADORES);
  if (!sheet) return { names: [], listFormula: "" };

  const used = sheet.getUsedRange();
  if (!used) return { names: [], listFormula: "" };

  const data = used.getTexts();
  if (!data || data.length < 2) return { names: [], listFormula: "" };

  const h = data[0];
  const idxNome = getIdx(h, "Nome civil do colaborador");
  if (idxNome === -1) return { names: [], listFormula: "" };

  const names: string[] = [];


  let lastNonEmptyRow0 = 0;
  for (let r = data.length - 1; r >= 1; r--) {
    const v = (data[r] && data[r][idxNome]) ? data[r][idxNome].toString().trim() : "";
    if (v) { lastNonEmptyRow0 = r; break; }
  }
  if (lastNonEmptyRow0 < 1) return { names: [], listFormula: "" };

  for (let r = 1; r <= lastNonEmptyRow0; r++) {
    const v = (data[r] && data[r][idxNome]) ? data[r][idxNome].toString().trim() : "";
    if (v) names.push(v);
  }


  const colLetter = colNumberToLetter(idxNome + 1);
  const endRowExcel = lastNonEmptyRow0 + 1;
  const listFormula = `=${CONSTANTS.SHEET_COLABORADORES}!$${colLetter}$2:$${colLetter}$${endRowExcel}`;

  return { names, listFormula };
}

function pickSecondNameToken(tokens: string[]): string {
  if (!tokens || tokens.length < 2) return "";
  const stop = new Set(["DA", "DE", "DO", "DAS", "DOS", "E"]);
  for (let i = 1; i < tokens.length; i++) {
    const t = tokens[i].trim().toUpperCase();
    if (t && !stop.has(t)) return tokens[i].trim();
  }
  return tokens[1].trim();
}







function buildUserPrefixMap(collabNames: string[]): Map<string, string> {
  const out = new Map<string, string>();

  type Info = { full: string; first: string; second: string };
  const infos: Info[] = [];

  for (const full of collabNames) {
    const clean = (full || "").toString().trim();
    if (!clean) continue;

    const tokens = clean.split(/\s+/).filter(t => (t ?? "").toString().trim() !== "");
    const first = tokens.length >= 1 ? tokens[0].trim() : "";
    const second = pickSecondNameToken(tokens);


    const firstNorm = onlyLettersLower(first);
    const secondNorm = onlyLettersLower(second);

    if (!firstNorm) continue;

    infos.push({ full: clean, first: firstNorm, second: secondNorm });
  }


  const groups = new Map<string, Info[]>();
  for (const i of infos) {
    if (!groups.has(i.first)) groups.set(i.first, []);
    groups.get(i.first)!.push(i);
  }

  for (const [first, group] of Array.from(groups.entries())) {


    for (const g of group) {
      if (!g.second) {
        out.set(first, g.full);
      }
    }


    for (const g of group) {
      if (!g.second) continue;

      let chosenLen = 1;

      if (group.length > 1) {
        const maxLen = Math.max(1, g.second.length);

        for (let len = 1; len <= maxLen; len++) {
          const pref = g.second.substring(0, len);

          let count = 0;
          for (const other of group) {
            if (!other.second) continue;
            if (other.second.startsWith(pref)) count++;
          }
          if (count === 1) { chosenLen = len; break; }
          chosenLen = len;
        }
      }

      const key = first + g.second.substring(0, chosenLen);
      out.set(key, g.full);
    }
  }

  return out;
}

function extractUserPrefixBeforeAgency(rawUser: string): string {
  if (!rawUser) return "";
  let u = rawUser.toString().trim();
  if (!u) return "";


  if (u.includes("\\")) u = (u.split("\\").pop() || "").trim();

  u = u.toLowerCase().trim();
  if (!u) return "";


  const idx = u.indexOf("4349");
  if (idx <= 0) return "";


  const prefixRaw = u.substring(0, idx);
  return onlyLettersLower(prefixRaw);
}

function suggestCollaboratorNameFromUser(rawUser: string, prefixMap: Map<string, string>): string {
  const prefix = extractUserPrefixBeforeAgency(rawUser);
  if (!prefix) return "";


  const exact = prefixMap.get(prefix);
  if (exact) return exact;


  let bestName = "";
  let bestLen = 0;

  for (const [k, v] of Array.from(prefixMap.entries())) {
    if (!k) continue;

    if (prefix.startsWith(k) || k.startsWith(prefix)) {
      const common = Math.min(prefix.length, k.length);
      if (common > bestLen) {
        bestLen = common;
        bestName = v;
      }
    }
  }

  return bestName;
}

function ensureVerifiedUsersSheet(workbook: ExcelScript.Workbook): ExcelScript.Worksheet {
  let sheet = workbook.getWorksheet(CONSTANTS.SHEET_VERIFIED_USERS);
  if (!sheet) {
    sheet = workbook.addWorksheet(CONSTANTS.SHEET_VERIFIED_USERS);
    sheet.setTabColor(CONSTANTS.COLORS.primary);
  }
  return sheet;
}

function loadVerifiedUsers(workbook: ExcelScript.Workbook): Map<string, VerifiedEntry> {
  const map = new Map<string, VerifiedEntry>();
  const sheet = ensureVerifiedUsersSheet(workbook);

  const used = sheet.getUsedRange();
  if (!used) {

    sheet.getRange("A1:D1").setValues([["HOSTNAME", "USUARIO_VERIFICADO", "STATUS", "UPDATED_AT"]]);
    return map;
  }

  const data = used.getTexts();
  if (!data || data.length < 2) {
    sheet.getRange("A1:D1").setValues([["HOSTNAME", "USUARIO_VERIFICADO", "STATUS", "UPDATED_AT"]]);
    return map;
  }

  const h = data[0];
  const idxHost = getIdx(h, "HOSTNAME");
  const idxUser = getIdx(h, "USUARIO_VERIFICADO");
  const idxStatus = getIdx(h, "STATUS");
  const idxUpd = getIdx(h, "UPDATED_AT");


  if (idxHost === -1 || idxUser === -1 || idxStatus === -1 || idxUpd === -1) {
    sheet.getRange("A:Z").clear(ExcelScript.ClearApplyTo.all);
    sheet.getRange("A1:D1").setValues([["HOSTNAME", "USUARIO_VERIFICADO", "STATUS", "UPDATED_AT"]]);
    return map;
  }

  for (let r = 1; r < data.length; r++) {
    const row = data[r];
    if (!row || row.length === 0) continue;

    const hostRaw = (row[idxHost] || "").toString().trim();
    const userRaw = (row[idxUser] || "").toString().trim();
    const stRaw = (row[idxStatus] || "").toString().trim().toUpperCase();
    const upRaw = (row[idxUpd] || "").toString().trim();

    const hostKey = normalizeMachineKey(hostRaw);
    if (!hostKey) continue;
    if (!userRaw) continue;

    const status: VerifiedStatus = (stRaw === "CONFIRMADO") ? "CONFIRMADO" : "SUGESTAO";

    map.set(hostKey, {
      user: userRaw,
      status,
      updatedAt: upRaw || ""
    });
  }

  return map;
}

function saveVerifiedUsers(workbook: ExcelScript.Workbook, map: Map<string, VerifiedEntry>) {
  const sheet = ensureVerifiedUsersSheet(workbook);


  const tables = sheet.getTables();
  tables.forEach(t => t.delete());

  sheet.getRange("A:Z").clear(ExcelScript.ClearApplyTo.all);

  const header = [["HOSTNAME", "USUARIO_VERIFICADO", "STATUS", "UPDATED_AT"]];
  sheet.getRange("A1:D1").setValues(header);

  const rows: string[][] = [];
  for (const [host, v] of Array.from(map.entries())) {
    rows.push([
      host,
      v.user || "",
      v.status || "SUGESTAO",
      v.updatedAt || ""
    ]);
  }


  rows.sort((a, b) => (a[0] || "").localeCompare(b[0] || ""));

  if (rows.length > 0) {
    sheet.getRangeByIndexes(1, 0, rows.length, 4).setValues(rows);
  }

  sheet.getRange("A:D").getFormat().autofitColumns();
}

function syncVerifiedFromExistingOutput(workbook: ExcelScript.Workbook) {
  const sheet = workbook.getWorksheet(CONSTANTS.SHEET_OUTPUT);
  if (!sheet) return;


  let table: ExcelScript.Table | null = null;
  const tables = sheet.getTables();

  for (const t of tables) {
    if (t.getName() === "TabelaConformidade") { table = t; break; }
  }
  if (!table && tables.length > 0) table = tables[0];
  if (!table) return;

  const range = table.getRange();
  const data = range.getTexts();
  if (!data || data.length < 2) return;

  const h = data[0];
  const idxHost = h.findIndex(x => (x || "").toString().trim().toUpperCase() === "HOSTNAME");
  const idxVerified = h.findIndex(x => (x || "").toString().trim().toUpperCase() === "USUÁRIOS VERIFICADOS");


  if (idxHost === -1 || idxVerified === -1) return;

  const map = loadVerifiedUsers(workbook);
  const nowIso = new Date().toISOString();

  for (let r = 1; r < data.length; r++) {
    const row = data[r];
    if (!row || row.length === 0) continue;

    const hostRaw = (row[idxHost] || "").toString().trim();
    const verRaw = (row[idxVerified] || "").toString().trim();

    const hostKey = normalizeMachineKey(hostRaw);
    if (!hostKey) continue;


    if (verRaw && verRaw !== "-" && verRaw.toUpperCase() !== "N/A") {
      map.set(hostKey, {
        user: verRaw,
        status: "CONFIRMADO",
        updatedAt: nowIso
      });
    }
  }

  saveVerifiedUsers(workbook, map);
}


function resetAndDrawDashboard(sheet: ExcelScript.Worksheet): AppConfig {
  let safeFilters: string[][] = [];
  let safeLegacy: string[][] = [];
  let safeConfig: string[][] = [];
  let safeOptional: string[][] = [];
  let savedSearch = "";
  let savedPaFilter = "";
  let savedPrintersToggle = "";
  let savedTelephonyToggle = "";

  let isResetting = false;

  try {
    const vNew = sheet.getRange("B7").getText().toUpperCase().trim();
    const vOld = sheet.getRange("B3").getText().toUpperCase().trim();
    if (vNew === "X" || vNew === "S" || vOld === "X") isResetting = true;
  } catch (e) { }

  if (!isResetting) {
    try {
      const checkHeader = sheet.getRange("A9").getText();
      if (checkHeader.includes("FILTROS")) {
        savedSearch = sheet.getRange("B5").getText();
        savedPaFilter = sheet.getRange("B6").getText();
        savedPrintersToggle = sheet.getRange("B48").getText();
        try { savedTelephonyToggle = sheet.getRange("B49").getText(); } catch (e) { }
        safeFilters = sheet.getRange("B10:B23").getTexts();
        if (sheet.getRange("A24").getText().includes("LEGADO")) {
          safeLegacy = sheet.getRange("B25:B29").getTexts();
          safeConfig = sheet.getRange("B32:B34").getTexts();
          if (sheet.getRange("A36").getText().includes("OPCIONAIS")) {
            safeOptional = sheet.getRange("B37:B45").getTexts();
          }
        }
      } else {
        const oldSearch = sheet.getRange("B5").getText();
        const oldPa = sheet.getRange("B38").getText();
        try {
          savedPrintersToggle = sheet.getRange("B47").getText();
        } catch (e) { }
        if (oldSearch) savedSearch = oldSearch;
        if (oldPa && oldPa !== "🏢 FILTRAR PA:") savedPaFilter = oldPa;

        if (sheet.getRange("A6").getText().includes("FILTROS")) {
          safeFilters = sheet.getRange("B7:B17").getTexts();
          safeLegacy = sheet.getRange("B21:B25").getTexts();
          safeConfig = sheet.getRange("B28:B30").getTexts();
          safeOptional = sheet.getRange("B33:B35").getTexts();
        } else {

          safeFilters = sheet.getRange("B10:B23").getTexts();
          safeLegacy = sheet.getRange("B25:B29").getTexts();
          safeConfig = sheet.getRange("B32:B34").getTexts();
          safeOptional = sheet.getRange("B37:B44").getTexts();
        }
      }

    } catch (e) { console.log("Reset layout."); }
  }


  sheet.getRange("A:Z").clear(ExcelScript.ClearApplyTo.all);


  sheet.getRange("A1:Z75").getFormat().setRowHeight(15);


  sheet.getRange("A7:Z7").getFormat().setRowHeight(15);



  sheet.setShowGridlines(false);
  sheet.setTabColor(CONSTANTS.COLORS.primary);

  const sidebar = sheet.getRange("A1:B75");
  sidebar.getFormat().getFill().setColor(CONSTANTS.COLORS.cardBg);

  const title = sheet.getRange("A2");
  title.setValue("PAINEL DE CONTROLE");
  title.getFormat().getFont().setBold(true);
  title.getFormat().getFont().setSize(14);
  title.getFormat().getFont().setColor(CONSTANTS.COLORS.primary);

  const statusBar = sheet.getRange("A3:B3");
  statusBar.merge(true);
  statusBar.setValue("AGUARDANDO DADOS...");
  statusBar.getFormat().getFill().setColor("#FFFFFF");
  statusBar.getFormat().getFont().setColor(CONSTANTS.COLORS.textMuted);
  statusBar.getFormat().getFont().setSize(8);
  statusBar.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);


  statusBar.getFormat().setRowHeight(28);


  const sHead = sheet.getRange("A4:B4");
  sHead.merge(true);
  sHead.setValue("🔎 PESQUISA & CONTROLE");
  sHead.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
  sHead.getFormat().getFont().setColor("#FFFFFF");
  sHead.getFormat().getFont().setBold(true);
  sHead.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  sheet.getRange("A5").setValue("Host:");
  sheet.getRange("A5").getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.right);
  const searchInput = sheet.getRange("B5");
  searchInput.setValue(savedSearch);
  searchInput.getFormat().getFill().setColor(CONSTANTS.COLORS.inputBg);
  searchInput.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeBottom).setStyle(ExcelScript.BorderLineStyle.continuous);

  sheet.getRange("A6").setValue("Filtrar PA:");
  sheet.getRange("A6").getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.right);
  const paInput = sheet.getRange("B6");
  paInput.setValue(savedPaFilter);
  paInput.getFormat().getFill().setColor(CONSTANTS.COLORS.inputBg);
  paInput.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeBottom).setStyle(ExcelScript.BorderLineStyle.continuous);

  const resetLabel = sheet.getRange("A7");
  resetLabel.setValue("♻️ REDEFINIR (X):");
  resetLabel.getFormat().getFont().setColor(CONSTANTS.COLORS.risk);
  resetLabel.getFormat().getFont().setBold(true);
  resetLabel.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.right);

  const resetInput = sheet.getRange("B7");
  resetInput.setValue("");
  resetInput.getFormat().getFill().setColor("#FFFFFF");
  resetInput.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeBottom).setStyle(ExcelScript.BorderLineStyle.continuous);
  resetInput.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  sheet.getRange("A5:A7").getFormat().getFont().setSize(9);
  sheet.getRange("A5:A7").getFormat().getFont().setBold(true);
  const fHead = sheet.getRange("A9:B9");
  fHead.merge(true);
  fHead.setValue("FILTROS GERAIS");
  fHead.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
  fHead.getFormat().getFont().setColor("#FFFFFF");
  fHead.getFormat().getFont().setBold(true);
  fHead.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  const filterLabels = [
    ["SEGURO (OK)"],
    ["FALTA UEM"],
    ["FALTA EDR"],
    ["FALTA ASSET"],
    ["FANTASMA (AD)"],
    ["PERIGO (SEM AGENTE)"],
    ["TROCA SERIAL"],
    ["DUPLICADO"],
    ["SISTEMA LEGADO"],
    ["OFFLINE"],
    ["GAP DE NOMES"],
    ["DISPONÍVEL"],
    ["INCONSISTÊNCIA DE BASE"],
    ["DIVERGÊNCIA PA x USUÁRIO"]
  ];
  sheet.getRange("A10:A23").setValues(filterLabels);



  if (!isResetting && safeFilters.length >= 14) {
    sheet.getRange("B10:B23").setValues(safeFilters.slice(0, 14));
  } else {
    const d = [
      ["X"],
      ["X"],
      ["X"],
      ["X"],
      ["X"],
      ["X"],
      ["X"],
      ["X"],
      ["X"],
      ["X"],
      [""],
      [""],
      [""],
      [""]
    ];
    sheet.getRange("B10:B23").setValues(d);
  }


  const fBorderGeneral = sheet.getRange("A10:B23");
  fBorderGeneral.getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setStyle(ExcelScript.BorderLineStyle.continuous);
  fBorderGeneral.getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setColor("#CBD5E1");

  const lHead = sheet.getRange("A24:B24");
  lHead.merge(true);
  lHead.setValue("DEFINIÇÃO DE LEGADO");
  lHead.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
  lHead.getFormat().getFont().setColor("#FFFFFF");
  lHead.getFormat().getFont().setBold(true);
  lHead.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  const legacyLabels = [
    ["Windows 11"], ["Windows 10"], ["Windows 8.1"], ["Windows 7"], ["Windows XP"]
  ];
  sheet.getRange("A25:A29").setValues(legacyLabels);

  if (!isResetting && safeLegacy.length === 5) {
    sheet.getRange("B25:B29").setValues(safeLegacy);
  } else {
    const lDef = [[""], [""], ["X"], ["X"], ["X"]];
    sheet.getRange("B25:B29").setValues(lDef);
  }
  const lBorder = sheet.getRange("A25:B29");
  lBorder.getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setStyle(ExcelScript.BorderLineStyle.continuous);
  lBorder.getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setColor("#CBD5E1");


  const cHead = sheet.getRange("A31:B31");
  cHead.merge(true);
  cHead.setValue("PARÂMETROS");
  cHead.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
  cHead.getFormat().getFont().setColor("#FFFFFF");
  cHead.getFormat().getFont().setBold(true);
  cHead.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  const configLabels = [["Gap Limite"], ["Checar Clones"], ["Dias Inativo"]];
  sheet.getRange("A32:A34").setValues(configLabels);

  if (!isResetting && safeConfig.length === 3) {
    sheet.getRange("B32:B34").setValues(safeConfig);
  } else {
    const cD = [["500"], ["SIM"], ["30"]];
    sheet.getRange("B32:B34").setValues(cD);
  }
  sheet.getRange("B32:B34").getFormat().getFill().setColor("#F1F5F9");


  const oHead = sheet.getRange("A36:B36");
  oHead.merge(true);
  oHead.setValue("COLUNAS OPCIONAIS");
  oHead.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
  oHead.getFormat().getFont().setColor("#FFFFFF");
  oHead.getFormat().getFont().setBold(true);
  oHead.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  const optLabels = [
    ["Modelo HW"],
    ["IP Local"],
    ["Tags / Sensor"],
    ["Fontes (AD / UEM / EDR / ASSET)"],
    ["Chassis (EDR)"],
    ["status_usuariologado (UEM_EXTRA)"],
    ["status_check_win11 (UEM_EXTRA)"],
    ["Sinal Individual (U.S AD/UEM/EDR)"],
    ["Último Sinal (Fallback)"]
  ];


  sheet.getRange("A37:A45").setValues(optLabels);


  if (!isResetting && safeOptional.length === 9) {
    sheet.getRange("B37:B45").setValues(safeOptional);
  } else {
    sheet.getRange("B37:B45").setValues([[""], [""], [""], [""], [""], [""], [""], [""], [""]]);
  }

  sheet.getRange("A10:A45").getFormat().getFont().setSize(9);
  sheet.getRange("B10:B45").getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  sheet.getRange("A37:B45").getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setStyle(ExcelScript.BorderLineStyle.continuous);
  sheet.getRange("A37:B45").getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setColor("#CBD5E1");




  {
    const otherHead = sheet.getRange("A47:B47");
    otherHead.merge(true);
    otherHead.setValue("OUTRAS PLANILHAS");
    otherHead.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
    otherHead.getFormat().getFont().setColor("#FFFFFF");
    otherHead.getFormat().getFont().setBold(true);
    otherHead.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

    sheet.getRange("A48").setValue("Impressoras:");
    sheet.getRange("A48").getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.right);
    sheet.getRange("A48").getFormat().getFont().setBold(true);

    const printersToggle = sheet.getRange("B48");
    printersToggle.getFormat().getFill().setColor(CONSTANTS.COLORS.inputBg);
    printersToggle.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);
    printersToggle.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeBottom).setStyle(ExcelScript.BorderLineStyle.continuous);

    sheet.getRange("A49").setValue("Telefonia:");
    sheet.getRange("A49").getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.right);
    sheet.getRange("A49").getFormat().getFont().setBold(true);

    const telephonyToggle = sheet.getRange("B49");
    telephonyToggle.getFormat().getFill().setColor(CONSTANTS.COLORS.inputBg);
    telephonyToggle.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);
    telephonyToggle.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeBottom).setStyle(ExcelScript.BorderLineStyle.continuous);


    if (!isResetting) {
      printersToggle.setValue(savedPrintersToggle);
      telephonyToggle.setValue(savedTelephonyToggle);
    } else {
      printersToggle.setValue("");
      telephonyToggle.setValue("");
    }

    sheet.getRange("A47:B49").getFormat().getFont().setSize(9);
    sheet.getRange("A48:B49").getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setStyle(ExcelScript.BorderLineStyle.continuous);
    sheet.getRange("A48:B49").getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setColor("#CBD5E1");
  }



  const drawKpiHeader = (col: string, title: string, color: string) => {
    const rTitle = sheet.getRange(`${col}2`);
    rTitle.setValue(title);
    rTitle.getFormat().getFont().setColor(color);
    rTitle.getFormat().getFont().setBold(true);
    rTitle.getFormat().getFont().setSize(10);
    rTitle.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.left);
    rTitle.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeTop).setStyle(ExcelScript.BorderLineStyle.continuous);
    rTitle.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeTop).setColor(color);
    rTitle.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeTop).setWeight(ExcelScript.BorderWeight.thick);
  };

  drawKpiHeader("C", "TOTAL ATIVO", CONSTANTS.COLORS.primary);
  drawKpiHeader("E", "CRÍTICOS", CONSTANTS.COLORS.risk);
  drawKpiHeader("G", "ANOMALIAS", CONSTANTS.COLORS.warning);

  sheet.getRange("A:A").getFormat().setColumnWidth(160);
  sheet.getRange("B:B").getFormat().setColumnWidth(30);
  sheet.getRange("C:M").getFormat().setColumnWidth(100);

  const getCheck = (row: number) => {
    const val = sheet.getRange(`B${row}`).getText().trim().toUpperCase();
    return val === "X" || val === "S" || val === "SIM" || val === "YES";
  };

  let legacyList: string[] = [];
  if (getCheck(25)) legacyList.push("Windows 11");
  if (getCheck(26)) legacyList.push("Windows 10");
  if (getCheck(27)) legacyList.push("Windows 8");
  if (getCheck(28)) legacyList.push("Windows 7");
  if (getCheck(29)) legacyList.push("Windows XP");

  let rawPaFilter = sheet.getRange("B6").getText().trim();
  if (rawPaFilter.length === 1 && !isNaN(parseInt(rawPaFilter))) {
    rawPaFilter = "0" + rawPaFilter;
  }

  let config: AppConfig = {
    filters: {
      compliant: getCheck(10),
      missingUem: getCheck(11),
      missingEdr: getCheck(12),
      missingAsset: getCheck(13),
      phantom: getCheck(14),
      rogue: getCheck(15),
      swap: getCheck(16),
      clone: getCheck(17),
      legacy: getCheck(18),
      offline: getCheck(19),
      gap: getCheck(20),
      available: getCheck(21),
      inconsistency: getCheck(22),
      paMismatch: getCheck(23)
    },


    searchTerm: sheet.getRange("B5").getText().toUpperCase().trim(),
    paFilter: rawPaFilter.toUpperCase(),
    optionalCols: {
      model: getCheck(37),
      ip: getCheck(38),
      tags: getCheck(39),
      sources: getCheck(40),
      chassis: getCheck(41),
      status_usuariologado: getCheck(42),
      status_check_win11: getCheck(43),
      sinal_individual: getCheck(44),
      ultimo_sinal: getCheck(45)
    },



    legacyDefinitions: legacyList,
    modules: {
      gapLimit: 500, cloneEnabled: true, staleDays: 30,
      printersEnabled: getCheck(48)
    }



  };

  const gap = sheet.getRange("B32").getText();
  if (parseInt(gap)) config.modules.gapLimit = parseInt(gap, 10);
  const cl = sheet.getRange("B33").getText().toUpperCase();
  config.modules.cloneEnabled = (cl === "SIM" || cl === "YES" || cl === "ATIVADO");
  const stale = sheet.getRange("B34").getText();
  if (parseInt(stale)) config.modules.staleDays = parseInt(stale);


  paintToggleRange(sheet, "B10:B23");
  paintToggleRange(sheet, "B25:B29");
  paintToggleRange(sheet, "B37:B45");
  paintToggleRange(sheet, "B48:B49");

  return config;

}

function loadPrinters(workbook: ExcelScript.Workbook): ProcessedRow[] {
  const rows: ProcessedRow[] = [];
  const sheet = workbook.getWorksheet(CONSTANTS.SHEET_PRINTERS);
  if (!sheet) return rows;

  const used = sheet.getUsedRange();
  if (!used) return rows;

  const data = used.getTexts();
  if (data.length < 2) return rows;

  const h = data[0];

  const idxPa = getIdx(h, "PA");
  const idxIp = getIdx(h, "IP IMPRESSORA");
  const idxModelo = getIdx(h, "MODELO");
  const idxHost = getIdx(h, "HOSTNAME");
  const idxDisplay = getIdx(h, "NOME DE EXIBIÇÃO");
  const idxUser = getIdx(h, "USUARIO");
  const idxSenha = getIdx(h, "SENHA");
  const idxTipo = getIdx(h, "TIPO");
  const idxMac = getIdx(h, "MAC");
  const idxPat = getIdx(h, "Patrimonio Lig Print");
  const idxLocal = getIdx(h, "Local da imp");

  const val = (r: string[], idx: number) => (idx !== -1 && r[idx]) ? r[idx] : "-";

  for (let i = 1; i < data.length; i++) {
    const r = data[i];

    if (!r || r.length === 0) continue;

    const pa = val(r, idxPa);
    const host = val(r, idxHost);


    const arr = [
      host,
      "🖨️ IMPRESSORA",
      pa,
      val(r, idxIp),
      val(r, idxModelo),
      val(r, idxDisplay),
      val(r, idxTipo),
      val(r, idxMac),
      val(r, idxPat),
      val(r, idxLocal),
      val(r, idxUser),
      val(r, idxSenha),
    ];

    rows.push({
      arr,
      colorCategory: "PRINTER",
      priority: 50,
      paColor: "#FFFFFF"
    });
  }

  return rows;
}






type DashboardMode = "MACHINES" | "TELEFONIA";

interface TelephonyConfig {
  modules: {
    expiringDays: number;
  };
  filters: {
    activeLine: boolean;
    inactiveLine: boolean;
    inactiveAccount: boolean;
    conflictActiveAccountInactive: boolean;
    contractExpired: boolean;
    contractExpiringSoon: boolean;
    missingUser: boolean;
    missingChip: boolean;
    missingUrmobo: boolean;
    dupNumber: boolean;
    dupChip: boolean;
  };
  optionalCols: {
    modelo: boolean;
    fabricante: boolean;
    tempoFidelizacao: boolean;
    chip: boolean;
    conta: boolean;
    cnpj: boolean;
    servicos: boolean;
    origem: boolean;
    waMe: boolean;
  };
  searchTerm: string;
  filterTerm: string;
}

interface TelephonyStats {
  total: number;
  active: number;
  inactive: number;
  inactiveAccount: number;
  conflict: number;
  contractExpired: number;
  contractExpiringSoon: number;
  missingUser: number;
  missingChip: number;
  missingUrmobo: number;
  dupNumber: number;
  dupChip: number;
  details: { [key: string]: number };
}

interface TelephonyProcessedRow {
  arr: (string | number | boolean)[];
  category: string;
  priority: number;
  numberKey?: string;
  flags: {
    activeLine: boolean;
    activeAccount: boolean;
    conflict: boolean;
    contractExpired: boolean;
    contractExpiringSoon: boolean;
    missingUser: boolean;
    missingChip: boolean;
    missingUrmobo: boolean;
    dupNumber: boolean;
    dupChip: boolean;
  };
}



function isToggleOn(raw: string): boolean {
  const v = (raw || "").toString().trim().toUpperCase();
  return v === "X" || v === "S" || v === "SIM" || v === "YES" || v === "TRUE" || v === "1";
}

function isResetRequested(sheet: ExcelScript.Worksheet): boolean {
  try {
    const vNew = (sheet.getRange("B7").getText() || "").toUpperCase().trim();
    const vOld = (sheet.getRange("B3").getText() || "").toUpperCase().trim();
    return (vNew === "X" || vNew === "S" || vOld === "X");
  } catch (e) {
    return false;
  }
}

function getDashboardMode(sheet: ExcelScript.Worksheet): DashboardMode {

  if (isResetRequested(sheet)) return "MACHINES";

  try {
    const tel = sheet.getRange("B49").getText();
    if (isToggleOn(tel)) return "TELEFONIA";
  } catch (e) { }

  return "MACHINES";
}

function normalizeTelephonyHeader(s: string): string {
  return (s || "")
    .toString()
    .trim()
    .replace(/^\uFEFF/, "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toUpperCase();
}

function digitsOnly(s: string): string {
  return (s || "").toString().replace(/\D+/g, "");
}

function formatPhone(ddd: string, numero: string): string {
  const d = digitsOnly(ddd);
  const n = digitsOnly(numero);
  if (!d && !n) return "";
  if (d && n) return `(${d}) ${n}`;
  return n || d;
}








type UrmoboPhoneInfo = {
  name: string;
  tel1: boolean;
  tel2: boolean;
};

type UrmoboPhoneIndex = {
  ok: boolean;

  phoneToInfo: Map<string, UrmoboPhoneInfo>;
};


function canonicalPhoneKeyFromDigits(raw: string): string {
  let d = digitsOnly(raw);
  if (!d) return "";
  if (d.startsWith("55") && (d.length === 12 || d.length === 13)) d = d.substring(2);
  if (d.length !== 10 && d.length !== 11) return "";
  return d;
}

function scientificToIntString(raw: string): string {
  const s = (raw || "").toString().trim().replace(/"/g, "").replace(/\s+/g, "").toUpperCase();
  const m = s.match(/^([0-9]+(?:[.,][0-9]+)?)E([+-]?[0-9]+)$/);
  if (!m) return digitsOnly(raw);

  const mantissa = m[1].replace(",", ".");
  const exp = parseInt(m[2], 10);

  const parts = mantissa.split(".");
  const intPart = parts[0] || "0";
  const fracPart = parts[1] || "";

  const digits = (intPart + fracPart).replace(/^0+/, "") || "0";
  const decPlaces = fracPart.length;

  const shift = exp - decPlaces;
  if (shift >= 0) return digits + "0".repeat(shift);

  const cut = digits.length + shift;
  if (cut <= 0) return "0";
  return digits.substring(0, cut);
}

function normalizeUrmoboPhoneCell(textValue: string): string {
  const s = (textValue || "").toString().trim().replace(/"/g, "");
  if (!s) return "";

  const upper = s.toUpperCase();

  if (upper.includes("E+") || upper.includes("E-") || /E[+-]?\d+/.test(upper)) {
    return canonicalPhoneKeyFromDigits(scientificToIntString(s));
  }

  const m = s.match(/^\s*(\d+)\s*([.,])\s*(\d+)\s*$/);
  if (m) {
    const intPart = m[1];
    const fracPart = m[3] || "";
    if (/^0+$/.test(fracPart)) return canonicalPhoneKeyFromDigits(intPart);
    return canonicalPhoneKeyFromDigits(intPart + fracPart);
  }

  return canonicalPhoneKeyFromDigits(s);
}

function normalizeUrmoboPhoneAny(
  value: string | number | boolean | null | undefined,
  textValue: string
): string {

  if (typeof value === "number") {
    if (!isFinite(value) || value === 0) return "";
    return canonicalPhoneKeyFromDigits(Math.round(value).toString());
  }
  return normalizeUrmoboPhoneCell(textValue);
}

function loadUrmoboPhoneIndex(workbook: ExcelScript.Workbook): UrmoboPhoneIndex {
  const empty: UrmoboPhoneIndex = { ok: false, phoneToInfo: new Map<string, UrmoboPhoneInfo>() };

  const sheet = workbook.getWorksheet("URMOBO");
  if (!sheet) return empty;

  const used = sheet.getUsedRange();
  if (!used) return empty;

  const texts = used.getTexts();
  const values = used.getValues() as (string | number | boolean)[][];
  if (!texts || texts.length === 0) return empty;


  const maxScan = Math.min(25, texts.length);
  let headerRow = -1;
  let idxNome = -1;
  let idxTel1 = -1;
  let idxTel2 = -1;

  const norm = (v: string | number | boolean | null | undefined): string =>
    (v ?? "")
      .toString()
      .trim()
      .toUpperCase()
      .replace(/^\uFEFF/, "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/\s+/g, " ");

  const pickIdxByCandidates = (map: { [k: string]: number }, candidates: string[]): number => {
    for (const c of candidates) {
      const key = norm(c);
      if (map[key] !== undefined) return map[key];
    }

    for (const k of Object.keys(map)) {
      if (k === "NOME" || k.startsWith("NOME ")) return map[k];
    }
    return -1;
  };

  for (let r = 0; r < maxScan; r++) {
    const row = texts[r] || [];
    const map: { [k: string]: number } = {};
    for (let c = 0; c < row.length; c++) map[norm(row[c])] = c;

    if (map["TELEFONE 1"] !== undefined && map["TELEFONE 2"] !== undefined) {
      headerRow = r;
      idxTel1 = map["TELEFONE 1"];
      idxTel2 = map["TELEFONE 2"];
      idxNome = pickIdxByCandidates(map, [
        "NOME",
        "NOME COMPLETO",
        "USUARIO",
        "USUÁRIO",
        "COLABORADOR",
        "FUNCIONARIO",
        "FUNCIONÁRIO"
      ]);
      break;
    }
  }

  if (headerRow === -1 || idxTel1 < 0 || idxTel2 < 0) return empty;

  const phoneToInfo = new Map<string, UrmoboPhoneInfo>();

  for (let r = headerRow + 1; r < texts.length; r++) {
    const tRow = texts[r] || [];
    const vRow: (string | number | boolean)[] = (values && values[r]) ? (values[r] as (string | number | boolean)[]) : [];


    const nome = (idxNome >= 0 && tRow[idxNome] != null) ? tRow[idxNome].toString().trim() : "";

    const p1 = normalizeUrmoboPhoneAny(vRow[idxTel1], tRow[idxTel1]);
    const p2 = normalizeUrmoboPhoneAny(vRow[idxTel2], tRow[idxTel2]);

    const upsert = (k: string, fromTel1: boolean, fromTel2: boolean) => {
      if (!k) return;

      if (!phoneToInfo.has(k)) {
        phoneToInfo.set(k, { name: nome, tel1: fromTel1, tel2: fromTel2 });
        return;
      }

      const old = phoneToInfo.get(k);
      if (!old) {
        phoneToInfo.set(k, { name: nome, tel1: fromTel1, tel2: fromTel2 });
        return;
      }

      const oldName = old.name || "";
      let mergedName = oldName;

      if (nome && oldName && oldName !== nome && !oldName.includes(nome)) mergedName = `${oldName} | ${nome}`;
      else if (!oldName && nome) mergedName = nome;

      phoneToInfo.set(k, {
        name: mergedName,
        tel1: old.tel1 || fromTel1,
        tel2: old.tel2 || fromTel2
      });
    };

    upsert(p1, true, false);
    upsert(p2, false, true);
  }

  return { ok: true, phoneToInfo };
}











const DAY_MS = 86400000;

function excelSerialToMs(serial: number): number {


  if (!isFinite(serial)) return 0;

  const base = new Date(1899, 11, 30).getTime();
  return base + Math.round(serial * DAY_MS);
}

function clampToEndOfDay(ms: number): number {
  if (!ms || ms <= 0) return 0;
  const d = new Date(ms);
  return new Date(d.getFullYear(), d.getMonth(), d.getDate(), 23, 59, 59).getTime();
}

function isValidYMD(y: number, m: number, d: number): boolean {
  if (y < 1900 || y > 2100) return false;
  if (m < 1 || m > 12) return false;
  if (d < 1 || d > 31) return false;
  const dt = new Date(y, m - 1, d);
  return dt.getFullYear() === y && (dt.getMonth() + 1) === m && dt.getDate() === d;
}

function parseDateStringFlexible(sRaw: string): number {
  if (!sRaw) return 0;
  let s = sRaw.toString().trim();


  s = s.replace(/^\'+/, "").trim();
  if (!s) return 0;

  const up = normNoDiaUpper(s);
  if (up === "N/A" || up.includes("NAO POSSUI")) return 0;


  if (/^\d{8}$/.test(s)) {
    const dd = parseInt(s.slice(0, 2), 10);
    const mm = parseInt(s.slice(2, 4), 10);
    const yyyy = parseInt(s.slice(4, 8), 10);
    if (!isValidYMD(yyyy, mm, dd)) return 0;
    return new Date(yyyy, mm - 1, dd, 0, 0, 0).getTime();
  }


  if (/^\d+(\.\d+)?$/.test(s)) {
    const n = Number(s);

    if (isFinite(n) && n > 20000 && n < 60000) return excelSerialToMs(n);
  }


  let m = s.match(/^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m) {
    const yyyy = parseInt(m[1], 10);
    const mm = parseInt(m[2], 10);
    const dd = parseInt(m[3], 10);
    if (!isValidYMD(yyyy, mm, dd)) return 0;

    const hasTime = !!m[4];
    const hh = hasTime ? parseInt(m[4], 10) : 0;
    const mi = hasTime ? parseInt(m[5], 10) : 0;
    const ss = hasTime ? (m[6] ? parseInt(m[6], 10) : 0) : 0;

    return new Date(yyyy, mm - 1, dd, hh, mi, ss).getTime();
  }


  m = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m) {
    const a = parseInt(m[1], 10);
    const b = parseInt(m[2], 10);
    const yyyy = parseInt(m[3], 10);

    const hasTime = !!m[4];
    const hh = hasTime ? parseInt(m[4], 10) : 0;
    const mi = hasTime ? parseInt(m[5], 10) : 0;
    const ss = hasTime ? (m[6] ? parseInt(m[6], 10) : 0) : 0;





    let dd = a;
    let mm = b;
    if (b > 12 && a <= 12) { mm = a; dd = b; }

    if (!isValidYMD(yyyy, mm, dd)) return 0;
    return new Date(yyyy, mm - 1, dd, hh, mi, ss).getTime();
  }


  m = s.match(/^(\d{1,2})[-.](\d{1,2})[-.](\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/);
  if (m) {
    const dd = parseInt(m[1], 10);
    const mm = parseInt(m[2], 10);
    const yyyy = parseInt(m[3], 10);
    if (!isValidYMD(yyyy, mm, dd)) return 0;

    const hasTime = !!m[4];
    const hh = hasTime ? parseInt(m[4], 10) : 0;
    const mi = hasTime ? parseInt(m[5], 10) : 0;
    const ss = hasTime ? (m[6] ? parseInt(m[6], 10) : 0) : 0;

    return new Date(yyyy, mm - 1, dd, hh, mi, ss).getTime();
  }

  return 0;
}


type CellScalar = string | number | boolean;

function parseTelephonyCellDateScore(text: string, value: CellScalar | null, forceEndOfDay: boolean): number {



  if (typeof value === "number") {
    const ms = (value > 20000 && value < 60000) ? excelSerialToMs(value) : 0;
    return forceEndOfDay ? clampToEndOfDay(ms) : ms;
  }


  const msS = parseDateStringFlexible(text || "");
  return forceEndOfDay ? clampToEndOfDay(msS) : msS;
}



function formatDateBR(ms: number): string {
  if (!ms || ms <= 0) return "-";
  const d = new Date(ms);
  const dd = String(d.getDate()).padStart(2, "0");
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const yy = d.getFullYear();
  return `${dd}/${mm}/${yy}`;
}

function normNoDiaUpper(v: string): string {

  return stripDiacritics((v || "").toString().trim()).toUpperCase();
}

function isInactiveStatus(raw: string): boolean {
  const s = normNoDiaUpper(raw);
  if (!s) return false;

  return (
    s.includes("INATIV") ||
    s.includes("CANCEL") ||
    s.includes("SUSPEN") ||
    s.includes("DESATIV") ||
    s.includes("BLOQUE")
  );
}

function isActiveStatus(raw: string): boolean {
  const s = normNoDiaUpper(raw);
  if (!s) return false;


  if (isInactiveStatus(s)) return false;



  return s.includes("ATIV");
}


function resetAndDrawTelephonyDashboard(sheet: ExcelScript.Worksheet): TelephonyConfig {
  let safeFilters: string[][] = [];
  let safeConfig: string[][] = [];
  let safeOptional: string[][] = [];

  let savedSearch = "";
  let savedFilterTerm = "";
  let savedPrintersToggle = "";
  let savedTelephonyToggle = "";

  const isResetting = isResetRequested(sheet);

  if (!isResetting) {
    try {
      savedSearch = sheet.getRange("B5").getText();
      savedFilterTerm = sheet.getRange("B6").getText();
      savedPrintersToggle = sheet.getRange("B48").getText();
      savedTelephonyToggle = sheet.getRange("B49").getText();
    } catch (e) { }

    try { safeFilters = sheet.getRange("B10:B20").getTexts(); } catch (e) { }
    try { safeConfig = sheet.getRange("B32:B32").getTexts(); } catch (e) { }
    try { safeOptional = sheet.getRange("B37:B45").getTexts(); } catch (e) { }
  }

  sheet.getRange("A:Z").clear(ExcelScript.ClearApplyTo.all);
  sheet.getRange("A1:Z75").getFormat().setRowHeight(15);
  sheet.getRange("A7:Z7").getFormat().setRowHeight(15);

  sheet.setShowGridlines(false);
  sheet.setTabColor(CONSTANTS.COLORS.primary);

  const sidebar = sheet.getRange("A1:B75");
  sidebar.getFormat().getFill().setColor(CONSTANTS.COLORS.cardBg);

  const title = sheet.getRange("A2");
  title.setValue("PAINEL TELEFONIA");
  title.getFormat().getFont().setBold(true);
  title.getFormat().getFont().setSize(14);
  title.getFormat().getFont().setColor(CONSTANTS.COLORS.primary);

  const statusBar = sheet.getRange("A3:B3");
  statusBar.merge(true);
  statusBar.setValue("📞 MODO TELEFONIA");
  statusBar.getFormat().getFill().setColor("#FFFFFF");
  statusBar.getFormat().getFont().setColor(CONSTANTS.COLORS.textMuted);
  statusBar.getFormat().getFont().setSize(8);
  statusBar.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);
  statusBar.getFormat().setRowHeight(28);

  const sHead = sheet.getRange("A4:B4");
  sHead.merge(true);
  sHead.setValue("🔎 PESQUISA & CONTROLE");
  sHead.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
  sHead.getFormat().getFont().setColor("#FFFFFF");
  sHead.getFormat().getFont().setBold(true);
  sHead.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  sheet.getRange("A5").setValue("Buscar:");
  sheet.getRange("A5").getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.right);
  const searchInput = sheet.getRange("B5");
  searchInput.setValue(savedSearch);
  searchInput.getFormat().getFill().setColor(CONSTANTS.COLORS.inputBg);
  searchInput.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeBottom).setStyle(ExcelScript.BorderLineStyle.continuous);

  sheet.getRange("A6").setValue("Filtro:");
  sheet.getRange("A6").getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.right);
  const filterInput = sheet.getRange("B6");
  filterInput.setValue(savedFilterTerm);
  filterInput.getFormat().getFill().setColor(CONSTANTS.COLORS.inputBg);
  filterInput.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeBottom).setStyle(ExcelScript.BorderLineStyle.continuous);

  const resetLabel = sheet.getRange("A7");
  resetLabel.setValue("♻️ REDEFINIR (X):");
  resetLabel.getFormat().getFont().setColor(CONSTANTS.COLORS.risk);
  resetLabel.getFormat().getFont().setBold(true);
  resetLabel.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.right);

  const resetInput = sheet.getRange("B7");
  resetInput.setValue("");
  resetInput.getFormat().getFill().setColor("#FFFFFF");
  resetInput.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeBottom).setStyle(ExcelScript.BorderLineStyle.continuous);
  resetInput.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  sheet.getRange("A5:A7").getFormat().getFont().setSize(9);
  sheet.getRange("A5:A7").getFormat().getFont().setBold(true);

  const fHead = sheet.getRange("A9:B9");
  fHead.merge(true);
  fHead.setValue("FILTROS TELEFONIA");
  fHead.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
  fHead.getFormat().getFont().setColor("#FFFFFF");
  fHead.getFormat().getFont().setBold(true);
  fHead.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  const filterLabels = [
    ["LINHA ATIVA"],
    ["LINHA INATIVA"],
    ["CONTA INATIVA"],
    ["CONFLITO (ATIVA + CONTA INATIVA)"],
    ["CONTRATO VENCIDO"],
    ["CONTRATO (VENCE EM BREVE)"],
    ["SEM USUÁRIO CADASTRADO"],
    ["SEM CHIP"],
    ["SEM URMOBO"],
    ["DUPLICIDADE DE NÚMERO"],
    ["DUPLICIDADE DE CHIP"]
  ];
  sheet.getRange("A10:A20").setValues(filterLabels);

  if (!isResetting && safeFilters.length === 11) {
    sheet.getRange("B10:B20").setValues(safeFilters);
  } else {
    sheet.getRange("B10:B20").setValues([["X"], ["X"], ["X"], ["X"], ["X"], ["X"], ["X"], ["X"], ["X"], ["X"], ["X"]]);
  }

  const fBorderTel = sheet.getRange("A10:B20");
  fBorderTel.getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setStyle(ExcelScript.BorderLineStyle.continuous);
  fBorderTel.getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setColor("#CBD5E1");

  const cHead = sheet.getRange("A31:B31");
  cHead.merge(true);
  cHead.setValue("PARÂMETROS");
  cHead.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
  cHead.getFormat().getFont().setColor("#FFFFFF");
  cHead.getFormat().getFont().setBold(true);
  cHead.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  sheet.getRange("A32").setValue("Dias p/ vencer:");
  sheet.getRange("A32").getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.right);
  const expDays = sheet.getRange("B32");
  expDays.getFormat().getFill().setColor("#F1F5F9");
  if (!isResetting && safeConfig.length === 1 && safeConfig[0] && safeConfig[0][0]) expDays.setValue(safeConfig[0][0]);
  else expDays.setValue("30");

  const oHead = sheet.getRange("A36:B36");
  oHead.merge(true);
  oHead.setValue("COLUNAS OPCIONAIS");
  oHead.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
  oHead.getFormat().getFont().setColor("#FFFFFF");
  oHead.getFormat().getFont().setBold(true);
  oHead.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  const optLabels = [
    ["Modelo"],
    ["Fabricante"],
    ["Tempo Fidelização"],
    ["Chip"],
    ["Conta"],
    ["CNPJ"],
    ["Serviços Ativos"],
    ["Origem (OUT1/OUT2/OUT3)"],
    ["WA.ME (copiar/colar)"]
  ];
  sheet.getRange("A37:A45").setValues(optLabels);


  if (!isResetting && safeOptional.length === 9) {
    sheet.getRange("B37:B45").setValues(safeOptional);
  } else if (!isResetting && safeOptional.length === 8) {
    sheet.getRange("B37:B45").setValues([...safeOptional, [""]]);
  } else {
    sheet.getRange("B37:B45").setValues([["X"], ["X"], [""], ["X"], ["X"], ["X"], ["X"], ["X"], [""]]);
  }

  sheet.getRange("A10:A45").getFormat().getFont().setSize(9);
  sheet.getRange("B10:B45").getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  sheet.getRange("A37:B45").getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setStyle(ExcelScript.BorderLineStyle.continuous);
  sheet.getRange("A37:B45").getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setColor("#CBD5E1");

  const otherHead = sheet.getRange("A47:B47");
  otherHead.merge(true);
  otherHead.setValue("OUTRAS PLANILHAS");
  otherHead.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
  otherHead.getFormat().getFont().setColor("#FFFFFF");
  otherHead.getFormat().getFont().setBold(true);
  otherHead.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);

  sheet.getRange("A48").setValue("Impressoras:");
  sheet.getRange("A48").getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.right);
  sheet.getRange("A48").getFormat().getFont().setBold(true);

  const printersToggle = sheet.getRange("B48");
  printersToggle.getFormat().getFill().setColor(CONSTANTS.COLORS.inputBg);
  printersToggle.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);
  printersToggle.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeBottom).setStyle(ExcelScript.BorderLineStyle.continuous);

  sheet.getRange("A49").setValue("Telefonia:");
  sheet.getRange("A49").getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.right);
  sheet.getRange("A49").getFormat().getFont().setBold(true);

  const telephonyToggle = sheet.getRange("B49");
  telephonyToggle.getFormat().getFill().setColor(CONSTANTS.COLORS.inputBg);
  telephonyToggle.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);
  telephonyToggle.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeBottom).setStyle(ExcelScript.BorderLineStyle.continuous);

  if (!isResetting) {
    printersToggle.setValue(savedPrintersToggle);
    telephonyToggle.setValue("X");
  } else {
    printersToggle.setValue("");
    telephonyToggle.setValue("");
  }

  sheet.getRange("A47:B49").getFormat().getFont().setSize(9);
  sheet.getRange("A48:B49").getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setStyle(ExcelScript.BorderLineStyle.continuous);
  sheet.getRange("A48:B49").getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal).setColor("#CBD5E1");

  const drawKpiHeader = (col: string, titleTxt: string, color: string) => {
    const rTitle = sheet.getRange(`${col}2`);
    rTitle.setValue(titleTxt);
    rTitle.getFormat().getFont().setColor(color);
    rTitle.getFormat().getFont().setBold(true);
    rTitle.getFormat().getFont().setSize(10);
    rTitle.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.left);
    rTitle.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeTop).setStyle(ExcelScript.BorderLineStyle.continuous);
    rTitle.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeTop).setColor(color);
    rTitle.getFormat().getRangeBorder(ExcelScript.BorderIndex.edgeTop).setWeight(ExcelScript.BorderWeight.thick);
  };

  drawKpiHeader("C", "TOTAL LINHAS", CONSTANTS.COLORS.primary);
  drawKpiHeader("E", "CRÍTICOS", CONSTANTS.COLORS.risk);
  drawKpiHeader("G", "ANOMALIAS", CONSTANTS.COLORS.warning);

  sheet.getRange("A:A").getFormat().setColumnWidth(160);
  sheet.getRange("B:B").getFormat().setColumnWidth(30);
  sheet.getRange("C:M").getFormat().setColumnWidth(100);

  const getCheck = (row: number) => {
    const val = sheet.getRange(`B${row}`).getText().trim().toUpperCase();
    return val === "X" || val === "S" || val === "SIM" || val === "YES";
  };

  const cfg: TelephonyConfig = {
    filters: {
      activeLine: getCheck(10),
      inactiveLine: getCheck(11),
      inactiveAccount: getCheck(12),
      conflictActiveAccountInactive: getCheck(13),
      contractExpired: getCheck(14),
      contractExpiringSoon: getCheck(15),
      missingUser: getCheck(16),
      missingChip: getCheck(17),
      missingUrmobo: getCheck(18),
      dupNumber: getCheck(19),
      dupChip: getCheck(20)

    },
    searchTerm: (sheet.getRange("B5").getText() || "").toString().trim(),
    filterTerm: (sheet.getRange("B6").getText() || "").toString().trim(),
    optionalCols: {
      modelo: getCheck(37),
      fabricante: getCheck(38),
      tempoFidelizacao: getCheck(39),
      chip: getCheck(40),
      conta: getCheck(41),
      cnpj: getCheck(42),
      servicos: getCheck(43),
      origem: getCheck(44),
      waMe: getCheck(45)
    },
    modules: {
      expiringDays: 30
    }
  };

  const expRaw = sheet.getRange("B32").getText();
  if (parseInt(expRaw)) cfg.modules.expiringDays = parseInt(expRaw, 10);

  paintToggleRange(sheet, "B10:B20");
  paintToggleRange(sheet, "B32:B32");
  paintToggleRange(sheet, "B37:B45");
  paintToggleRange(sheet, "B48:B49");

  return cfg;

}

function loadTelephony(workbook: ExcelScript.Workbook, expiringDays: number): { rows: TelephonyProcessedRow[]; stats: TelephonyStats } {
  const rawRows: {
    usuario: string;
    ddd: string;
    numero: string;
    dataAtiv: string;
    termino: string;
    plano: string;
    statusLinha: string;
    statusConta: string;
    modelo: string;
    fabricante: string;
    tempoFidel: string;
    chip: string;
    conta: string;
    cnpj: string;
    servicos: string;
    origem: string;
    numberKey: string;
    chipKey: string;
    hasUrmobo: boolean;
    tel1Present: boolean;
    tel2Present: boolean;
    urmoboUser: string;
    contractScore: number;
    activScore: number;
  }[] = [];


  const urmoboIndex = loadUrmoboPhoneIndex(workbook);

  const sheets = workbook.getWorksheets();

  for (const sh of sheets) {
    const name = sh.getName();
    if (!/^OUT\d+$/i.test(name) && !/^OUT$/i.test(name) && !/^OUT_/i.test(name)) continue;

    const used = sh.getUsedRange();
    if (!used) continue;


    const data = used.getTexts();
    if (!data || data.length < 2) continue;


    let headerRow = -1;
    for (let r = 0; r < data.length; r++) {
      const row = data[r];
      if (!row) continue;
      for (let c = 0; c < row.length; c++) {
        if (normalizeTelephonyHeader(row[c]) === "USUARIO_CADASTRADO") { headerRow = r; break; }
      }
      if (headerRow !== -1) break;
    }
    if (headerRow === -1) headerRow = 0;

    const h = (data[headerRow] || []).map(x => x || "");
    const idx = (col: string) => getIdx(h, col);

    const idxUsuario = idx("USUARIO_CADASTRADO");
    const idxDdd = idx("DDD");
    const idxNumero = idx("NUMERO");
    const idxDataAt = idx("DATA_ATIVACAO");
    const idxTerm = idx("TERMINO_CONTRATO");
    const idxPlano = idx("PLANO_LINHA");
    const idxStLinha = idx("STATUS_LINHA");
    const idxStConta = idx("STATUS_CONTA");
    const idxModelo = idx("MODELO_APARELHO");
    const idxFab = idx("FABRICANTE");
    const idxFid = idx("TEMPO_FIDELIZACAO");
    const idxChip = idx("CHIP");
    const idxConta = idx("CONTA");
    const idxCnpj = idx("CNPJ");

    const serviceIdxs: number[] = [];
    for (let c = 0; c < h.length; c++) {
      const hn = normalizeTelephonyHeader(h[c]);
      if (hn.startsWith("SERVICO_ATIVOS")) serviceIdxs.push(c);
    }

    for (let r = headerRow + 1; r < data.length; r++) {
      const row = data[r];
      if (!row || row.length === 0) continue;

      const rowText: string[] = (data[r] || []);


      const valT = (i: number): string =>
        (i !== -1 && rowText && rowText[i] != null) ? rowText[i].toString().trim() : "";


      const valV = (_i: number): (string | number | boolean) | null => null;

      const usuario = valT(idxUsuario);
      const ddd = valT(idxDdd);
      const numero = valT(idxNumero);


      const dataAt = valT(idxDataAt);
      const termino = valT(idxTerm);

      const plano = valT(idxPlano);
      const stLinha = valT(idxStLinha);
      const stConta = valT(idxStConta);
      const modelo = valT(idxModelo);
      const fabricante = valT(idxFab);
      const tempoFid = valT(idxFid);
      const chip: string = valT(idxChip);
      const conta: string = valT(idxConta);
      const cnpj: string = valT(idxCnpj);



      const activScore = parseTelephonyCellDateScore(dataAt, valV(idxDataAt), false);


      const contractScore = parseTelephonyCellDateScore(termino, valV(idxTerm), true);


      const services: string[] = [];
      for (const si of serviceIdxs) {
        const sv = (si !== -1 && row[si]) ? row[si].toString().trim() : "";
        if (sv) services.push(sv);
      }
      const servicos = services.join("\n");

      const dDigits = digitsOnly(ddd);
      const nDigits = digitsOnly(numero);


      const keyFromNumero = canonicalPhoneKeyFromDigits(nDigits);
      const keyFromDddNumero = canonicalPhoneKeyFromDigits(`${dDigits}${nDigits}`);
      const numberKey = keyFromNumero || keyFromDddNumero || "";


      let hasUrmobo = false;
      let tel1Present = false;
      let tel2Present = false;
      let urmoboUser = "";

      if (numberKey && urmoboIndex.ok) {
        const hit = urmoboIndex.phoneToInfo.get(numberKey);
        if (hit) {
          hasUrmobo = true;
          tel1Present = !!hit.tel1;
          tel2Present = !!hit.tel2;
          urmoboUser = hit.name || "";
        }
      }


      const chipKey = digitsOnly(chip);

      const hasAnyField = [usuario, ddd, numero, plano, stLinha, stConta, chip, conta, cnpj].some(x => (x || "").trim() !== "");
      if (!hasAnyField) continue;

      rawRows.push({
        usuario, ddd, numero, dataAtiv: dataAt, termino, plano, statusLinha: stLinha, statusConta: stConta,
        modelo, fabricante, tempoFidel: tempoFid, chip, conta, cnpj, servicos,
        origem: name,
        numberKey,
        chipKey,
        hasUrmobo,
        tel1Present,
        tel2Present,
        urmoboUser,
        contractScore,
        activScore
      });

    }
  }

  const numberCount = new Map<string, number>();
  const chipCount = new Map<string, number>();

  for (const r of rawRows) {
    if (r.numberKey) numberCount.set(r.numberKey, (numberCount.get(r.numberKey) || 0) + 1);
    if (r.chipKey) chipCount.set(r.chipKey, (chipCount.get(r.chipKey) || 0) + 1);
  }

  const now = new Date().getTime();

  const stats: TelephonyStats = {
    total: 0,
    active: 0,
    inactive: 0,
    inactiveAccount: 0,
    conflict: 0,
    contractExpired: 0,
    contractExpiringSoon: 0,
    missingUser: 0,
    missingChip: 0,
    missingUrmobo: 0,
    dupNumber: 0,
    dupChip: 0,
    details: {}
  };

  const processed: TelephonyProcessedRow[] = [];

  for (const r of rawRows) {
    stats.total++;

    const activeLine = isActiveStatus(r.statusLinha);
    const activeAccount = isActiveStatus(r.statusConta);

    const conflict = activeLine && !activeAccount;

    const missingUser = !r.usuario || r.usuario.trim() === "";
    const missingChip = !r.chipKey;
    const missingUrmobo = !r.hasUrmobo;

    const dupNumber = !!r.numberKey && (numberCount.get(r.numberKey) || 0) > 1;
    const dupChip = !!r.chipKey && (chipCount.get(r.chipKey) || 0) > 1;

    const daysCfg = (expiringDays && expiringDays > 0) ? expiringDays : 30;

    const expired = (r.contractScore > 0 && r.contractScore < now);

    const daysLeft =
      (r.contractScore > 0)
        ? Math.ceil((r.contractScore - now) / DAY_MS)
        : 999999;

    const expSoon =
      (r.contractScore > 0) &&
      !expired &&
      (daysLeft <= daysCfg);


    if (activeLine) stats.active++; else stats.inactive++;
    if (!activeAccount) stats.inactiveAccount++;
    if (conflict) stats.conflict++;
    if (expired) stats.contractExpired++;
    if (expSoon) stats.contractExpiringSoon++;
    if (missingUser) stats.missingUser++;
    if (missingChip) stats.missingChip++;
    if (missingUrmobo) stats.missingUrmobo++;
    if (dupNumber) stats.dupNumber++;
    if (dupChip) stats.dupChip++;

    let category = "OK";
    let priority = 20;

    if (conflict) { category = "INCONSISTENCY"; priority = 0; }
    else if (expired) { category = "EXPIRED"; priority = 1; }
    else if (dupNumber || dupChip) { category = "DUPLICATE"; priority = 2; }
    else if (expSoon) { category = "EXPIRING"; priority = 3; }
    else if (missingUser || missingChip) { category = "MISSING"; priority = 4; }
    else if (missingUrmobo) { category = "URMOBO"; priority = 5; }
    else if (!activeLine || isInactiveStatus(r.statusLinha)) { category = "INACTIVE"; priority = 6; }

    stats.details[category] = (stats.details[category] || 0) + 1;

    const ind = (conflict || expired) ? "🔴" : (dupNumber || dupChip || expSoon || missingUser || missingChip || missingUrmobo || !activeLine) ? "🟠" : "";

    const derivedDdd = digitsOnly(r.ddd) || (r.numberKey ? r.numberKey.substring(0, 2) : "");
    const derivedNumeroPart = (r.numberKey && r.numberKey.length > 2) ? r.numberKey.substring(2) : digitsOnly(r.numero);
    const numeroFmt = formatPhone(derivedDdd, derivedNumeroPart);

    const ativFmt = formatDateBR(r.activScore);
    const termFmt = (r.contractScore > 0)
      ? (expired
        ? `${formatDateBR(r.contractScore)} (VENCIDO)`
        : expSoon
          ? `${formatDateBR(r.contractScore)} (VENCE EM BREVE)`
          : formatDateBR(r.contractScore)
      )
      : (r.termino && normNoDiaUpper(r.termino).includes("NAO POSSUI")
        ? "NÃO POSSUI CONTRATO"
        : "-"
      );


    processed.push({
      arr: [
        ind,
        r.usuario || "-",
        sourceIcon(r.hasUrmobo),
        sourceIcon(r.tel1Present),
        sourceIcon(r.tel2Present),
        r.urmoboUser || "-",
        derivedDdd || "-",
        numeroFmt,
        (r.statusLinha || "-").toUpperCase(),
        (r.statusConta || "-").toUpperCase(),
        r.plano || "-",
        ativFmt,
        termFmt,
        r.modelo || "-",
        r.fabricante || "-",
        r.tempoFidel || "-",
        r.chip || "-",
        r.conta || "-",
        r.cnpj || "-",
        r.servicos || "-",
        r.origem || "-"
      ],
      category,
      priority,
      numberKey: r.numberKey,
      flags: {
        activeLine,
        activeAccount,
        conflict,
        contractExpired: expired,
        contractExpiringSoon: expSoon,
        missingUser,
        missingChip,
        missingUrmobo,
        dupNumber,
        dupChip
      }
    });
  }

  return { rows: processed, stats };
}

function applyTelephonyFilters(rows: TelephonyProcessedRow[], config: TelephonyConfig): TelephonyProcessedRow[] {
  const anyFilterOn = Object.values(config.filters).some(v => v === true);

  const term = (config.searchTerm || "").toString().trim().toUpperCase();
  const filterTerm = (config.filterTerm || "").toString().trim().toUpperCase();
  const filterDigits = digitsOnly(filterTerm);

  const out: TelephonyProcessedRow[] = [];

  for (const r of rows) {
    let searchOk = true;
    if (term) {
      const blob = r.arr.map(x => (x ?? "").toString()).join(" ").toUpperCase();
      searchOk = blob.includes(term) || digitsOnly(blob).includes(digitsOnly(term));
    }

    let filterOk = true;
    if (filterTerm) {
      if (filterDigits.length === 2) {
        const ddd = (r.arr[6] || "").toString().trim();
        filterOk = ddd === filterDigits;
      } else {
        const blob = r.arr.map(x => (x ?? "").toString()).join(" ").toUpperCase();
        filterOk = blob.includes(filterTerm);
      }
    }

    let filterMatch = !anyFilterOn;

    if (config.filters.activeLine && r.flags.activeLine) filterMatch = true;
    if (config.filters.inactiveLine && !r.flags.activeLine) filterMatch = true;
    if (config.filters.inactiveAccount && !r.flags.activeAccount) filterMatch = true;
    if (config.filters.conflictActiveAccountInactive && r.flags.conflict) filterMatch = true;
    if (config.filters.contractExpired && r.flags.contractExpired) filterMatch = true;
    if (config.filters.contractExpiringSoon && r.flags.contractExpiringSoon) filterMatch = true;
    if (config.filters.missingUser && r.flags.missingUser) filterMatch = true;
    if (config.filters.missingChip && r.flags.missingChip) filterMatch = true;
    if (config.filters.missingUrmobo && r.flags.missingUrmobo) filterMatch = true;
    if (config.filters.dupNumber && r.flags.dupNumber) filterMatch = true;
    if (config.filters.dupChip && r.flags.dupChip) filterMatch = true;


    if (searchOk && filterOk && filterMatch) out.push(r);
  }

  out.sort((a, b) => {
    const p = a.priority - b.priority;
    if (p !== 0) return p;
    const ua = (a.arr[1] || "").toString();
    const ub = (b.arr[1] || "").toString();
    return ua.localeCompare(ub);
  });

  return out;
}

function renderTelephonyOutput(workbook: ExcelScript.Workbook, rows: TelephonyProcessedRow[], config: TelephonyConfig) {
  let sheet = workbook.getWorksheet(CONSTANTS.SHEET_OUTPUT);
  if (!sheet) sheet = workbook.getWorksheet(CONSTANTS.SHEET_MENU);
  if (!sheet) return;

  const startRow = 3;
  const startCol = 2;

  const headers: string[] = [
    "IND",
    "USUARIO_CADASTRADO",
    "URMOBO",
    "TEL_1",
    "TEL_2",
    "USUARIO_URMOBO",
    "DDD",
    "NUMERO",
    "STATUS_LINHA",
    "STATUS_CONTA",
    "PLANO_LINHA",
    "DATA_ATIVACAO",
    "TERMINO_CONTRATO"
  ];


  if (config.optionalCols.modelo) headers.push("MODELO_APARELHO");
  if (config.optionalCols.fabricante) headers.push("FABRICANTE");
  if (config.optionalCols.tempoFidelizacao) headers.push("TEMPO_FIDELIZACAO");
  if (config.optionalCols.chip) headers.push("CHIP");
  if (config.optionalCols.conta) headers.push("CONTA");
  if (config.optionalCols.cnpj) headers.push("CNPJ");
  if (config.optionalCols.servicos) headers.push("SERVICOS_ATIVOS");
  if (config.optionalCols.origem) headers.push("ORIGEM");
  if (config.optionalCols.waMe) headers.push("WA_ME");

  const tables = sheet.getTables();
  tables.forEach(t => t.delete());

  safeClearOutputArea(sheet, startRow, startCol, headers.length);

  if (rows.length === 0) return;

  const data = rows.map(r => {
    const base = r.arr.slice(0, 13);
    const opt: (string | number | boolean)[] = [];

    if (config.optionalCols.modelo) opt.push(r.arr[13] ?? "-");
    if (config.optionalCols.fabricante) opt.push(r.arr[14] ?? "-");
    if (config.optionalCols.tempoFidelizacao) opt.push(r.arr[15] ?? "-");
    if (config.optionalCols.chip) opt.push(r.arr[16] ?? "-");
    if (config.optionalCols.conta) opt.push(r.arr[17] ?? "-");
    if (config.optionalCols.cnpj) opt.push(r.arr[18] ?? "-");
    if (config.optionalCols.servicos) opt.push(r.arr[19] ?? "-");
    if (config.optionalCols.origem) opt.push(r.arr[20] ?? "-");

    if (config.optionalCols.waMe) {
      const nk =
        (r.numberKey && r.numberKey.toString().trim() !== "")
          ? r.numberKey.toString().trim()
          : canonicalPhoneKeyFromDigits(digitsOnly((r.arr[7] ?? "").toString()));

      opt.push(nk ? `https://wa.me/55${nk}` : "-");
    }

    const arr = [...base, ...opt];
    while (arr.length < headers.length) arr.push("-");
    return arr;
  });


  const fullRange = sheet.getRangeByIndexes(startRow, startCol, data.length + 1, headers.length);
  const table = sheet.addTable(fullRange, true);
  safeSetTableName(workbook, table, "TabelaTelefonia");
  table.setShowFilterButton(true);
  table.setShowBandedRows(false);
  table.setPredefinedTableStyle("TableStyleLight1");

  const headerRange = fullRange.getRow(0);
  headerRange.setValues([headers]);

  const bodyRange = sheet.getRangeByIndexes(startRow + 1, startCol, data.length, headers.length);
  bodyRange.setValues(data);

  headerRange.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
  headerRange.getFormat().getFont().setColor(CONSTANTS.COLORS.accent);
  headerRange.getFormat().getFont().setBold(true);
  headerRange.getFormat().setRowHeight(30);

  const idxInd = headers.indexOf("IND");
  const idxStLinha = headers.indexOf("STATUS_LINHA");
  const idxStConta = headers.indexOf("STATUS_CONTA");
  const idxTerm = headers.indexOf("TERMINO_CONTRATO");
  const idxNumero = headers.indexOf("NUMERO");
  const idxUsuario = headers.indexOf("USUARIO_CADASTRADO");
  const idxUrmobo = headers.indexOf("URMOBO");
  const idxTel1 = headers.indexOf("TEL_1");
  const idxTel2 = headers.indexOf("TEL_2");
  const idxUrmoboUser = headers.indexOf("USUARIO_URMOBO");
  const idxChip = headers.indexOf("CHIP");
  const idxCnpj = headers.indexOf("CNPJ");

  const idxServices = headers.indexOf("SERVICOS_ATIVOS");
  if (idxServices !== -1) {
    try { bodyRange.getColumn(idxServices).getFormat().setWrapText(true); } catch (e) { }
  }

  for (let i = 0; i < rows.length; i++) {
    const r = rows[i];
    const getCell = (colIdx: number) => bodyRange.getCell(i, colIdx);

    if (idxInd !== -1) {
      const c = getCell(idxInd);
      const v = (r.arr[0] || "").toString().trim();
      if (v === "🔴") {
        c.getFormat().getFill().setColor("#FEE2E2");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.risk);
        c.getFormat().getFont().setBold(true);
        c.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);
      } else if (v === "🟠") {
        c.getFormat().getFill().setColor("#FFEDD5");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.warning);
        c.getFormat().getFont().setBold(true);
        c.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);
      }
    }

    if (idxStLinha !== -1) {
      const c = getCell(idxStLinha);
      if (r.flags.activeLine) {
        c.getFormat().getFill().setColor("#DCFCE7");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.success);
        c.getFormat().getFont().setBold(true);
      } else {
        c.getFormat().getFill().setColor("#FEE2E2");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.risk);
        c.getFormat().getFont().setBold(true);
      }
    }

    const paintCheck = (colIdx: number) => {
      if (colIdx === -1) return;
      const c = getCell(colIdx);
      const v = (r.arr[colIdx] || "").toString().trim();
      if (v === "✅") {
        c.getFormat().getFill().setColor("#DCFCE7");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.success);
        c.getFormat().getFont().setBold(true);
        c.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);
      } else if (v === "❌") {
        c.getFormat().getFill().setColor("#FEE2E2");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.risk);
        c.getFormat().getFont().setBold(true);
        c.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);
      }
    };


    paintCheck(idxUrmobo);
    paintCheck(idxTel1);
    paintCheck(idxTel2);

    if (idxUrmoboUser !== -1 && idxUrmobo !== -1) {
      const c = getCell(idxUrmoboUser);
      const has = ((r.arr[idxUrmobo] || "").toString().trim() === "✅");
      if (!has) {
        c.getFormat().getFill().setColor("#E5E7EB");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.textMuted);
        c.getFormat().getFont().setBold(true);
      }
    }

    if (idxStConta !== -1) {
      const c = getCell(idxStConta);
      if (r.flags.activeAccount) {
        c.getFormat().getFill().setColor("#DCFCE7");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.success);
        c.getFormat().getFont().setBold(true);
      } else {
        c.getFormat().getFill().setColor("#FEE2E2");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.risk);
        c.getFormat().getFont().setBold(true);
      }
    }

    if (r.flags.conflict && idxStLinha !== -1 && idxStConta !== -1) {
      const c1 = getCell(idxStLinha);
      const c2 = getCell(idxStConta);
      c1.getFormat().getFill().setColor("#EDE9FE");
      c2.getFormat().getFill().setColor("#EDE9FE");
      c1.getFormat().getFont().setColor("#7C3AED");
      c2.getFormat().getFont().setColor("#7C3AED");
      c1.getFormat().getFont().setBold(true);
      c2.getFormat().getFont().setBold(true);
    }

    if (idxTerm !== -1) {
      const c = getCell(idxTerm);
      const t = (r.arr[idxTerm] || "").toString().trim().toUpperCase();

      if (t.includes("NÃO POSSUI") || t.includes("NAO POSSUI")) {
        c.getFormat().getFill().setColor("#E5E7EB");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.textMuted);
        c.getFormat().getFont().setBold(true);
      } else if (r.flags.contractExpired) {
        c.getFormat().getFill().setColor("#FEE2E2");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.risk);
        c.getFormat().getFont().setBold(true);
      } else if (r.flags.contractExpiringSoon) {
        c.getFormat().getFill().setColor("#FFEDD5");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.warning);
        c.getFormat().getFont().setBold(true);
      }
    }

    if (r.flags.dupNumber && idxNumero !== -1) {
      const c = getCell(idxNumero);
      c.getFormat().getFill().setColor("#FFEDD5");
      c.getFormat().getFont().setColor(CONSTANTS.COLORS.warning);
      c.getFormat().getFont().setBold(true);
    }

    if (r.flags.dupChip && idxChip !== -1) {
      const c = getCell(idxChip);
      c.getFormat().getFill().setColor("#FFEDD5");
      c.getFormat().getFont().setColor(CONSTANTS.COLORS.warning);
      c.getFormat().getFont().setBold(true);
    }


    if (idxUsuario !== -1) {
      const v = (r.arr[idxUsuario] || "").toString().trim();
      if (v && v !== "-") {
        const c = getCell(idxUsuario);
        c.getFormat().getFill().setColor(getStableTagColor(v));
      }
    }

    if (idxCnpj !== -1) {
      const v = (r.arr[idxCnpj] || "").toString().trim();
      if (v && v !== "-") {
        const c = getCell(idxCnpj);
        c.getFormat().getFill().setColor(getStableTagColor(v));
      }
    }

    if (r.flags.missingUser && idxUsuario !== -1) {
      const c = getCell(idxUsuario);
      c.getFormat().getFill().setColor("#E5E7EB");
      c.getFormat().getFont().setColor(CONSTANTS.COLORS.textMuted);
      c.getFormat().getFont().setBold(true);
    }
  }

  sheet.getRange("C:Z").getFormat().autofitColumns();
  sheet.getRange("D:D").getFormat().setColumnWidth(200);
}

function updateTelephonyWidgets(sheet: ExcelScript.Worksheet, stats: TelephonyStats) {
  const critical = stats.conflict + stats.contractExpired;
  const anomalies = stats.dupNumber + stats.dupChip + stats.contractExpiringSoon + stats.missingUser + stats.missingChip + stats.missingUrmobo + stats.inactiveAccount;

  const setVal = (col: string, val: number) => {
    const r = sheet.getRange(`${col}3`);
    r.setValue(val);
    r.getFormat().getFont().setSize(24);
    r.getFormat().getFont().setBold(true);
    r.getFormat().getFont().setColor(CONSTANTS.COLORS.textMain);
    r.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.left);
  };

  setVal("C", stats.total);
  setVal("E", critical);
  setVal("G", anomalies);

  const labels = [
    [`LINHA ATIVA [${stats.active}]`],
    [`LINHA INATIVA [${stats.inactive}]`],
    [`CONTA INATIVA [${stats.inactiveAccount}]`],
    [`CONFLITO (ATIVA + CONTA INATIVA) [${stats.conflict}]`],
    [`CONTRATO VENCIDO [${stats.contractExpired}]`],
    [`CONTRATO (VENCE EM BREVE) [${stats.contractExpiringSoon}]`],
    [`SEM USUÁRIO CADASTRADO [${stats.missingUser}]`],
    [`SEM CHIP [${stats.missingChip}]`],
    [`SEM URMOBO [${stats.missingUrmobo}]`],
    [`DUPLICIDADE DE NÚMERO [${stats.dupNumber}]`],
    [`DUPLICIDADE DE CHIP [${stats.dupChip}]`]
  ];
  sheet.getRange("A10:A20").setValues(labels);

  const statusBar = sheet.getRange("A3");
  if (critical > 0) {
    statusBar.setValue(`🔴 CRÍTICO: ${critical} PROBLEMAS`);
    statusBar.getFormat().getFill().setColor(CONSTANTS.COLORS.risk);
    statusBar.getFormat().getFont().setColor("#FFFFFF");
  } else if (anomalies > 0) {
    statusBar.setValue(`🟠 ALERTA: ${anomalies} ANOMALIAS`);
    statusBar.getFormat().getFill().setColor(CONSTANTS.COLORS.warning);
    statusBar.getFormat().getFont().setColor("#FFFFFF");
  } else {
    statusBar.setValue(`✅ TELEFONIA SEM ALERTAS`);
    statusBar.getFormat().getFill().setColor(CONSTANTS.COLORS.success);
    statusBar.getFormat().getFont().setColor("#FFFFFF");
  }
}

function loadPaDatabase(workbook: ExcelScript.Workbook): Map<string, PaInfo> {
  const map = new Map<string, PaInfo>();
  const sheet = workbook.getWorksheet(CONSTANTS.SHEET_PA_DB);
  if (!sheet) return map;

  const range = sheet.getUsedRange();
  if (!range) return map;

  const texts = range.getTexts();
  if (texts.length < 2) return map;

  const h = texts[0];
  const idxCode = getIdx(h, "Código");
  const idxName = getIdx(h, "Nome");
  const idxCity = getIdx(h, "Endereço: Cidade");

  const safeIdxCode = idxCode !== -1 ? idxCode : h.findIndex(c => c.toUpperCase().includes("COD"));
  const safeIdxName = idxName !== -1 ? idxName : h.findIndex(c => c.toUpperCase().includes("PA COMPLETO"));
  const safeIdxCity = idxCity !== -1 ? idxCity : h.findIndex(c => c.toUpperCase().includes("CIDADE"));

  for (let i = 1; i < texts.length; i++) {
    const row = texts[i];
    let code = safeIdxCode !== -1 ? row[safeIdxCode].trim() : "";
    if (code.length === 1) code = "0" + code;

    const name = safeIdxName !== -1 ? row[safeIdxName].trim() : "";
    const city = safeIdxCity !== -1 ? row[safeIdxCity].trim() : "";

    if (code) {
      map.set(code, {
        code: code,
        name: name,
        city: city,
        fullName: `PA ${code} - ${name} (${city})`
      });
    }
  }
  return map;
}
function loadUemExtra(workbook: ExcelScript.Workbook): Map<string, { user: string; win11: string; chassis: string }> {
  const map = new Map<string, { user: string; win11: string; chassis: string }>();
  const sheet = workbook.getWorksheet("UEM_EXTRA");
  if (!sheet) return map;

  const used = sheet.getUsedRange();
  if (!used) return map;

  const data = used.getTexts();
  if (!data || data.length < 2) return map;

  const norm = (v: string) =>
    (v || "")
      .toString()
      .trim()
      .replace(/^\uFEFF/, "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toUpperCase();




  let headerRow = -1;

  for (let r = 0; r < data.length; r++) {
    const row = data[r];
    if (!row) continue;

    for (let c = 0; c < row.length; c++) {
      if (norm(row[c]) === "DEVICE_FRIENDLY_NAME") {
        headerRow = r;
        break;
      }
    }
    if (headerRow !== -1) break;
  }


  if (headerRow === -1) headerRow = 0;

  const h: string[] = data[headerRow] || [];




  const idxName = getIdx(h, "device_friendly_name");
  const idxUser = getIdx(h, "status_usuariologado");
  const idxWin11 = getIdx(h, "status_check_win11");


  let idxChassis = getIdx(h, "Chassis");
  if (idxChassis === -1) idxChassis = getIdx(h, "chassis");
  if (idxChassis === -1) idxChassis = getIdx(h, "Chassis Type");
  if (idxChassis === -1) idxChassis = getIdx(h, "chassis_type");
  if (idxChassis === -1) idxChassis = getIdx(h, "Chassi");
  if (idxChassis === -1) idxChassis = getIdx(h, "Tipo de chassi");


  if (idxChassis === -1) {
    idxChassis = h.findIndex(col => norm(col).includes("CHASSI"));
  }


  console.log(`UEM_EXTRA headerRow=${headerRow + 1} idxChassis=${idxChassis} headerChassis=${idxChassis !== -1 ? h[idxChassis] : "NAO ENCONTRADO"}`);


  if (idxName === -1) {
    console.log("AVISO: UEM_EXTRA -> coluna device_friendly_name não encontrada no header detectado.");
    return map;
  }




  for (let i = headerRow + 1; i < data.length; i++) {
    const r = data[i];
    if (!r || r.length === 0) continue;

    const rawName = (r[idxName] || "").toString().trim();
    const key = normalizeSicFromExtra(rawName);
    if (!key) continue;

    const rawUser = (idxUser !== -1 && r[idxUser]) ? r[idxUser] : "";
    const rawWin11 = (idxWin11 !== -1 && r[idxWin11]) ? r[idxWin11] : "";
    const rawChassis = (idxChassis !== -1 && r[idxChassis]) ? r[idxChassis] : "";


    const chassisClean = (rawChassis || "").toString().trim();

    map.set(key, {
      user: parseUserLogado(rawUser),
      win11: (rawWin11 || "").toString(),
      chassis: chassisClean
    });
  }

  console.log(`UEM_EXTRA carregado: ${map.size} registros`);
  return map;
}




function loadAssetSet(workbook: ExcelScript.Workbook): Set<string> {
  const set = new Set<string>();
  const sheet = workbook.getWorksheet("ASSET");
  if (!sheet) return set;

  const used = sheet.getUsedRange();
  if (!used) return set;

  const data = used.getTexts();
  if (!data || data.length === 0) return set;

  const norm = (v: string) =>
    (v || "")
      .toString()
      .trim()
      .replace(/^\uFEFF/, "")
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toUpperCase();

  let headerRow = -1;
  let idxNomeAtivo = -1;


  for (let r = 0; r < data.length; r++) {
    const row = data[r];
    if (!row) continue;

    for (let c = 0; c < row.length; c++) {
      if (norm(row[c]) === "NOME DO ATIVO") {
        headerRow = r;
        idxNomeAtivo = c;
        break;
      }
    }
    if (headerRow !== -1) break;
  }


  if (headerRow === -1) {

    const h0: string[] = (data.length > 0 && data[0]) ? data[0] : [];
    const idx0 = getIdx(h0, "Nome do ativo");
    if (idx0 !== -1) {
      headerRow = 0;
      idxNomeAtivo = idx0;
    }
  }

  if (headerRow === -1 || idxNomeAtivo === -1) {
    console.log('AVISO: ASSET -> coluna "Nome do ativo" não encontrada (header dinâmico).');
    return set;
  }


  for (let i = headerRow + 1; i < data.length; i++) {
    const row = data[i];
    if (!row || row.length === 0) continue;

    const raw = (row.length > idxNomeAtivo && row[idxNomeAtivo]) ? row[idxNomeAtivo] : "";
    const key = normalizeAssetHostname(raw);
    if (key) set.add(key);
  }


  console.log(`ASSET carregado: ${set.size} ativos (headerRow=${headerRow + 1}, colIdx=${idxNomeAtivo + 1}).`);
  return set;
}



function main(workbook: ExcelScript.Workbook) {
  let sheetMenu = workbook.getWorksheet(CONSTANTS.SHEET_MENU);
  if (!sheetMenu) {
    sheetMenu = workbook.addWorksheet(CONSTANTS.SHEET_MENU);
    sheetMenu.setPosition(0);
  }

  const mode = getDashboardMode(sheetMenu);


  if (mode === "TELEFONIA") {
    const telConfig = resetAndDrawTelephonyDashboard(sheetMenu);

    const telLoaded = loadTelephony(workbook, telConfig.modules.expiringDays);
    const filtered = applyTelephonyFilters(telLoaded.rows, telConfig);

    renderTelephonyOutput(workbook, filtered, telConfig);
    updateTelephonyWidgets(sheetMenu, telLoaded.stats);

    return;
  }

  const config = resetAndDrawDashboard(sheetMenu);


  syncVerifiedFromExistingOutput(workbook);


  const collaborators = loadCollaborators(workbook);
  const collaboratorPrefixMap = buildUserPrefixMap(collaborators.names);


  const verifiedMap = loadVerifiedUsers(workbook);

  const paMap = loadPaDatabase(workbook);
  const sheetAD = workbook.getWorksheet("AD");
  const sheetUEM = workbook.getWorksheet("UEM");
  const sheetEDR = workbook.getWorksheet("EDR");
  if (!sheetAD || !sheetUEM || !sheetEDR) return;


  const dataAD = sheetAD.getUsedRange()?.getTexts() || [];
  const dataUEM = sheetUEM.getUsedRange()?.getTexts() || [];
  const dataEDR = sheetEDR.getUsedRange()?.getTexts() || [];


  const hAD = dataAD.length > 0 ? dataAD[0] : [];
  const hUEM = dataUEM.length > 0 ? dataUEM[0] : [];
  const hEDR = dataEDR.length > 0 ? dataEDR[0] : [];


  const idxAdName = getIdx(hAD, "Computer Name");
  const idxAdLogon = getIdx(hAD, "Last Logon Time");
  const idxAdPwdSet = getIdx(hAD, "Password Last Set");
  const idxAdOs = getIdx(hAD, "Operating System");


  let idxUemName = getIdx(hUEM, "Hostname");
  if (idxUemName === -1) idxUemName = getIdx(hUEM, "Friendly Name");

  const idxUemUser = getIdx(hUEM, "Username");
  const idxUemSerial = getIdx(hUEM, "Serial Number");
  const idxUemSeen = getIdx(hUEM, "Last Seen");
  const idxUemDmSeen = getIdx(hUEM, "DM Last Seen");
  const idxUemOs = getIdx(hUEM, "OS");
  const idxUemModel = getIdx(hUEM, "Model");


  let idxEdrName = getIdx(hEDR, "Friendly Name");
  if (idxEdrName === -1) idxEdrName = getIdx(hEDR, "Hostname");


  if (idxEdrName === -1) console.log("AVISO: Coluna de nome não encontrada no EDR (Tentado: Friendly Name, Hostname)");

  const idxEdrUser = getIdx(hEDR, "Last Logged In User Account");
  const idxEdrSeen = getIdx(hEDR, "Last Seen");
  const idxEdrLogin = getIdx(hEDR, "Last User Account Login");
  const idxEdrSerial = getIdx(hEDR, "Serial Number");
  const idxEdrOs = getIdx(hEDR, "OS Version");
  const idxEdrIp = getIdx(hEDR, "Local IP");
  const idxEdrTags = getIdx(hEDR, "Sensor Tags");
  const idxEdrChassis = getIdx(hEDR, "Chassis");


  let masterMap = new Map<string, MachineData>();
  const normalize = (s: string) => {
    if (!s) return "";
    let out = s.toString().trim().toUpperCase();

    out = out.replace(/\..*$/, "");

    return out;
  };

  function getBaseName(name: string): string {
    if (!name) return "";
    const parts = name.toUpperCase().split("_");
    if (parts.length <= 1) return name.toUpperCase();


    parts.pop();
    return parts.join("_");
  }


  const extractPa = (name: string): string => {
    const parts = name.split('_');
    if (parts.length >= 4) {
      return parts[3];
    }
    return "??";
  };

  const upsert = (row: string[], source: "AD" | "UEM" | "EDR", nameIdx: number) => {
    if (nameIdx === -1) return;


    if (!row || row.length <= nameIdx) return;

    const name = row[nameIdx];
    if (!name) return;

    const key = normalize(name);

    if (!masterMap.has(key)) {

      const rawName = name.trim();
      masterMap.set(key, {
        name: rawName,
        paCode: extractPa(rawName),
        sources: { AD: false, UEM: false, EDR: false, ASSET: false },
        data: {

          adLogon: "", adPwdSet: "", adOs: "",
          uemUser: "", uemSeen: "", uemDmSeen: "", uemSerial: "", uemOs: "",
          edrUser: "", edrSeen: "", edrLogin: "", edrSerial: "", edrOs: "",
          model: "", ip: "", tags: "", edrChassis: "",

          uemExtraUserLogado: "",
          uemExtraCheckWin11: "",
          uemExtraChassis: "",

          lastSeenScore: 0,
          lastSeenSource: "",


        },
        isVirtualGap: false
      });
    }
    const entry = masterMap.get(key)!;
    entry.sources[source] = true;
    const val = (idx: number) => (idx !== -1 && row[idx]) ? row[idx] : "";

    if (source === "AD") {
      entry.data.adLogon = val(idxAdLogon);
      entry.data.adPwdSet = val(idxAdPwdSet);
      entry.data.adOs = val(idxAdOs);


      entry.data.lastSeenScore = parseDateScore(entry.data.adLogon);
      entry.data.lastSeenSource = entry.data.lastSeenScore > 0 ? "AD" : "";

    } else if (source === "UEM") {
      entry.data.uemUser = val(idxUemUser);
      entry.data.uemSeen = val(idxUemSeen);
      entry.data.uemDmSeen = val(idxUemDmSeen);
      entry.data.uemSerial = val(idxUemSerial);
      entry.data.uemOs = val(idxUemOs);

      if (val(idxUemModel)) entry.data.model = val(idxUemModel);


      const uemScore = parseDateScore(entry.data.uemSeen);
      if (entry.data.lastSeenScore === 0 && uemScore > 0) {
        entry.data.lastSeenScore = uemScore;
        entry.data.lastSeenSource = "UEM";
      }
    }
    else if (source === "EDR") {
      entry.data.edrUser = val(idxEdrUser); entry.data.edrSeen = val(idxEdrSeen); entry.data.edrLogin = val(idxEdrLogin);
      entry.data.edrSerial = val(idxEdrSerial);
      entry.data.edrOs = val(idxEdrOs);
      entry.data.edrChassis = val(idxEdrChassis);
      if (val(idxEdrIp)) entry.data.ip = val(idxEdrIp);
      if (val(idxEdrTags)) entry.data.tags = val(idxEdrTags);

      const edrScore = parseDateScore(entry.data.edrSeen);




      const canOverrideWithEdr =
        (!entry.sources.AD) ||
        (entry.data.lastSeenScore === 0) ||
        (entry.data.lastSeenSource === "") ||
        (entry.data.lastSeenSource === "UEM");

      if (canOverrideWithEdr && edrScore > 0) {
        entry.data.lastSeenScore = edrScore;
        entry.data.lastSeenSource = "EDR";
      }
    }
  };

  for (let i = 1; i < dataAD.length; i++) upsert(dataAD[i], "AD", idxAdName);
  for (let i = 1; i < dataUEM.length; i++) upsert(dataUEM[i], "UEM", idxUemName);
  for (let i = 1; i < dataEDR.length; i++) upsert(dataEDR[i], "EDR", idxEdrName);



  const uemExtraMap = loadUemExtra(workbook);


  const sicJoinIndex = new Map<string, string>();
  for (const k of Array.from(masterMap.keys())) {
    const base = normalizeSicFromExtra(k);
    if (base && !sicJoinIndex.has(base)) sicJoinIndex.set(base, k);
  }

  for (const [baseKey, extra] of Array.from(uemExtraMap.entries())) {
    if (!baseKey) continue;


    const resolvedKey =
      masterMap.has(baseKey) ? baseKey :
        (sicJoinIndex.get(baseKey) || baseKey);

    if (!masterMap.has(resolvedKey)) {

      masterMap.set(resolvedKey, {
        name: resolvedKey,
        paCode: extractPa(resolvedKey),
        sources: { AD: false, UEM: true, EDR: false, ASSET: false },
        data: {
          adLogon: "", adPwdSet: "", adOs: "",
          uemUser: "", uemSeen: "", uemDmSeen: "", uemSerial: "", uemOs: "",
          edrUser: "", edrSeen: "", edrLogin: "", edrSerial: "", edrOs: "", edrChassis: "",
          model: "", ip: "", tags: "",
          uemExtraUserLogado: extra.user || "",
          uemExtraCheckWin11: extra.win11 || "",
          uemExtraChassis: extra.chassis || "",
          lastSeenScore: 0,
          lastSeenSource: ""
        },
        isVirtualGap: false
      });
    } else {
      const e = masterMap.get(resolvedKey)!;


      if (extra.user !== undefined) e.data.uemExtraUserLogado = extra.user || e.data.uemExtraUserLogado;
      if (extra.win11 !== undefined) e.data.uemExtraCheckWin11 = extra.win11 || e.data.uemExtraCheckWin11;
      if (extra.chassis !== undefined) e.data.uemExtraChassis = extra.chassis || e.data.uemExtraChassis;
    }
  }



  const assetSet = loadAssetSet(workbook);

  let gapCount = 0;

  if (config.filters.available) {
    const regexPattern = /([a-zA-Z]+[_\W]+)(\d+)([_\W].*)?/;

    let maxNum = 0; let patternPrefix = ""; let patternSuffix = ""; let foundNumbers = new Set<number>();
    for (const key of Array.from(masterMap.keys())) {
      const m = key.match(regexPattern);
      if (m) {
        const n = parseInt(m[2]);
        if (!isNaN(n)) { foundNumbers.add(n); if (n > maxNum) maxNum = n; if (!patternPrefix) { patternPrefix = m[1]; patternSuffix = m[3] || ""; } }
      }
    }
    const limit = Math.min(maxNum + 20, config.modules.gapLimit);
    for (let i = 1; i <= limit; i++) {
      if (!foundNumbers.has(i)) {
        let numStr = i.toString().padStart(2, '0');
        gapCount++;
        masterMap.set(`__GAP__${i}`, {
          name: `${patternPrefix}${numStr}${patternSuffix}`,
          paCode: "??",
          sources: { AD: false, UEM: false, EDR: false, ASSET: false },
          data: {

            adLogon: "", adPwdSet: "", adOs: "",
            uemUser: "", uemSeen: "", uemDmSeen: "", uemSerial: "", uemOs: "",
            edrUser: "", edrSeen: "", edrLogin: "", edrSerial: "", edrOs: "",
            model: "", ip: "", tags: "", edrChassis: "",

            uemExtraUserLogado: "",
            uemExtraCheckWin11: "",
            uemExtraChassis: "",

            lastSeenScore: 0,
            lastSeenSource: "" as const


          },
          isVirtualGap: true
        });
      }
    }
  }


  for (const [k, m] of Array.from(masterMap.entries())) {
    const base = normalizeSicFromExtra(k);
    m.sources.ASSET = assetSet.has(k) || (base ? assetSet.has(base) : false);
  }


  let serialMap = new Map<string, string[]>();

  if (config.modules.cloneEnabled) {
    for (const m of Array.from(masterMap.values())) {
      if (m.isVirtualGap) continue;
      const s = m.data.edrSerial || m.data.uemSerial;
      if (s && s.length >= 5) {
        if (!serialMap.has(s)) serialMap.set(s, []);
        serialMap.get(s)!.push(getFinalId(m.name));
      }
    }
  }

  function getFinalId(name: string): string {
    if (!name) return "";
    const parts = name.toUpperCase().split("_");
    return parts.length > 1 ? parts[parts.length - 1] : name.toUpperCase();
  }

  let stats: Stats = {
    total: 0, compliant: 0, missingUem: 0, missingEdr: 0, missingAsset: 0, phantom: 0, rogue: 0,
    swap: 0, clone: 0, legacy: 0, offline: 0, gap: 0, available: 0,
    details: {}
  };

  const keys = [
    "COMPLIANT",
    "MISSING_UEM",
    "MISSING_EDR",
    "MISSING_ASSET",
    "PHANTOM",
    "ROGUE",
    "SWAP",
    "CLONE",
    "LEGACY",
    "OFFLINE",
    "GAP",
    "AVAILABLE",
    "INCONSISTENCY"
  ];


  for (const k of keys) { stats.details[k] = 0; }
  stats.gap = gapCount;
  stats.details["GAP"] = gapCount;


  let processed: ProcessedRow[] = [];
  const now = new Date().getTime();

  const buildAvailableRow = (hostname: string): (string | number | boolean)[] => {
    const arr: (string | number | boolean)[] = ["", hostname, "ℹ️ DISPONÍVEL"];

    if (config.optionalCols.sources) arr.push("-", "-", "-", "-");
    if (config.optionalCols.ultimo_sinal) arr.push("-");
    if (config.optionalCols.sinal_individual) arr.push("-", "-", "-");

    // USUÁRIO
    arr.push("-");

    if (config.optionalCols.status_usuariologado) arr.push("-");

    // USUÁRIOS VERIFICADOS
    arr.push("");
    // OS
    arr.push("-");

    if (config.filters.legacy) arr.push("-");
    if (config.optionalCols.status_check_win11) arr.push("-");

    // SERIAIS
    arr.push("-", "-");

    if (config.optionalCols.model) arr.push("-");
    if (config.optionalCols.ip) arr.push("-");
    if (config.optionalCols.tags) arr.push("-");
    if (config.optionalCols.chassis) arr.push("-");

    return arr;
  };

  for (const m of Array.from(masterMap.values())) {
    if (m.isVirtualGap) {
      stats.available++;
      stats.details["AVAILABLE"]++;
      const matchesSearch = !!(config.searchTerm && m.name.toUpperCase().includes(config.searchTerm));

      // Exibe "disponíveis" quando:
      // - há busca ativa e o hostname bate, OU
      // - filtro DISPONÍVEL está marcado (sem busca)
      if ((config.searchTerm && matchesSearch) || (!config.searchTerm && config.filters.available)) {
        processed.push({
          arr: buildAvailableRow(m.name),
          colorCategory: "AVAILABLE",
          priority: 99,
          paColor: "#FFFFFF"
        });
      }
      continue;
    }

    stats.total++;
    let statusKey = "COMPLIANT";
    let statusTxt = "✅ SEGURO";
    let priority = 20;

    const sUem = m.data.uemSerial; const sEdr = m.data.edrSerial;
    const mainOs = m.data.adOs || "-";
    const mainUser = m.data.edrUser || m.data.uemUser || "";




    const hostKey = normalizeMachineKey(m.name);

    let verifiedUser = "";
    let verifiedSuggested = false;

    const existingVerified = verifiedMap.get(hostKey);
    if (existingVerified && existingVerified.user) {
      verifiedUser = existingVerified.user;
      verifiedSuggested = (existingVerified.status === "SUGESTAO");
    } else {

      const candidateRaw = (m.data.uemExtraUserLogado || "").trim() || (mainUser || "").trim();
      const suggestion = suggestCollaboratorNameFromUser(candidateRaw, collaboratorPrefixMap);

      if (suggestion) {
        verifiedUser = suggestion;
        verifiedSuggested = true;

        verifiedMap.set(hostKey, {
          user: suggestion,
          status: "SUGESTAO",
          updatedAt: new Date().toISOString()
        });
      }
    }


    const machineSuffixRaw = getFinalId(m.name);
    const machineSuffix = (machineSuffixRaw && machineSuffixRaw.match(/^\d{1,2}$/))
      ? machineSuffixRaw.padStart(2, "0")
      : "";

    const userSuffix = extractUserPaSuffix(mainUser) || extractUserPaSuffix(m.data.uemExtraUserLogado);
    const paMismatch = (machineSuffix !== "" && userSuffix !== "" && machineSuffix !== userSuffix);

    if (paMismatch) {
      priority = Math.min(priority, 4);
    }



    let isLegacy = false;
    if (mainOs && mainOs !== "-") {
      for (const l of config.legacyDefinitions) {
        if (mainOs.toUpperCase().includes(l.toUpperCase())) { isLegacy = true; break; }
      }
    }



    if (!m.sources.AD && (m.sources.UEM || m.sources.EDR)) {
      statusKey = "INCONSISTENCY";
      statusTxt = "🧩 INCONSISTÊNCIA DE BASE";
      priority = 0;
    }
    else if (!m.sources.AD) {
      statusKey = "PHANTOM";
      statusTxt = "👻 FANTASMA (AD)";
      priority = 1;
    }

    else if (!m.sources.UEM && !m.sources.EDR) { statusKey = "ROGUE"; statusTxt = "🚨 PERIGO (SEM EDR & UEM)"; priority = 0; }
    else if (!m.sources.UEM) { statusKey = "MISSING_UEM"; statusTxt = "⚠️ FALTA UEM"; priority = 2; }
    else if (!m.sources.EDR) { statusKey = "MISSING_EDR"; statusTxt = "⚠️ FALTA EDR"; priority = 2; }
    else {
      if (sUem && sEdr && sUem !== sEdr) {
        statusKey = "SWAP"; statusTxt = "🔄 TROCA DE SERIAL"; priority = 3;
      }
      else if (
        config.modules.cloneEnabled &&
        sEdr &&
        serialMap.has(sEdr) &&
        serialMap.get(sEdr)!.length !== new Set(serialMap.get(sEdr)).size
      ) {
        statusKey = "CLONE";
        statusTxt = "👯 DUPLICADO";
        priority = 3;
      }
    }

    if (m.data.lastSeenScore > 0) {
      let days = Math.floor((now - m.data.lastSeenScore) / (1000 * 60 * 60 * 24));
      if (days > config.modules.staleDays && statusKey === "COMPLIANT") {
        statusKey = "OFFLINE";
        statusTxt = `💤 OFFLINE (${days}d)`;
        priority = 6;
      }
    }

    if (isLegacy) {
      stats.legacy++;
      stats.details["LEGACY"] = (stats.details["LEGACY"] || 0) + 1;
    }
    if (stats.details[statusKey] !== undefined) stats.details[statusKey]++;
    else stats.details[statusKey] = 1;


    if (statusKey === "COMPLIANT") stats.compliant++;
    else if (statusKey === "PHANTOM") stats.phantom++;
    else if (statusKey === "ROGUE") stats.rogue++;
    else if (statusKey === "MISSING_UEM") stats.missingUem++;
    else if (statusKey === "MISSING_EDR") stats.missingEdr++;
    else if (statusKey === "SWAP") stats.swap++;
    else if (statusKey === "CLONE") stats.clone++;
    else if (statusKey === "OFFLINE") stats.offline++;


    const hasAD = m.sources.AD;
    const hasUEM = m.sources.UEM;
    const hasEDR = m.sources.EDR;
    const hasASSET = m.sources.ASSET;


    const isMissingAsset = (hasAD && !hasASSET && (hasUEM || hasEDR));
    if (isMissingAsset) {
      stats.missingAsset++;
      stats.details["MISSING_ASSET"] = (stats.details["MISSING_ASSET"] || 0) + 1;
    }

    const anyFilterOn = Object.values(config.filters).some(v => v === true);



    let queryMatch = true;

    if (config.searchTerm) {
      queryMatch = m.name.toUpperCase().includes(config.searchTerm);
    }
    else if (config.paFilter) {
      const paInfo = paMap.get(m.paCode);
      const filter = config.paFilter.toUpperCase();
      const matchCode = m.paCode === filter;
      const matchName = paInfo ? paInfo.fullName.toUpperCase().includes(filter) : false;
      queryMatch = (matchCode || matchName);
    }


    let filterMatch = !anyFilterOn;

    if (config.filters.inconsistency && statusKey === "INCONSISTENCY") filterMatch = true;
    if (config.filters.compliant && statusKey === "COMPLIANT") filterMatch = true;
    if (config.filters.phantom && statusKey === "PHANTOM") filterMatch = true;
    if (config.filters.swap && statusKey === "SWAP") filterMatch = true;
    if (config.filters.clone && statusKey === "CLONE") filterMatch = true;
    if (config.filters.legacy && isLegacy) filterMatch = true;
    if (config.filters.offline && statusKey === "OFFLINE") filterMatch = true;

    if (config.filters.rogue && hasAD && !hasUEM && !hasEDR) filterMatch = true;
    if (config.filters.missingUem && hasAD && !hasUEM && hasEDR) filterMatch = true;
    if (config.filters.missingEdr && hasAD && hasUEM && !hasEDR) filterMatch = true;


    if (config.filters.missingAsset && isMissingAsset) filterMatch = true;


    if (config.filters.paMismatch && paMismatch) filterMatch = true;



    let include = queryMatch && filterMatch;


    if (include) {

      let ind = "";


      if (statusKey === "INCONSISTENCY") ind += "🟣";


      if (statusKey === "ROGUE" || statusKey === "PHANTOM") ind += "🔴";


      if (statusKey === "MISSING_UEM" || statusKey === "MISSING_EDR") ind += "🟠";
      if (paMismatch) ind += "🟠";


      if (statusKey === "SWAP") ind += "🔄";
      if (statusKey === "CLONE") ind += "👯";


      if (statusKey === "OFFLINE") ind += "💤";
      if (isLegacy) ind += "🧓";


      if (isMissingAsset) ind += "📦";

      const baseArr = [
        ind,
        m.name,
        statusTxt.toUpperCase()
      ];



      if (config.optionalCols.sources) {
        baseArr.push(
          sourceIcon(m.sources.AD),
          sourceIcon(m.sources.UEM),
          sourceIcon(m.sources.EDR),
          sourceIcon(m.sources.ASSET)
        );
      }


      const ultimoSinal =
        m.data.lastSeenScore > 0
          ? getDetailedDuration(m.data.lastSeenScore, now)
          : "-";

      const srcTag =
        (m.data.lastSeenSource === "EDR") ? "EDR" :
          (m.data.lastSeenSource === "UEM") ? "UEM" :
            "";

      const ultimoSinalTxt =
        (srcTag && ultimoSinal !== "-")
          ? `${ultimoSinal} (${srcTag})`
          : ultimoSinal;


      const usAdScore = parseDateScore(m.data.adLogon);
      const usUemScore = parseDateScore(m.data.uemSeen);
      const usEdrScore = parseDateScore(m.data.edrSeen);

      const usAdTxt = usAdScore > 0 ? getDetailedDuration(usAdScore, now) : "-";
      const usUemTxt = usUemScore > 0 ? getDetailedDuration(usUemScore, now) : "-";
      const usEdrTxt = usEdrScore > 0 ? getDetailedDuration(usEdrScore, now) : "-";

      if (config.optionalCols.ultimo_sinal) {
        baseArr.push(ultimoSinalTxt);
      }

      if (config.optionalCols.sinal_individual) {
        baseArr.push(usAdTxt, usUemTxt, usEdrTxt);
      }

      baseArr.push(mainUser);

      if (config.optionalCols.status_usuariologado) {
        baseArr.push(m.data.uemExtraUserLogado || "-");
      }


      baseArr.push(verifiedUser || "");

      baseArr.push(mainOs);


      if (config.filters.legacy) {
        baseArr.push(isLegacy ? "SIM" : "-");
      }

      if (config.optionalCols.status_check_win11) baseArr.push((m.data.uemExtraCheckWin11 || "-"));

      baseArr.push(
        sEdr || "-",
        sUem || "-"
      );




      if (config.optionalCols.model) baseArr.push(m.data.model || "-");
      if (config.optionalCols.ip) baseArr.push(m.data.ip || "-");
      if (config.optionalCols.tags) baseArr.push(m.data.tags || "-");
      if (config.optionalCols.chassis) {
        const chRaw = (m.data.edrChassis || m.data.uemExtraChassis || "-").toString().trim();
        baseArr.push(chRaw ? chRaw.toUpperCase() : "-");
      }


      processed.push({
        arr: baseArr,
        colorCategory: statusKey,
        priority: priority,
        paColor: getPaColor(m.paCode),
        lastSignalFromEdr: (m.data.lastSeenSource === "EDR"),
        lastSignalFromUem: (m.data.lastSeenSource === "UEM"),
        isLegacy,
        paMismatch,


        verifiedSuggested
      });



    }
  }

  saveVerifiedUsers(workbook, verifiedMap);



  if (config.modules.printersEnabled) {

    const printerRows = loadPrinters(workbook);
    renderPrintersOutput(workbook, printerRows);

    updateDashboardWidgets(sheetMenu, stats, now);
    sheetMenu.getRange("A3").setValue("🖨️ MODO IMPRESSORAS ATIVADO");
    return;
  }

  processed.sort((a, b) => {
    const pDiff = a.priority - b.priority;
    if (pDiff !== 0) return pDiff;
    return a.arr[1].toString().localeCompare(b.arr[1].toString());
  });

  renderOutput(workbook, processed, config, collaborators.listFormula);
  updateDashboardWidgets(sheetMenu, stats, now);

}

function updateDashboardWidgets(sheet: ExcelScript.Worksheet, stats: Stats, time: number) {
  const risks = stats.missingUem + stats.missingEdr + stats.missingAsset + stats.phantom + stats.rogue;
  const anomalies = stats.swap + stats.clone + stats.legacy + stats.offline;

  const setVal = (col: string, val: number) => {
    const r = sheet.getRange(`${col}3`);
    r.setValue(val);
    r.getFormat().getFont().setSize(24);
    r.getFormat().getFont().setBold(true);
    r.getFormat().getFont().setColor(CONSTANTS.COLORS.textMain);
    r.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.left);
  };
  setVal("C", stats.total);
  setVal("E", risks);
  setVal("G", anomalies);

  const labels = [
    [`SEGURO (OK) [${stats.details["COMPLIANT"]}]`],
    [`FALTA UEM [${stats.details["MISSING_UEM"]}]`],
    [`FALTA EDR [${stats.details["MISSING_EDR"]}]`],
    [`FALTA ASSET [${stats.details["MISSING_ASSET"]}]`],
    [`FANTASMA (AD) [${stats.details["PHANTOM"]}]`],
    [`PERIGO (SEM AGENTE) [${stats.details["ROGUE"]}]`],
    [`TROCA SERIAL [${stats.details["SWAP"]}]`],
    [`DUPLICADO [${stats.details["CLONE"]}]`],
    [`SISTEMA LEGADO [${stats.details["LEGACY"]}]`],
    [`OFFLINE [${stats.details["OFFLINE"]}]`],
    [`GAP DE NOMES [${stats.gap}]`],
    [`DISPONÍVEL [${stats.details["AVAILABLE"]}]`],
    [`INCONSISTÊNCIA DE BASE [${stats.details["INCONSISTENCY"]}]`]
  ];
  sheet.getRange("A10:A22").setValues(labels);


  const statusBar = sheet.getRange("A3");

  if (risks > 0) {
    statusBar.setValue(`⚠️ ATENÇÃO: ${risks} MÁQUINAS EM RISCO`);
    statusBar.getFormat().getFill().setColor(CONSTANTS.COLORS.risk);
    statusBar.getFormat().getFont().setColor("#FFFFFF");
  } else if (anomalies > 0) {
    statusBar.setValue(`⚠️ ALERTA: ${anomalies} ANOMALIAS DETECTADAS`);
    statusBar.getFormat().getFill().setColor(CONSTANTS.COLORS.warning);
    statusBar.getFormat().getFont().setColor("#FFFFFF");
  } else {
    statusBar.setValue(`✅ AMBIENTE 100% SEGURO`);
    statusBar.getFormat().getFill().setColor(CONSTANTS.COLORS.success);
    statusBar.getFormat().getFont().setColor("#FFFFFF");
  }
}

function renderPrintersOutput(workbook: ExcelScript.Workbook, rows: ProcessedRow[]) {

  let sheet = workbook.getWorksheet(CONSTANTS.SHEET_OUTPUT);
  if (!sheet) sheet = workbook.getWorksheet(CONSTANTS.SHEET_MENU);
  if (!sheet) return;

  const startRow = 3;
  const startCol = 2;

  const headers = [
    "HOSTNAME",
    "TIPO",
    "PA",
    "IP IMPRESSORA",
    "MODELO",
    "NOME DE EXIBIÇÃO",
    "TIPO IMPRESSÃO",
    "MAC",
    "PATRIMÔNIO",
    "LOCAL",
    "USUARIO",
    "SENHA"
  ];


  const existingTables = sheet.getTables();
  existingTables.forEach(t => t.delete());


  safeClearOutputArea(sheet, startRow, startCol, headers.length);

  if (rows.length === 0) return;

  const data = rows.map(r => r.arr);

  const fullRange = sheet.getRangeByIndexes(startRow, startCol, data.length + 1, headers.length);
  const table = sheet.addTable(fullRange, true);
  safeSetTableName(workbook, table, "TabelaImpressoras");
  table.setShowFilterButton(true);
  table.setShowBandedRows(false);
  table.setPredefinedTableStyle("TableStyleLight1");

  const headerRange = fullRange.getRow(0);
  headerRange.setValues([headers]);

  const bodyRange = sheet.getRangeByIndexes(startRow + 1, startCol, data.length, headers.length);
  bodyRange.setValues(data);

  headerRange.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
  headerRange.getFormat().getFont().setColor(CONSTANTS.COLORS.accent);
  headerRange.getFormat().getFont().setBold(true);
  headerRange.getFormat().setRowHeight(30);


  const typeCol = bodyRange.getColumn(1);
  const fmt = typeCol.addConditionalFormat(ExcelScript.ConditionalFormatType.containsText).getTextComparison();
  fmt.setRule({ operator: ExcelScript.ConditionalTextOperator.contains, text: "IMPRESSORA" });
  fmt.getFormat().getFont().setBold(true);

  sheet.getRange("C:Z").getFormat().autofitColumns();
}

function renderOutput(workbook: ExcelScript.Workbook, rows: ProcessedRow[], config: AppConfig, collaboratorListFormula: string) {
  let sheet = workbook.getWorksheet(CONSTANTS.SHEET_OUTPUT);
  if (!sheet) {
    sheet = workbook.getWorksheet(CONSTANTS.SHEET_MENU);
  }
  if (!sheet) return;

  const startRow = 3;
  const startCol = 2;

  const headers = [
    "IND",
    "HOSTNAME",
    "STATUS ATUAL"
  ];


  if (config.optionalCols.sources) {
    headers.push("AD", "UEM", "EDR", "ASSET");
  }


  if (config.optionalCols.ultimo_sinal) {
    headers.push("ÚLTIMO SINAL");
  }

  if (config.optionalCols.sinal_individual) {
    headers.push("U.S AD", "U.S UEM", "U.S EDR");
  }

  headers.push("USUÁRIO");

  if (config.optionalCols.status_usuariologado) {
    headers.push("status_usuariologado");
  }


  headers.push("USUÁRIOS VERIFICADOS");

  headers.push("OS");


  if (config.filters.legacy) {
    headers.push("LEGADO");
  }

  if (config.optionalCols.status_check_win11) headers.push("status_check_win11");

  headers.push(
    "SERIAL EDR",
    "SERIAL UEM"
  );



  if (config.optionalCols.model) headers.push("MODELO");
  if (config.optionalCols.ip) headers.push("IP LOCAL");
  if (config.optionalCols.tags) headers.push("TAGS");
  if (config.optionalCols.chassis) headers.push("CHASSIS");


  const existingTables = sheet.getTables();
  existingTables.forEach(t => t.delete());

  safeClearOutputArea(sheet, startRow, startCol, headers.length);

  if (rows.length === 0) return;

  const data = rows.map(r => {
    const arr = r.arr.slice(0, headers.length);
    while (arr.length < headers.length) arr.push("-");
    return arr;
  });


  const fullRange = sheet.getRangeByIndexes(startRow, startCol, data.length + 1, headers.length);

  const newTable = sheet.addTable(fullRange, true);
  safeSetTableName(workbook, newTable, "TabelaConformidade");
  newTable.setShowFilterButton(true);
  newTable.setShowBandedRows(false);
  newTable.setPredefinedTableStyle("TableStyleLight1");

  const headerRange = fullRange.getRow(0);
  headerRange.setValues([headers]);

  const bodyRange = sheet.getRangeByIndexes(startRow + 1, startCol, data.length, headers.length);
  bodyRange.setValues(data);

  headerRange.getFormat().getFill().setColor(CONSTANTS.COLORS.primary);
  headerRange.getFormat().getFont().setColor(CONSTANTS.COLORS.accent);
  headerRange.getFormat().getFont().setBold(true);
  headerRange.getFormat().setRowHeight(30);

  const dRange = bodyRange;



  const applyColor = (txt: string, color: string) => {
    const idxStatus = headers.indexOf("STATUS ATUAL");
    const statusCol = (idxStatus >= 0) ? dRange.getColumn(idxStatus) : dRange.getColumn(2);

    const r = statusCol
      .addConditionalFormat(ExcelScript.ConditionalFormatType.containsText)
      .getTextComparison();

    r.setRule({ operator: ExcelScript.ConditionalTextOperator.contains, text: txt });
    r.getFormat().getFont().setColor(color);
    r.getFormat().getFont().setBold(true);
  };

  applyColor("SEGURO", CONSTANTS.COLORS.success);
  applyColor("PERIGO", CONSTANTS.COLORS.risk);
  applyColor("FANTASMA", CONSTANTS.COLORS.risk);
  applyColor("FALTA", CONSTANTS.COLORS.warning);
  applyColor("TROCA", CONSTANTS.COLORS.warning);
  applyColor("DUPLICADO", CONSTANTS.COLORS.warning);
  applyColor("OFFLINE", CONSTANTS.COLORS.textMuted);
  applyColor("DISPONÍVEL", CONSTANTS.COLORS.accent);
  applyColor("INCONSISTÊNCIA", "#7C3AED");

  const border = dRange.getFormat().getRangeBorder(ExcelScript.BorderIndex.insideHorizontal);
  border.setStyle(ExcelScript.BorderLineStyle.continuous);
  border.setColor("#E2E8F0");

  const idxUltimoSinal = headers.indexOf("ÚLTIMO SINAL");
  const idxUsAd = headers.indexOf("U.S AD");
  const idxUsUem = headers.indexOf("U.S UEM");
  const idxUsEdr = headers.indexOf("U.S EDR");

  const idxUsuario = headers.indexOf("USUÁRIO");
  const idxUserLogado = headers.indexOf("status_usuariologado");
  const idxVerified = headers.indexOf("USUÁRIOS VERIFICADOS");
  const idxWin11 = headers.indexOf("status_check_win11");
  const idxAd = headers.indexOf("AD");
  const idxUem = headers.indexOf("UEM");
  const idxEdr = headers.indexOf("EDR");
  const idxAsset = headers.indexOf("ASSET");




  if (idxVerified !== -1 && collaboratorListFormula && collaboratorListFormula.trim() !== "") {
    const verifiedColRange = dRange.getColumn(idxVerified);
    const dv = verifiedColRange.getDataValidation();


    dv.clear();


    dv.setRule({
      list: {
        inCellDropDown: true,
        source: collaboratorListFormula
      }
    });


    dv.setIgnoreBlanks(true);


    dv.setErrorAlert({
      showAlert: true,
      style: ExcelScript.DataValidationAlertStyle.stop,
      title: "Valor inválido",
      message: "Selecione um colaborador da lista."
    });
  }





  const paintSource: (rowIdx: number, colIdx: number) => void = (rowIdx, colIdx) => {
    if (colIdx === -1) return;

    const v = ((rows[rowIdx].arr[colIdx] ?? "") as string | number | boolean).toString().trim();
    const c = dRange.getCell(rowIdx, colIdx);

    const isAssetCol = (colIdx === idxAsset);

    if (v === "✅") {
      if (isAssetCol) {
        c.getFormat().getFill().setColor("#DBEAFE");
        c.getFormat().getFont().setColor("#1D4ED8");
        c.getFormat().getFont().setBold(true);
      } else {
        c.getFormat().getFill().setColor("#DCFCE7");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.success);
        c.getFormat().getFont().setBold(true);
      }
    } else if (v === "❌") {
      c.getFormat().getFill().setColor("#FEE2E2");
      c.getFormat().getFont().setColor(CONSTANTS.COLORS.risk);
      c.getFormat().getFont().setBold(true);
    }
  };


  const paintTime: (rowIdx: number, colIdx: number) => void = (rowIdx, colIdx) => {
    if (colIdx === -1) return;

    const v = ((rows[rowIdx].arr[colIdx] ?? "") as string | number | boolean).toString().trim();
    const c = dRange.getCell(rowIdx, colIdx);

    if (!v || v === "-" || v.toUpperCase() === "N/A") return;

    if (v.includes("h atrás")) {
      c.getFormat().getFont().setColor(CONSTANTS.COLORS.success);
      c.getFormat().getFont().setBold(true);
    } else if (v.includes("dias")) {
      c.getFormat().getFont().setColor(CONSTANTS.COLORS.warning);
      c.getFormat().getFont().setBold(true);
    }
  };



  sheet.getRange("C:C").getFormat().setColumnWidth(30);
  sheet.getRange("D:D").getFormat().setColumnWidth(180);


  const isWeirdUser: (v: string | number | boolean) => boolean = (v) => {
    const t = (v ?? "").toString().trim();
    if (!t || t === "-" || t.toUpperCase() === "N/A") return false;
    return extractUserPaSuffix(t) === "";
  };

  for (let i = 0; i < rows.length; i++) {

    paintSource(i, idxAd);
    paintSource(i, idxUem);
    paintSource(i, idxEdr);
    paintSource(i, idxAsset);

    paintTime(i, idxUltimoSinal);
    paintTime(i, idxUsAd);
    paintTime(i, idxUsUem);
    paintTime(i, idxUsEdr);




    const idxHostname = headers.indexOf("HOSTNAME");
    const cell = dRange.getCell(i, idxHostname);

    if (rows[i].paColor && rows[i].paColor !== "#FFFFFF") {
      cell.getFormat().getFill().setColor(rows[i].paColor);
    }


    if (rows[i].isLegacy && !config.filters.legacy) {
      cell.getFormat().getFill().setColor("#FEE2E2");
      cell.getFormat().getFont().setColor("#991B1B");
      cell.getFormat().getFont().setBold(true);
    }


    if (idxWin11 !== -1) {
      const raw = (rows[i].arr[idxWin11] || "").toString().trim().toUpperCase();
      const c = dRange.getCell(i, idxWin11);

      if (raw.startsWith("ELEGIVEL PARA WINDOWS 11")) {
        c.getFormat().getFill().setColor("#DCFCE7");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.success);
        c.getFormat().getFont().setBold(true);
      }
      else if (raw.startsWith("NAO ELEGIVEL PARA WINDOWS 11")) {
        c.getFormat().getFill().setColor("#FEE2E2");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.risk);
        c.getFormat().getFont().setBold(true);
      }
    }




    const idxInd = headers.indexOf("IND");
    if (idxInd !== -1) {
      const c = dRange.getCell(i, idxInd);
      const v = (rows[i].arr[idxInd] ?? "").toString();

      if (!v) {

        c.getFormat().getFill().setColor("#FFFFFF");
      } else {
        c.getFormat().setHorizontalAlignment(ExcelScript.HorizontalAlignment.center);
        c.getFormat().getFont().setBold(true);

        if (v.includes("🔴")) {
          c.getFormat().getFill().setColor("#FEE2E2");
          c.getFormat().getFont().setColor(CONSTANTS.COLORS.risk);
        } else if (v.includes("🟣")) {
          c.getFormat().getFill().setColor("#EDE9FE");
          c.getFormat().getFont().setColor("#7C3AED");
        } else if (v.includes("🟠") || v.includes("📦") || v.includes("🔄") || v.includes("👯")) {
          c.getFormat().getFill().setColor("#FFEDD5");
          c.getFormat().getFont().setColor(CONSTANTS.COLORS.warning);
        } else if (v.includes("💤")) {
          c.getFormat().getFill().setColor("#E5E7EB");
          c.getFormat().getFont().setColor(CONSTANTS.COLORS.textMuted);
        } else if (v.includes("🧓")) {
          c.getFormat().getFill().setColor("#FCE7F3");
          c.getFormat().getFont().setColor("#9D174D");
        }
      }
    }








    if (idxVerified !== -1 && rows[i].verifiedSuggested) {
      const v = (rows[i].arr[idxVerified] ?? "").toString().trim();
      if (v) {
        const c = dRange.getCell(i, idxVerified);
        c.getFormat().getFill().setColor("#FFEDD5");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.warning);
        c.getFormat().getFont().setBold(true);
      }
    }




    if (!rows[i].paMismatch) {


      if (idxUsuario !== -1 && isWeirdUser(rows[i].arr[idxUsuario])) {
        const c = dRange.getCell(i, idxUsuario);
        c.getFormat().getFill().setColor("#E5E7EB");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.textMuted);
        c.getFormat().getFont().setBold(true);
      }

      if (idxUserLogado !== -1 && isWeirdUser(rows[i].arr[idxUserLogado])) {
        const c = dRange.getCell(i, idxUserLogado);
        c.getFormat().getFill().setColor("#E5E7EB");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.textMuted);
        c.getFormat().getFont().setBold(true);
      }
    }




    if (idxUltimoSinal !== -1) {

      if (rows[i].lastSignalFromEdr) {
        const c = dRange.getCell(i, idxUltimoSinal);
        c.getFormat().getFill().setColor("#FFEDD5");
        c.getFormat().getFont().setColor(CONSTANTS.COLORS.warning);
        c.getFormat().getFont().setBold(true);
      }


      if (rows[i].paMismatch) {
        if (idxUsuario !== -1) {
          const c = dRange.getCell(i, idxUsuario);
          c.getFormat().getFill().setColor("#FFEDD5");
          c.getFormat().getFont().setColor(CONSTANTS.COLORS.warning);
          c.getFormat().getFont().setBold(true);
        }

        if (idxUserLogado !== -1) {
          const c = dRange.getCell(i, idxUserLogado);
          c.getFormat().getFill().setColor("#FFEDD5");
          c.getFormat().getFont().setColor(CONSTANTS.COLORS.warning);
          c.getFormat().getFont().setBold(true);
        }
      }


      else if (rows[i].lastSignalFromUem) {
        const c = dRange.getCell(i, idxUltimoSinal);
        c.getFormat().getFill().setColor("#DBEAFE");
        c.getFormat().getFont().setColor("#1D4ED8");
        c.getFormat().getFont().setBold(true);
      }
    }

  }


  sheet.getRange("C:Z").getFormat().autofitColumns();

  if (config.optionalCols.chassis) {
    const idx = headers.indexOf("CHASSIS");
    if (idx !== -1) {
      const chassisCol = dRange.getColumn(idx);

      const fLaptop = chassisCol.addConditionalFormat(
        ExcelScript.ConditionalFormatType.containsText
      ).getTextComparison();
      fLaptop.setRule({ operator: ExcelScript.ConditionalTextOperator.contains, text: "LAPTOP" });
      fLaptop.getFormat().getFill().setColor("#DCFCE7");
      fLaptop.getFormat().getFont().setBold(true);

      const fDesktop = chassisCol.addConditionalFormat(
        ExcelScript.ConditionalFormatType.containsText
      ).getTextComparison();
      fDesktop.setRule({ operator: ExcelScript.ConditionalTextOperator.contains, text: "DESKTOP" });
      fDesktop.getFormat().getFill().setColor("#DBEAFE");
      fDesktop.getFormat().getFont().setBold(true);

      const fOther = chassisCol.addConditionalFormat(
        ExcelScript.ConditionalFormatType.containsText
      ).getTextComparison();
      fOther.setRule({ operator: ExcelScript.ConditionalTextOperator.contains, text: "OTHER" });
      fOther.getFormat().getFill().setColor("#E5E7EB");
      fOther.getFormat().getFont().setBold(true);


    }
  }

}
