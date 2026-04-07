#nullable enable
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.VisualBasic;

namespace Uroboros
{
    public static class AquadatFastCli
    {
        // -----------------------------
        // INI Helper
        // -----------------------------
        public sealed class IniFile
        {
            public string Path { get; }

            public IniFile(string iniPath) => Path = iniPath;

            [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
            private static extern int GetPrivateProfileString(
                string lpAppName,
                string lpKeyName,
                string lpDefault,
                StringBuilder lpReturnedString,
                int nSize,
                string lpFileName
            );

            [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
            private static extern bool WritePrivateProfileString(
                string lpAppName,
                string lpKeyName,
                string lpString,
                string lpFileName
            );

            public string Read(string section, string key, string defaultValue)
            {
                var sb = new StringBuilder(512);
                GetPrivateProfileString(section, key, defaultValue, sb, sb.Capacity, Path);
                return sb.ToString();
            }

            public void Write(string section, string key, string value)
            {
                WritePrivateProfileString(section, key, value, Path);
            }
        }

        // =========================================================
        // Request/Options (CLI)
        // =========================================================
        public enum RunMode
        {
            ExportCsvOnly = 1,
            WriteDbOnly = 4,
            ExportAndWriteDb = 6
        }

        public sealed class MultiOutputRequest
        {
            public string BeginDtString { get; set; } = "";
            public string EndDtString { get; set; } = "";
            public bool RemoveOddHour { get; set; } = false;
            public string Token { get; set; } = "";

            // Force fetch by plant text (ex "MS") -> plant_id=2
            public string ForcePlantText { get; set; } = ""; // "" = no force

            // mapping sources
            public Dictionary<string, string> ExternalJsonByKey { get; } = new(StringComparer.Ordinal);
            public Dictionary<string, string> JsonFileByKey { get; } = new(StringComparer.Ordinal);

            // outputs
            public Dictionary<string, string> CsvFileByKey { get; } = new(StringComparer.Ordinal);
            public string CsvFileNamePattern { get; set; } = "output_{key}.csv";
            public RunMode Mode { get; set; } = RunMode.ExportCsvOnly;

            public bool Overwrite2400To0000 { get; set; } = true;
            public bool DeleteAll0000Rows { get; set; } = false;
            public bool Delete2400Rows { get; set; } = true;

            // Optional: meta mapping (configID -> meta info)
            public Dictionary<string, MetaInfo>? MetaLookup { get; set; } = null;

            // Optional: keep export consistent with an extra DateTime column
            public bool IncludeDateTimeColumn { get; set; } = true;

            // === DB write options (per key) ===
            public Dictionary<string, bool> DbWriteByKey { get; } = new(StringComparer.Ordinal);
            public Dictionary<string, string> DbPathByKey { get; } = new(StringComparer.Ordinal);    // default "data.db"
            public Dictionary<string, string> DbPrefixByKey { get; } = new(StringComparer.Ordinal); // "" = narrow_v2, "FWS" = FWS_v2

            // legacy fallback (แทน DataGridViewRow)
            // key -> list of (configID, plant, station)
            public Dictionary<string, List<LegacyMapItem>> LegacyMapByKey { get; } = new(StringComparer.Ordinal);
        }

        public readonly struct LegacyMapItem
        {
            public LegacyMapItem(string configID, string plant, string station)
            {
                ConfigID = configID;
                Plant = plant;
                Station = station;
            }
            public string ConfigID { get; }
            public string Plant { get; }
            public string Station { get; }
        }

        public struct MetaInfo
        {
            public string Plant;
            public string Station;
            public string ParamName;
            public string EquipName;
            public string Unit;
        }

        // =========================================================
        // AquadatFastQuery (CLI)
        // =========================================================
        public sealed class AquadatFastQuery
        {
            private const string INI_SECTION = "LOGIN";
            private const string INI_KEY_USERNAME = "USERNAME";
            private const string INI_KEY_PASSWORD = "PASSWORD";

            // CLI: base dir
            private static readonly string BaseDir = AppContext.BaseDirectory;

            private readonly string iniFilePath = System.IO.Path.Combine(BaseDir, "media", "init.ini");
            private readonly IniFile iniFile;

            private const string URL_ENROLL = "http://aquadat.mwa.co.th:12007/api/aquaDATService/Enroll";
            private const string URL_DATA_BASE = "http://aquadat.mwa.co.th:12007/api/aquaDATService/Data";

            private static readonly HttpClient Http = new HttpClient(new HttpClientHandler
            {
                AutomaticDecompression = System.Net.DecompressionMethods.GZip | System.Net.DecompressionMethods.Deflate
            })
            {
                Timeout = TimeSpan.FromSeconds(60)
            };

            public AquadatFastQuery()
            {
                iniFile = new IniFile(iniFilePath);
            }

            // =========================================================
            // Entry: Multi (CLI)
            // =========================================================
            public async Task ProcessMultiAsync(MultiOutputRequest req, CancellationToken ct = default)
            {
                if (req is null) throw new ArgumentNullException(nameof(req));
                if (string.IsNullOrWhiteSpace(req.BeginDtString) || string.IsNullOrWhiteSpace(req.EndDtString))
                    throw new InvalidOperationException("BeginDtString/EndDtString is required.");

                // =========================================================
                // TIME FIX:
                // fetch ย้อน 1 วันเพื่อใช้ baseline/24:00 สำหรับ CSV/pivot
                // แต่ตอนเขียน DB จะกรองกลับให้เหลือเฉพาะ originalBegin/originalEnd
                // =========================================================
                var originalBegin = DateTime.Parse(req.BeginDtString, CultureInfo.InvariantCulture).Date;
                var originalEnd = DateTime.Parse(req.EndDtString, CultureInfo.InvariantCulture).Date;

                var fetchBegin = originalBegin;
                var fetchEnd = originalEnd;
                var fetchBeginStr = fetchBegin.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                var fetchEndStr = fetchEnd.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                var originalBeginStr = originalBegin.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                var originalEndStr = originalEnd.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

                // keys = union(Csv, Db, mapping)
                var keys = new HashSet<string>(StringComparer.Ordinal);
                foreach (var k in req.CsvFileByKey.Keys) keys.Add(k);
                foreach (var k in req.DbWriteByKey.Keys) keys.Add(k);
                foreach (var k in req.ExternalJsonByKey.Keys) keys.Add(k);
                foreach (var k in req.JsonFileByKey.Keys) keys.Add(k);
                foreach (var k in req.LegacyMapByKey.Keys) keys.Add(k);

                if (keys.Count == 0)
                    throw new InvalidOperationException("No key specified. Provide at least one key in CsvFileByKey/DbWriteByKey/mapping sources.");

                // mode validation
                if (req.Mode == RunMode.ExportCsvOnly && req.CsvFileByKey.Count == 0)
                    throw new InvalidOperationException("Mode=ExportCsvOnly requires CsvFileByKey.");
                if (req.Mode == RunMode.WriteDbOnly && req.DbWriteByKey.Count == 0)
                    throw new InvalidOperationException("Mode=WriteDbOnly requires DbWriteByKey.");
                if (req.Mode == RunMode.ExportAndWriteDb && (req.CsvFileByKey.Count == 0 || req.DbWriteByKey.Count == 0))
                    throw new InvalidOperationException("Mode=ExportAndWriteDb requires CsvFileByKey and DbWriteByKey.");

                // meta from request OR from aqtable.db
                var metaLookupToUse = req.MetaLookup;
                if (metaLookupToUse is null || metaLookupToUse.Count == 0)
                    metaLookupToUse = LoadMetaLookupFromAqtableDb();

                // 1) Build per-key maps + union
                var perKeyCfgMap = new Dictionary<string, Dictionary<string, (string Plant, string Station)>>(StringComparer.Ordinal);
                var unionCfgMap = new Dictionary<string, (string Plant, string Station)>(StringComparer.Ordinal);

                foreach (var key in keys)
                {
                    var cfgMap = BuildConfigPlantMapForKey(req, key);
                    if (cfgMap.Count == 0)
                        throw new InvalidOperationException($"No config mapping found for key='{key}'.");

                    // ForcePlantText filter
                    if (!string.IsNullOrWhiteSpace(req.ForcePlantText))
                    {
                        var forced = new Dictionary<string, (string Plant, string Station)>(StringComparer.Ordinal);
                        foreach (var kvp in cfgMap)
                        {
                            if (string.Equals(kvp.Value.Plant, req.ForcePlantText, StringComparison.OrdinalIgnoreCase))
                                forced[kvp.Key] = kvp.Value;
                        }
                        cfgMap = forced;
                    }

                    if (cfgMap.Count == 0)
                        throw new InvalidOperationException($"Key='{key}' has no mapping after ForcePlantText='{req.ForcePlantText}'.");

                    perKeyCfgMap[key] = cfgMap;

                    foreach (var kvp in cfgMap)
                        if (!unionCfgMap.ContainsKey(kvp.Key)) unionCfgMap.Add(kvp.Key, kvp.Value);
                }

                var neededCfgSet = new HashSet<string>(unionCfgMap.Keys, StringComparer.Ordinal);

                // 2) Group fetch by Plant (union)
                var grouped = unionCfgMap
                    .GroupBy(kvp => kvp.Value.Plant)
                    .Select(g => new
                    {
                        PlantText = g.Key,
                        PlantId = GetPlantCode(g.Key),
                        StationTypes = g.Select(x => MapStationToStationType(x.Value.Station))
                                       .Where(s => !string.IsNullOrWhiteSpace(s))
                                       .Distinct()
                                       .ToList()
                    })
                    .ToList();

                // 3) Ensure token
                var token = req.Token;
                if (string.IsNullOrWhiteSpace(token))
                {
                    token = await LoginToAPI(ct).ConfigureAwait(false);
                    if (string.IsNullOrWhiteSpace(token))
                        throw new InvalidOperationException("LoginToAPI failed. Token is empty.");
                }
                req.Token = token;

                // 4) Fetch union rows
                var allRows = new List<Dictionary<string, object?>>();

                foreach (var pg in grouped)
                {
                    ct.ThrowIfCancellationRequested();

                    if (string.IsNullOrWhiteSpace(pg.PlantId))
                        throw new InvalidOperationException($"Unknown plant '{pg.PlantText}' (GetPlantCode returned empty).");

                    var stationTypeCode = string.Join(",", pg.StationTypes);
                    if (string.IsNullOrWhiteSpace(stationTypeCode))
                        throw new InvalidOperationException($"No stationtype_code for plant '{pg.PlantText}'.");

                    // ใช้ช่วง fetchBegin..fetchEnd เพื่อ baseline
                    var rawJson = await FetchAPIData(token, pg.PlantId, stationTypeCode, fetchBeginStr, fetchEndStr, "json", ct)
                        .ConfigureAwait(false);

                    if (rawJson.StartsWith("{\"status\":", StringComparison.Ordinal))
                        throw new InvalidOperationException("API error: " + rawJson);

                    var fetched = ParseRowsFromApiResponse(rawJson);
                    foreach (var d in fetched)
                        allRows.Add(d);
                }

                // 4.5) DB dictionary (if needed)
                ConcurrentDictionary<string, List<object>>? finalDataDictionary = null;
                if (req.Mode == RunMode.WriteDbOnly || req.Mode == RunMode.ExportAndWriteDb)
                    finalDataDictionary = BuildFinalDataDictionaryFromAllRows(allRows);

                // 5) Master pivot (union columns)
                var master = BuildPivotTable(fetchBeginStr, fetchEndStr, neededCfgSet, req.RemoveOddHour);

                var rowLookup = new Dictionary<string, int>(StringComparer.Ordinal);
                for (int i = 0; i < master.Rows.Count; i++)
                {
                    var k = Convert.ToString(master.Rows[i]["Date"]) + "|" + Convert.ToString(master.Rows[i]["Time"]);
                    rowLookup[k] = i;
                }

                foreach (var r in allRows)
                {
                    var cfg = SafeToString(r, "configparam_id");
                    if (string.IsNullOrWhiteSpace(cfg) || !neededCfgSet.Contains(cfg)) continue;

                    var rawDate = SafeToString(r, "trans_dtm");
                    if (string.IsNullOrWhiteSpace(rawDate)) continue;
                    var dMatch = Regex.Match(rawDate.Trim(), "(?<d>\\d{4}-\\d{2}-\\d{2})");
                    if (!dMatch.Success) continue;
                    var dStr = dMatch.Groups["d"].Value;

                    var rawTime = SafeToString(r, "trans_time");
                    if (string.IsNullOrWhiteSpace(rawTime)) continue;
                    var tStr = NormalizeTimeHHmm(rawTime);
                    if (string.IsNullOrWhiteSpace(tStr)) continue;

                    if (req.RemoveOddHour && tStr != "24:00")
                    {
                        if (int.TryParse(tStr.Substring(0, 2), out var hh) && (hh % 2) == 1)
                            continue;
                    }

                    var vStr = SafeToString(r, "trans_value");
                    var kk = dStr + "|" + tStr;

                    if (rowLookup.TryGetValue(kk, out var rowIndex))
                        master.Rows[rowIndex][cfg] = vStr;
                }

                // Normalize missing -> 0 for config columns
                foreach (DataRow row in master.Rows)
                {
                    foreach (DataColumn col in master.Columns)
                    {
                        if (col.ColumnName is "Date" or "Time") continue;
                        var v = row[col];
                        if (v is null || v == DBNull.Value) row[col] = 0;
                        else if (v is string s && string.IsNullOrWhiteSpace(s)) row[col] = 0;
                    }
                }

                MoveBaseline2400ToNextDay0000(
                    master,
                    overwrite: req.Overwrite2400To0000,
                    deleteAll0000Rows: req.DeleteAll0000Rows,
                    delete2400Rows: req.Delete2400Rows
                );

                // trim กลับมาเหลือช่วงจริง
                TrimDateRangeKeepEndPlus0000(master, originalBeginStr, originalEndStr);

                // 6) Fan-out per key (Export/DB)
                foreach (var key in keys)
                {
                    var cfgMap = perKeyCfgMap[key];
                    var cfgList = cfgMap.Keys.Distinct().ToList();

                    var subTable = BuildSubTable(master, cfgList);

                    var finalTable = subTable;
                    if (req.IncludeDateTimeColumn)
                        finalTable = AddDateTimeColumn(finalTable);

                    if (metaLookupToUse is not null && metaLookupToUse.Count > 0)
                        finalTable = AddMetaRowsToTable(finalTable, metaLookupToUse);

                    // Export CSV
                    if (req.Mode == RunMode.ExportCsvOnly || req.Mode == RunMode.ExportAndWriteDb)
                    {
                        var outPath = ResolveCsvPath(req, key);
                        if (!string.IsNullOrWhiteSpace(outPath))
                            ExportDataTableToCsv(finalTable, outPath);
                    }

                    // DB write
                    if (req.Mode == RunMode.WriteDbOnly || req.Mode == RunMode.ExportAndWriteDb)
                    {
                        var doDb = req.DbWriteByKey.TryGetValue(key, out var b) && b;
                        if (doDb)
                        {
                            if (finalDataDictionary is null)
                                throw new InvalidOperationException("finalDataDictionary is required for DB mode but is null.");

                            var dbp = req.DbPathByKey.TryGetValue(key, out var dbx) ? dbx : "data.db";
                            var prefix = req.DbPrefixByKey.TryGetValue(key, out var px) ? px : "";

                            // ✅ สำคัญ: เขียนลง DB เฉพาะ "วันปัจจุบัน" เท่านั้น
                            var latestFullDay = originalEnd.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                            var dbRange = FilterDataDictionaryForLogicalDay(finalDataDictionary, latestFullDay);

                            // ✅ ล้างตารางก่อนเขียนทุกครั้ง
                            if (string.IsNullOrEmpty(prefix))
                                await AQtable.ClearAllNarrowV2Async(dbp, ct).ConfigureAwait(false);
                            else
                                await AQtable.ClearAllFwsV2Async(dbp, ct).ConfigureAwait(false);

                            _ = await OnPostApiWriteToDbAsync(
                                dbRange,
                                cfgMap,
                                req.RemoveOddHour,
                                dbp,
                                prefix
                            ).ConfigureAwait(false);
                        }
                    }
                }
            }

            // ✅ TIME FIX helper: remove rows outside [begin..end] by Date string (yyyy-MM-dd)
            private static void TrimDateRangeInPlace(DataTable dt, string beginYmd, string endYmd)
            {
                if (dt is null || dt.Rows.Count == 0) return;
                if (!dt.Columns.Contains("Date")) return;

                var delIdx = new List<int>();
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    var d = (Convert.ToString(dt.Rows[i]["Date"]) ?? "").Trim();
                    if (d.Length == 0) { delIdx.Add(i); continue; }

                    if (string.CompareOrdinal(d, beginYmd) < 0 || string.CompareOrdinal(d, endYmd) > 0)
                        delIdx.Add(i);
                }

                for (int k = delIdx.Count - 1; k >= 0; k--)
                {
                    var idx = delIdx[k];
                    if (idx >= 0 && idx < dt.Rows.Count) dt.Rows.RemoveAt(idx);
                }
            }

            private static void TrimDateRangeKeepEndPlus0000(DataTable dt, string beginYmd, string endYmd)
            {
                if (dt is null || dt.Rows.Count == 0) return;
                if (!dt.Columns.Contains("Date") || !dt.Columns.Contains("Time")) return;

                // end+1 day string
                var endDt = DateTime.ParseExact(endYmd, "yyyy-MM-dd", CultureInfo.InvariantCulture);
                var endPlus = endDt.AddDays(1).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

                var delIdx = new List<int>();

                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    var d = (Convert.ToString(dt.Rows[i]["Date"]) ?? "").Trim();
                    var t = (Convert.ToString(dt.Rows[i]["Time"]) ?? "").Trim();

                    if (d.Length == 0) { delIdx.Add(i); continue; }

                    // ✅ keep normal range [begin..end]
                    if (string.CompareOrdinal(d, beginYmd) >= 0 && string.CompareOrdinal(d, endYmd) <= 0)
                        continue;

                    // ✅ keep ONLY end+1 at 00:00 (this is where 24:00 is pasted to)
                    if (d == endPlus && t == "00:00")
                        continue;

                    // otherwise delete
                    delIdx.Add(i);
                }

                for (int k = delIdx.Count - 1; k >= 0; k--)
                {
                    var idx = delIdx[k];
                    if (idx >= 0 && idx < dt.Rows.Count) dt.Rows.RemoveAt(idx);
                }
            }

            // =========================================================
            // Mapping per key (System.Text.Json)
            // Expect JSON array: [{ "configID":"8402", "plant":"MS", "station":"TPS" }, ...]
            // =========================================================
            private Dictionary<string, (string Plant, string Station)> BuildConfigPlantMapForKey(
                MultiOutputRequest req,
                string key)
            {
                var map = new Dictionary<string, (string Plant, string Station)>(StringComparer.Ordinal);

                // 1) external json
                if (req.ExternalJsonByKey.TryGetValue(key, out var jsonText) && !string.IsNullOrWhiteSpace(jsonText))
                    return ParseMapJson(jsonText);

                // 2) json file (BaseDir/Config_/file)
                if (req.JsonFileByKey.TryGetValue(key, out var fn) && !string.IsNullOrWhiteSpace(fn))
                {
                    var fullPath = System.IO.Path.Combine(BaseDir, "Config_", fn);
                    if (File.Exists(fullPath))
                    {
                        var txt = File.ReadAllText(fullPath, Encoding.UTF8);
                        if (!string.IsNullOrWhiteSpace(txt))
                            return ParseMapJson(txt);
                    }
                }

                // 3) legacy list
                if (req.LegacyMapByKey.TryGetValue(key, out var legacy) && legacy is not null)
                {
                    foreach (var it in legacy)
                    {
                        if (string.IsNullOrWhiteSpace(it.ConfigID) ||
                            string.IsNullOrWhiteSpace(it.Plant) ||
                            string.IsNullOrWhiteSpace(it.Station))
                            continue;

                        if (!map.ContainsKey(it.ConfigID))
                            map.Add(it.ConfigID, (it.Plant, it.Station));
                    }
                }

                return map;

                Dictionary<string, (string Plant, string Station)> ParseMapJson(string json)
                {
                    var res = new Dictionary<string, (string Plant, string Station)>(StringComparer.Ordinal);

                    using var doc = JsonDocument.Parse(json);
                    if (doc.RootElement.ValueKind != JsonValueKind.Array) return res;

                    foreach (var el in doc.RootElement.EnumerateArray())
                    {
                        if (el.ValueKind != JsonValueKind.Object) continue;

                        var cfg = el.TryGetProperty("configID", out var pCfg) ? pCfg.GetString() : null;
                        var plant = el.TryGetProperty("plant", out var pPlant) ? pPlant.GetString() : null;
                        var station = el.TryGetProperty("station", out var pStation) ? pStation.GetString() : null;

                        if (string.IsNullOrWhiteSpace(cfg) || string.IsNullOrWhiteSpace(plant) || string.IsNullOrWhiteSpace(station))
                            continue;

                        if (!res.ContainsKey(cfg!))
                            res.Add(cfg!, (plant!, station!));
                    }

                    return res;
                }
            }

            // =========================================================
            // Meta from SQLite (BaseDir/media/aqtable.db)
            // =========================================================
            private Dictionary<string, MetaInfo> LoadMetaLookupFromAqtableDb()
            {
                var meta = new Dictionary<string, MetaInfo>(StringComparer.Ordinal);

                var dbPath = System.IO.Path.Combine(BaseDir, "config_", "aqtable.db");
                if (!File.Exists(dbPath)) return meta;

                using var conn = new SqliteConnection($"Data Source={dbPath};");
                conn.Open();

                var sql =
                    "SELECT configparam_id, plant_en, station_name, station_code, Param_name, equipment_name, measure_th " +
                    "FROM aqtable";

                using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;

                using var rdr = cmd.ExecuteReader();
                while (rdr.Read())
                {
                    var cfg = (rdr["configparam_id"]?.ToString() ?? "").Trim();
                    if (string.IsNullOrWhiteSpace(cfg)) continue;
                    if (meta.ContainsKey(cfg)) continue;

                    var plant = rdr["plant_en"]?.ToString() ?? "";
                    var stationName = rdr["station_name"]?.ToString() ?? "";
                    var stationCode = rdr["station_code"]?.ToString() ?? "";
                    var paramName = rdr["Param_name"]?.ToString() ?? "";
                    var equipName = rdr["equipment_name"]?.ToString() ?? "";
                    var unit = rdr["measure_th"]?.ToString() ?? "";

                    var stationFull = stationName;
                    if (!string.IsNullOrWhiteSpace(stationCode))
                        stationFull = stationName + " (" + stationCode + ")";

                    meta[cfg] = new MetaInfo
                    {
                        Plant = plant,
                        Station = stationFull,
                        ParamName = paramName,
                        EquipName = equipName,
                        Unit = unit
                    };
                }

                return meta;
            }

            // =========================================================
            // HTTP: Login + Fetch (System.Text.Json)
            // =========================================================
            public async Task<string> LoginToAPI(CancellationToken ct = default)
            {
                var username = iniFile.Read(INI_SECTION, INI_KEY_USERNAME, "00102616");
                var password = iniFile.Read(INI_SECTION, INI_KEY_PASSWORD, "88888888");

                var token = await TryLogin(username, password, ct).ConfigureAwait(false);
                if (!string.IsNullOrWhiteSpace(token)) return token;

                for (int i = 1; i <= 9; i++)
                {
                    var brute = new string(i.ToString()[0], 8);
                    token = await TryLogin(username, brute, ct).ConfigureAwait(false);
                    if (!string.IsNullOrWhiteSpace(token)) return token;
                }

                return "";
            }

            private static async Task<string> TryLogin(string username, string password, CancellationToken ct)
            {
                var payload = new Dictionary<string, string>
                {
                    ["username"] = username,
                    ["password"] = password
                };

                var json = JsonSerializer.Serialize(payload);
                using var content = new StringContent(json, Encoding.UTF8, "application/json");

                try
                {
                    using var resp = await Http.PostAsync(URL_ENROLL, content, ct).ConfigureAwait(false);
                    var txt = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);

                    using var doc = JsonDocument.Parse(txt);
                    var root = doc.RootElement;

                    if (root.ValueKind != JsonValueKind.Object) return "";

                    if (root.TryGetProperty("status", out var st) && st.ValueKind == JsonValueKind.Number && st.GetInt32() == 200)
                    {
                        if (root.TryGetProperty("results", out var results) && results.ValueKind == JsonValueKind.Object)
                        {
                            if (results.TryGetProperty("token", out var tok) && tok.ValueKind == JsonValueKind.String)
                                return tok.GetString() ?? "";
                        }
                    }
                }
                catch
                {
                    // keep behavior
                }

                return "";
            }

            private static async Task<string> FetchAPIData(
                string token,
                string plant_id,
                string stationTypeCode,
                string beginDt,
                string endDt,
                string format,
                CancellationToken ct)
            {
                if (string.IsNullOrWhiteSpace(plant_id)) throw new InvalidOperationException("plant_id is empty.");
                if (string.IsNullOrWhiteSpace(stationTypeCode)) throw new InvalidOperationException("stationtype_code is empty.");

                var qs = new List<string>
                {
                    "token=" + Uri.EscapeDataString(token),
                    "plant_id=" + Uri.EscapeDataString(plant_id),
                    "stationtype_code=" + Uri.EscapeDataString(stationTypeCode),
                    "begin_dt=" + Uri.EscapeDataString(beginDt),
                    "end_dt=" + Uri.EscapeDataString(endDt),
                    "format=" + Uri.EscapeDataString(format)
                };

                var url = URL_DATA_BASE + "?" + string.Join("&", qs);

                using var resp = await Http.GetAsync(url, ct).ConfigureAwait(false);
                var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);

                if (!resp.IsSuccessStatusCode)
                    throw new InvalidOperationException($"FetchAPIData failed: {(int)resp.StatusCode} {resp.ReasonPhrase}\nURL={url}\nBODY={body}");

                return body;
            }

            // Parse API response -> List<Dictionary<string, object?>>
            private static List<Dictionary<string, object?>> ParseRowsFromApiResponse(string responseData)
            {
                using var doc = JsonDocument.Parse(responseData);
                var root = doc.RootElement;

                if (root.ValueKind == JsonValueKind.Array)
                    return root.EnumerateArray().Select(ToDict).ToList();

                if (root.ValueKind == JsonValueKind.Object)
                {
                    // error-style block {status, results:{...}}
                    if (root.TryGetProperty("status", out _) &&
                        root.TryGetProperty("results", out var resultsObj) &&
                        resultsObj.ValueKind == JsonValueKind.Object)
                        return new List<Dictionary<string, object?>>();

                    if (root.TryGetProperty("results", out var resultsArr) && resultsArr.ValueKind == JsonValueKind.Array)
                        return resultsArr.EnumerateArray().Select(ToDict).ToList();

                    if (root.TryGetProperty("data", out var dataArr) && dataArr.ValueKind == JsonValueKind.Array)
                        return dataArr.EnumerateArray().Select(ToDict).ToList();

                    return new List<Dictionary<string, object?>>();
                }

                return new List<Dictionary<string, object?>>();

                static Dictionary<string, object?> ToDict(JsonElement el)
                {
                    var d = new Dictionary<string, object?>(StringComparer.Ordinal);
                    if (el.ValueKind != JsonValueKind.Object) return d;

                    foreach (var p in el.EnumerateObject())
                    {
                        d[p.Name] = p.Value.ValueKind switch
                        {
                            JsonValueKind.String => p.Value.GetString(),
                            JsonValueKind.Number => p.Value.TryGetInt64(out var i64) ? i64 :
                                                    p.Value.TryGetDouble(out var dbl) ? dbl : (object?)p.Value.ToString(),
                            JsonValueKind.True => true,
                            JsonValueKind.False => false,
                            JsonValueKind.Null => null,
                            _ => p.Value.ToString()
                        };
                    }
                    return d;
                }
            }

            // =========================================================
            // Pivot / CSV / Helpers (เหมือนเดิม แต่ BaseDir)
            // =========================================================
            private static string SafeToString(Dictionary<string, object?> row, string key)
            {
                if (row is null || !row.TryGetValue(key, out var v) || v is null) return "";
                return v.ToString() ?? "";
            }

            private static string NormalizeTimeHHmm(string t)
            {
                if (string.IsNullOrWhiteSpace(t)) return "";
                var s = t.Trim();
                var m = Regex.Match(s, "^(?<h>\\d{1,2}):(?<m>\\d{2})$");
                if (!m.Success) return "";

                if (!int.TryParse(m.Groups["h"].Value, out var hh)) return "";
                if (!int.TryParse(m.Groups["m"].Value, out var mm)) return "";

                if (hh == 24 && mm == 0) return "24:00";
                if (hh < 0 || hh > 23) return "";
                if (mm < 0 || mm > 59) return "";
                return hh.ToString("00", CultureInfo.InvariantCulture) + ":" + mm.ToString("00", CultureInfo.InvariantCulture);
            }

            private static DataTable BuildPivotTable(string beginDtString, string endDtString, HashSet<string> neededCfgSet, bool removeOddHour)
            {
                var beginDt = DateTime.Parse(beginDtString, CultureInfo.InvariantCulture).Date;
                var endDt = DateTime.Parse(endDtString, CultureInfo.InvariantCulture).Date;

                var dt = new DataTable("AquaPivotMaster");
                dt.Columns.Add("Date", typeof(string));
                dt.Columns.Add("Time", typeof(string));
                foreach (var cfg in neededCfgSet) dt.Columns.Add(cfg, typeof(string));

                dt.BeginLoadData();
                try
                {
                    var cur = beginDt;
                    while (cur <= endDt)
                    {
                        for (int h = 0; h <= 24; h++)
                        {
                            if (removeOddHour && (h % 2) == 1) continue;

                            var r = dt.NewRow();
                            r["Date"] = cur.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                            r["Time"] = (h == 24) ? "24:00" : h.ToString("00", CultureInfo.InvariantCulture) + ":00";
                            dt.Rows.Add(r);
                        }
                        cur = cur.AddDays(1);
                    }
                }
                finally { dt.EndLoadData(); }

                return dt;
            }

            private static DataTable BuildSubTable(DataTable master, List<string> cfgList)
            {
                var dt = new DataTable("AquaSub");
                dt.Columns.Add("Date", typeof(string));
                dt.Columns.Add("Time", typeof(string));
                foreach (var cfg in cfgList)
                    if (!dt.Columns.Contains(cfg)) dt.Columns.Add(cfg, typeof(string));

                dt.BeginLoadData();
                try
                {
                    foreach (DataRow r in master.Rows)
                    {
                        var nr = dt.NewRow();
                        nr["Date"] = Convert.ToString(r["Date"]);
                        nr["Time"] = Convert.ToString(r["Time"]);
                        foreach (var cfg in cfgList)
                            nr[cfg] = (master.Columns.Contains(cfg) && !r.IsNull(cfg)) ? Convert.ToString(r[cfg]) : "";
                        dt.Rows.Add(nr);
                    }
                }
                finally { dt.EndLoadData(); }

                return dt;
            }

            private static DataTable AddDateTimeColumn(DataTable dt)
            {
                if (dt is null) return dt;
                if (!dt.Columns.Contains("Date") || !dt.Columns.Contains("Time")) return dt;
                if (dt.Columns.Contains("DateTime")) return dt;

                var col = new DataColumn("DateTime", typeof(string));
                dt.Columns.Add(col);
                col.SetOrdinal(2);

                foreach (DataRow r in dt.Rows)
                {
                    var d = (Convert.ToString(r["Date"]) ?? "").Trim();
                    var t = (Convert.ToString(r["Time"]) ?? "").Trim();

                    if (DateTime.TryParse(d, CultureInfo.InvariantCulture, DateTimeStyles.None, out var dd) &&
                        Regex.IsMatch(t, "^\\d{2}:\\d{2}$"))
                        r["DateTime"] = dd.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture) + " " + t;
                    else
                        r["DateTime"] = "";
                }

                return dt;
            }

            private static DataTable AddMetaRowsToTable(DataTable dt, Dictionary<string, MetaInfo> metaLookup)
            {
                if (dt is null || dt.Columns.Count < 2) return dt;
                if (metaLookup is null || metaLookup.Count == 0) return dt;

                var r1 = dt.NewRow();
                var r2 = dt.NewRow();
                var r3 = dt.NewRow();
                for (int c = 0; c < dt.Columns.Count; c++) { r1[c] = ""; r2[c] = ""; r3[c] = ""; }

                r1[0] = "Plant - Station";
                r2[0] = "Param - Equipment";
                r3[0] = "Unit";

                var cfgStart = dt.Columns.Contains("DateTime") ? 3 : 2;

                for (int colIndex = cfgStart; colIndex < dt.Columns.Count; colIndex++)
                {
                    var cfgId = dt.Columns[colIndex].ColumnName;
                    if (metaLookup.TryGetValue(cfgId, out var meta))
                    {
                        r1[colIndex] = $"{meta.Plant} - {meta.Station}";
                        r2[colIndex] = $"{meta.ParamName} - {meta.EquipName}";
                        r3[colIndex] = meta.Unit;
                    }
                }

                dt.Rows.InsertAt(r3, 0);
                dt.Rows.InsertAt(r2, 0);
                dt.Rows.InsertAt(r1, 0);
                return dt;
            }

            private static void ExportDataTableToCsv(DataTable dt, string outputPath)
            {
                var dir = System.IO.Path.GetDirectoryName(outputPath);
                if (!string.IsNullOrWhiteSpace(dir) && !Directory.Exists(dir))
                    Directory.CreateDirectory(dir);

                using var sw = new StreamWriter(outputPath, false, new UTF8Encoding(true));

                var headers = dt.Columns.Cast<DataColumn>().Select(c => EscapeCsv(c.ColumnName));
                sw.WriteLine(string.Join(",", headers));

                foreach (DataRow row in dt.Rows)
                {
                    var fields = dt.Columns.Cast<DataColumn>()
                        .Select(c => EscapeCsv(row.IsNull(c) ? "" : (Convert.ToString(row[c]) ?? "")));
                    sw.WriteLine(string.Join(",", fields));
                }
            }

            private static string EscapeCsv(string s)
            {
                s ??= "";
                var needsQuote = s.Contains(',') || s.Contains('\"') || s.Contains('\r') || s.Contains('\n');
                if (s.Contains('\"')) s = s.Replace("\"", "\"\"");
                if (needsQuote) s = "\"" + s + "\"";
                return s;
            }

            private static string ResolveCsvPath(MultiOutputRequest req, string key)
            {
                if (req.CsvFileByKey.TryGetValue(key, out var p) && !string.IsNullOrWhiteSpace(p))
                    return MakeAbsolutePath(p);

                var pattern = req.CsvFileNamePattern ?? "";
                if (string.IsNullOrWhiteSpace(pattern)) return "";

                var fileName = pattern.Replace("{key}", key);
                if (!fileName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)) fileName += ".csv";
                return MakeAbsolutePath(fileName);
            }

            private static string MakeAbsolutePath(string pathOrFile)
            {
                if (string.IsNullOrWhiteSpace(pathOrFile)) return "";
                if (System.IO.Path.IsPathRooted(pathOrFile)) return pathOrFile;

                var dirPart = System.IO.Path.GetDirectoryName(pathOrFile);
                if (!string.IsNullOrWhiteSpace(dirPart))
                    return System.IO.Path.Combine(BaseDir, pathOrFile);

                return System.IO.Path.Combine(BaseDir, "media", pathOrFile);
            }

            private static ConcurrentDictionary<string, List<object>> BuildFinalDataDictionaryFromAllRows(List<Dictionary<string, object?>> allRows)
            {
                var dict = new ConcurrentDictionary<string, List<object>>(StringComparer.Ordinal);
                if (allRows is null || allRows.Count == 0) return dict;

                var temp = new Dictionary<string, List<object>>(StringComparer.Ordinal);

                foreach (var r in allRows)
                {
                    if (r is null) continue;

                    var plantId = (r.TryGetValue("plant_id", out var pidObj) && pidObj is not null)
                        ? (pidObj.ToString() ?? "").Trim()
                        : "";

                    if (plantId == "") plantId = "UNKNOWN";

                    var jo = (object)r;

                    if (!temp.TryGetValue(plantId, out var bucket))
                    {
                        bucket = new List<object>();
                        temp[plantId] = bucket;
                    }
                    bucket.Add(jo);
                }

                foreach (var kv in temp) dict.TryAdd(kv.Key, kv.Value);
                return dict;
            }

            private static ConcurrentDictionary<string, List<object>> FilterDataDictionaryForLogicalDay(
                ConcurrentDictionary<string, List<object>> dataDictionary,
                string targetDayYmd)
            {
                var filtered = new ConcurrentDictionary<string, List<object>>(StringComparer.Ordinal);
                var target = DateTime.ParseExact(targetDayYmd, "yyyy-MM-dd", CultureInfo.InvariantCulture);
                var startTs = target;              // 2026-03-24 00:00
                var endTs = target.AddDays(1);     // 2026-03-25 00:00

                foreach (var kv in dataDictionary)
                {
                    var kept = new List<object>();

                    foreach (var anyObj in kv.Value)
                    {
                        if (anyObj is not Dictionary<string, object?> row)
                            continue;

                        var d = row.TryGetValue("trans_dtm", out var dObj) ? (dObj?.ToString() ?? "").Trim() : "";
                        var t = row.TryGetValue("trans_time", out var tObj) ? (tObj?.ToString() ?? "").Trim() : "";

                        if (string.IsNullOrEmpty(d) || string.IsNullOrEmpty(t))
                            continue;

                        var ts = ParseLogicalTs(d, t);

                        if (ts >= startTs && ts <= endTs)
                            kept.Add(row);
                    }

                    if (kept.Count > 0)
                        filtered.TryAdd(kv.Key, kept);
                }

                return filtered;
            }

            private static DateTime ParseLogicalTs(string ymd, string hhmm)
            {
                var d = DateTime.ParseExact(ymd, "yyyy-MM-dd", CultureInfo.InvariantCulture);

                if (hhmm == "24:00")
                    return d.AddDays(1); // next day 00:00

                var t = TimeSpan.ParseExact(hhmm, @"hh\:mm", CultureInfo.InvariantCulture);
                return d.Add(t);
            }

            // =========================================================
            // TIME POLICY (เหมือนเดิม)
            // =========================================================
            private static void MoveBaseline2400ToNextDay0000(DataTable dt, bool overwrite = true, bool deleteAll0000Rows = false, bool delete2400Rows = false)
            {
                if (dt is null || dt.Rows.Count == 0) return;

                bool RowHasData(DataRow r)
                {
                    foreach (DataColumn col in dt.Columns)
                    {
                        if (col.ColumnName is "Date" or "Time") continue;
                        var s = r.IsNull(col) ? "" : (Convert.ToString(r[col]) ?? "");
                        if (!string.IsNullOrWhiteSpace(s)) return true;
                    }
                    return false;
                }

                var idx24 = new List<int>();
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    var r = dt.Rows[i];
                    if (Convert.ToString(r["Time"]) == "24:00" && RowHasData(r)) idx24.Add(i);
                }

                foreach (var i in idx24)
                {
                    var src = dt.Rows[i];

                    if (!DateTime.TryParse(Convert.ToString(src["Date"]), CultureInfo.InvariantCulture, DateTimeStyles.None, out var dd))
                        continue;

                    var nextDate = dd.AddDays(1).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

                    var targetIndex = -1;
                    for (int j = 0; j < dt.Rows.Count; j++)
                    {
                        var rj = dt.Rows[j];
                        if (Convert.ToString(rj["Date"]) == nextDate && Convert.ToString(rj["Time"]) == "00:00")
                        {
                            targetIndex = j;
                            break;
                        }
                    }

                    if (targetIndex == -1)
                    {
                        var nr = dt.NewRow();
                        nr["Date"] = nextDate;
                        nr["Time"] = "00:00";
                        dt.Rows.Add(nr);
                        targetIndex = dt.Rows.Count - 1;
                    }

                    var target = dt.Rows[targetIndex];

                    foreach (DataColumn col in dt.Columns)
                    {
                        if (col.ColumnName is "Date" or "Time") continue;
                        var s = src.IsNull(col) ? "" : (Convert.ToString(src[col]) ?? "");
                        if (string.IsNullOrWhiteSpace(s)) continue;

                        if (overwrite) target[col] = s;
                        else
                        {
                            var t = target.IsNull(col) ? "" : (Convert.ToString(target[col]) ?? "");
                            if (string.IsNullOrWhiteSpace(t)) target[col] = s;
                        }
                    }
                }

                var delIdx = new List<int>();
                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    var r = dt.Rows[i];
                    if (Convert.ToString(r["Time"]) != "00:00") continue;
                    if (deleteAll0000Rows || !RowHasData(r)) delIdx.Add(i);
                }

                if (delete2400Rows)
                {
                    for (int i = 0; i < dt.Rows.Count; i++)
                        if (Convert.ToString(dt.Rows[i]["Time"]) == "24:00") delIdx.Add(i);
                }

                foreach (var i in delIdx.Distinct().OrderByDescending(x => x))
                    if (i >= 0 && i < dt.Rows.Count) dt.Rows.RemoveAt(i);
            }

            // =========================================================
            // Plant/station helpers (เหมือนเดิม)
            // =========================================================
            private static string GetPlantCode(string plantId) => (plantId ?? "").ToUpperInvariant() switch
            {
                "BK" => "1",
                "MS" => "2",
                "SS1" => "3",
                "SS2" => "4",
                "SS3" => "5",
                "SS4" => "6",
                "TB" => "7",
                "WQ" => "8",
                "SS" => "12",
                _ => ""
            };

            private static string MapStationToStationType(string stationText)
            {
                if (string.IsNullOrWhiteSpace(stationText)) return "";
                var s = stationText.Trim().ToUpperInvariant();

                var m = Regex.Match(s, "\\((?<code>[A-Z0-9_]+)\\)");
                if (m.Success)
                {
                    var code = m.Groups["code"].Value;
                    if (IsValidStationType(code)) return code;
                }

                if (IsValidStationType(s)) return s;

                foreach (var code in new[] { "RWS", "CDS", "CWS", "FWS", "TPS", "DPS", "QWS", "CTS" })
                    if (s.Contains(code)) return code;

                return "";
            }

            private static bool IsValidStationType(string code) => code switch
            {
                "RWS" or "CDS" or "CWS" or "FWS" or "TPS" or "DPS" or "QWS" or "CTS" => true,
                _ => false
            };

            // =========================================================
            // DB write (เรียกของคุณเอง)
            // =========================================================
            private static async Task<int> OnPostApiWriteToDbAsync(
                ConcurrentDictionary<string, List<object>> finalDataDictionary,
                Dictionary<string, (string Plant, string Station)> finalConfigPlantMap,
                bool removeOddHour,
                string dbPath,
                string prefix)
            {
                // คุณมีของจริงอยู่แล้ว -> แค่คง signature ไว้
                var selectedIds = GetSelectedConfigIdSet(finalConfigPlantMap);
                var filteredDict = FilterDataDictionaryBySelectedConfigIds(finalDataDictionary, selectedIds);

                if (string.IsNullOrEmpty(prefix))
                    return await AQtable.UpsertAquaDatAsync(filteredDict, dbPath, removeOddHour).ConfigureAwait(false);

                return await AQtable.UpsertAquaDatAsync_FWS(filteredDict, dbPath, removeOddHour).ConfigureAwait(false);
            }

            private static HashSet<int> GetSelectedConfigIdSet(Dictionary<string, (string Plant, string Station)> finalConfigPlantMap)
            {
                var setIds = new HashSet<int>();
                foreach (var k in finalConfigPlantMap.Keys)
                    if (int.TryParse((k ?? "").Trim(), out var id)) setIds.Add(id);
                return setIds;
            }

            private static ConcurrentDictionary<string, List<object>> FilterDataDictionaryBySelectedConfigIds(
                ConcurrentDictionary<string, List<object>> dataDictionary,
                HashSet<int> selectedIds)
            {
                var filtered = new ConcurrentDictionary<string, List<object>>();
                if (dataDictionary is null || selectedIds is null || selectedIds.Count == 0) return filtered;

                foreach (var kv in dataDictionary)
                {
                    var kept = new List<object>();

                    foreach (var anyObj in kv.Value)
                    {
                        // ใน CLI เราเก็บ anyObj เป็น Dictionary<string, object?> (จาก BuildFinalDataDictionaryFromAllRows)
                        if (anyObj is not Dictionary<string, object?> row) continue;

                        var idStr = row.TryGetValue("configparam_id", out var idObj) ? (idObj?.ToString() ?? "").Trim() : "";
                        if (!int.TryParse(idStr, out var id)) continue;

                        if (selectedIds.Contains(id))
                            kept.Add(row);
                    }

                    if (kept.Count > 0) filtered.TryAdd(kv.Key, kept);
                }

                return filtered;
            }
        }


        public static class AQtable
        {
            // ==============================
            // State & Init
            // ==============================
            private static string _connStr = "";
            private static readonly object _initLock = new();
            private static bool _initialized = false;
            private static string _dbPath = "data.db";

            /// <summary>เรียกครั้งเดียวก่อนใช้งาน (เช่นตอนโปรแกรมเริ่ม)</summary>
            public static void Initialize(string dbPath = "data.db", bool rebuildViews = false)
            {
                lock (_initLock)
                {
                    if (_initialized) return;

                    _dbPath = string.IsNullOrWhiteSpace(dbPath) ? "data.db" : dbPath;

                    EnsureWal(_dbPath);

                    _connStr = $"Data Source={_dbPath};Cache=Shared;Pooling=True;";

                    using var conn = new SqliteConnection(_connStr);
                    conn.Open();

                    // ================================
                    // 1) TABLE + INDEX: AQ_readings_narrow_v2
                    // ================================
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS AQ_readings_narrow_v2(
    ts          TEXT    NOT NULL,                         -- ISO8601 'yyyy-MM-dd HH:mm:ss'
    trans_type  TEXT    NOT NULL DEFAULT '',              -- เช่น 'RWS' 'FWS' 'TPS' 'DPS'
    plant_id    INTEGER NOT NULL,                         -- จาก API
    configID    INTEGER NOT NULL,                         -- จาก configparam_id
    station     TEXT    NOT NULL DEFAULT '',              -- ถ้าไม่มีใน API ให้ ''
    header      TEXT    NOT NULL DEFAULT '',              -- ชื่อคีย์ (สำรอง/เพื่ออ่านง่าย)
    value       REAL,                                     -- ค่าตัวเลข (ถ้ามี)
    value_text  TEXT,                                     -- ค่าข้อความ เช่น 'ON', 'STANDBY'
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now', '+7 hours')),
    CONSTRAINT uq_time_cfg_plant_type UNIQUE (ts, configID, plant_id, trans_type)
);";
                        cmd.ExecuteNonQuery();
                    }

                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = @"
CREATE INDEX IF NOT EXISTS idx_v2_ts                ON AQ_readings_narrow_v2(ts);
CREATE INDEX IF NOT EXISTS idx_v2_cfg_ts            ON AQ_readings_narrow_v2(configID, ts);
CREATE INDEX IF NOT EXISTS idx_v2_type_plant_cfg_ts ON AQ_readings_narrow_v2(trans_type, plant_id, configID, ts);
CREATE INDEX IF NOT EXISTS idx_v2_plant_type_ts     ON AQ_readings_narrow_v2(plant_id, trans_type, ts);
CREATE INDEX IF NOT EXISTS idx_v2_updated           ON AQ_readings_narrow_v2(updated_at);";
                        cmd.ExecuteNonQuery();
                    }
                    if (rebuildViews)
                        EnsureLatestView(conn);

                    // ================================
                    // 2) TABLE + INDEX: AQ_readings_FWS_v2
                    // ================================
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS AQ_readings_FWS_v2(
    ts          TEXT    NOT NULL,                         -- ISO8601 'yyyy-MM-dd HH:mm:ss'
    trans_type  TEXT    NOT NULL DEFAULT '',              -- เช่น 'FWS' / 'QWS'
    plant_id    INTEGER NOT NULL,                         -- จาก API
    configID    INTEGER NOT NULL,                         -- จาก configparam_id
    station     TEXT    NOT NULL DEFAULT '',              -- ถ้าไม่มีใน API ให้ ''
    header      TEXT    NOT NULL DEFAULT '',              -- อ่านง่าย
    value       REAL,                                     -- ค่าตัวเลข (nullable)
    value_text  TEXT,                                     -- ค่าข้อความ เช่น 'ON', 'STANDBY'
    updated_at  TEXT    NOT NULL DEFAULT (datetime('now','+7 hours')),
    CONSTRAINT uq_time_cfg_plant_type UNIQUE (ts, configID, plant_id, trans_type)
);";
                        cmd.ExecuteNonQuery();
                    }

                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = @"
CREATE INDEX IF NOT EXISTS idx_fws_ts                ON AQ_readings_FWS_v2(ts);
CREATE INDEX IF NOT EXISTS idx_fws_cfg_ts            ON AQ_readings_FWS_v2(configID, ts);
CREATE INDEX IF NOT EXISTS idx_fws_type_plant_cfg_ts ON AQ_readings_FWS_v2(trans_type, plant_id, configID, ts);
CREATE INDEX IF NOT EXISTS idx_fws_plant_type_ts     ON AQ_readings_FWS_v2(plant_id, trans_type, ts);
CREATE INDEX IF NOT EXISTS idx_fws_updated           ON AQ_readings_FWS_v2(updated_at);";
                        cmd.ExecuteNonQuery();
                    }

                    _initialized = true;
                }
            }

            private static void EnsureInit()
            {
                if (_initialized) return;
                Initialize(_dbPath);
            }

            private static SqliteConnection NewConn()
            {
                EnsureInit();
                return new SqliteConnection(_connStr);
            }

            // ==============================
            // Helpers (time/parse)
            // ==============================

            /// <summary>(date:"yyyy-MM-dd", time:"HH:mm" | "24:00") → "yyyy-MM-dd HH:mm:ss"</summary>
            private static string ToIsoTs(string? trans_dtm, string? trans_time)
            {
                if (string.IsNullOrWhiteSpace(trans_dtm)) return "";
                var d = trans_dtm.Trim();
                var tt = (trans_time ?? "").Trim();

                if (tt.Equals("24:00", StringComparison.OrdinalIgnoreCase))
                {
                    if (DateTime.TryParseExact(d, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dd))
                    {
                        return dd.AddDays(1).ToString("yyyy-MM-dd 00:00:00", CultureInfo.InvariantCulture);
                    }
                }

                if (tt.Length == 5) tt += ":00"; // HH:mm -> HH:mm:00

                var merged = (d + " " + tt).Trim();

                if (DateTime.TryParseExact(merged, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                    return dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

                if (DateTime.TryParse(merged, CultureInfo.InvariantCulture, DateTimeStyles.None, out dt))
                    return dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

                return "";
            }

            private static int ToInt(string? s)
            {
                if (string.IsNullOrWhiteSpace(s)) return 0;
                return int.TryParse(s.Trim(), out var n) ? n : 0;
            }

            private static double? ToDoubleFlexible(string? s)
            {
                if (string.IsNullOrWhiteSpace(s)) return null;

                if (double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var v)) return v;
                if (double.TryParse(s, NumberStyles.Float, CultureInfo.CurrentCulture, out v)) return v;
                return null;
            }

            // ---- Unified getter (รองรับ Dictionary / JsonElement / string JSON) ----
            private static string GetStr(object? obj, params string[] keys)
            {
                if (obj is null) return "";

                // 1) Dictionary<string, object?>
                if (obj is Dictionary<string, object?> dict)
                {
                    foreach (var k in keys)
                    {
                        if (!dict.TryGetValue(k, out var v) || v is null) continue;
                        var s = v.ToString()?.Trim() ?? "";
                        if (s.Length > 0) return s;
                    }
                    return "";
                }

                // 2) JsonElement
                if (obj is JsonElement je)
                    return GetStrFromJsonElement(je, keys);

                // 3) string JSON
                if (obj is string jsonText)
                {
                    jsonText = jsonText.Trim();
                    if (jsonText.Length == 0) return "";

                    try
                    {
                        using var doc = JsonDocument.Parse(jsonText);
                        return GetStrFromJsonElement(doc.RootElement, keys);
                    }
                    catch
                    {
                        return jsonText;
                    }
                }

                // 4) อื่นๆ
                try
                {
                    var bytes = JsonSerializer.SerializeToUtf8Bytes(obj);
                    using var doc = JsonDocument.Parse(bytes);
                    return GetStrFromJsonElement(doc.RootElement, keys);
                }
                catch
                {
                    return "";
                }
            }

            private static string GetStrFromJsonElement(JsonElement root, params string[] keys)
            {
                if (root.ValueKind != JsonValueKind.Object) return "";

                foreach (var k in keys)
                {
                    if (!root.TryGetProperty(k, out var p)) continue;

                    string s = p.ValueKind switch
                    {
                        JsonValueKind.String => p.GetString() ?? "",
                        JsonValueKind.Number => p.ToString(),
                        JsonValueKind.True => "true",
                        JsonValueKind.False => "false",
                        JsonValueKind.Null => "",
                        _ => p.ToString()
                    };

                    s = s.Trim();
                    if (s.Length > 0) return s;
                }

                return "";
            }

            // ==============================
            // Writes (UPSERT)
            // ==============================

            public static async Task<int> UpsertAquaDatAsync(
                ConcurrentDictionary<string, List<object>> dataDictionary,
                string dbPath = "data.db",
                bool removeOddHour = false,
                CancellationToken ct = default)
            {
                Initialize(dbPath);

                int total = 0;

                await using var conn = NewConn();
                await conn.OpenAsync(ct).ConfigureAwait(false);

                await using var tx = conn.BeginTransaction();

                const string sql = @"
INSERT INTO AQ_readings_narrow_v2
(ts, trans_type, plant_id, configID, station, header, value, value_text, updated_at)
VALUES ($ts, $type, $plant, $cfg, $station, $hdr, $val, $valtxt, datetime('now', '+7 hours'))
ON CONFLICT(ts, configID, plant_id, trans_type)
DO UPDATE SET
  value      = COALESCE(excluded.value, value),
  value_text = COALESCE(excluded.value_text, value_text),
  updated_at = excluded.updated_at;";

                await using var cmd = conn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = sql;

                var pTs = cmd.CreateParameter(); pTs.ParameterName = "$ts"; cmd.Parameters.Add(pTs);
                var pType = cmd.CreateParameter(); pType.ParameterName = "$type"; cmd.Parameters.Add(pType);
                var pPlant = cmd.CreateParameter(); pPlant.ParameterName = "$plant"; cmd.Parameters.Add(pPlant);
                var pCfg = cmd.CreateParameter(); pCfg.ParameterName = "$cfg"; cmd.Parameters.Add(pCfg);
                var pStation = cmd.CreateParameter(); pStation.ParameterName = "$station"; cmd.Parameters.Add(pStation);
                var pHdr = cmd.CreateParameter(); pHdr.ParameterName = "$hdr"; cmd.Parameters.Add(pHdr);
                var pVal = cmd.CreateParameter(); pVal.ParameterName = "$val"; cmd.Parameters.Add(pVal);
                var pValTxt = cmd.CreateParameter(); pValTxt.ParameterName = "$valtxt"; cmd.Parameters.Add(pValTxt);

                foreach (var kv in dataDictionary)
                {
                    ct.ThrowIfCancellationRequested();

                    var items = kv.Value;
                    if (items is null) continue;

                    foreach (var anyObj in items)
                    {
                        ct.ThrowIfCancellationRequested();

                        var trans_type = GetStr(anyObj, "trans_type");
                        var cfgId = ToInt(GetStr(anyObj, "configparam_id"));
                        var plantId = ToInt(GetStr(anyObj, "plant_id"));
                        var d = GetStr(anyObj, "trans_dtm");
                        var t = GetStr(anyObj, "trans_time");
                        var ts = ToIsoTs(d, t);
                        if (ts.Length == 0) continue;

                        if (removeOddHour)
                        {
                            if (DateTime.TryParseExact(ts, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                            {
                                if (dt.Minute != 0 || (dt.Hour % 2) != 0) continue;
                            }
                        }

                        var rawVal = GetStr(anyObj, "trans_value");
                        var num = ToDoubleFlexible(rawVal);

                        var station = GetStr(anyObj, "station");
                        if (station.Length == 0) station = "";
                        var header = trans_type;

                        pTs.Value = ts;
                        pType.Value = trans_type ?? "";
                        pPlant.Value = plantId;
                        pCfg.Value = cfgId;
                        pStation.Value = station;
                        pHdr.Value = header;

                        if (num.HasValue)
                        {
                            pVal.Value = num.Value;
                            pValTxt.Value = DBNull.Value;
                        }
                        else
                        {
                            pVal.Value = DBNull.Value;
                            pValTxt.Value = rawVal ?? "";
                        }

                        _ = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                        total++;
                    }
                }

                await tx.CommitAsync(ct).ConfigureAwait(false);
                return total;
            }

            public static async Task<int> UpsertAquaDatAsync_FWS(
                ConcurrentDictionary<string, List<object>> dataDictionary,
                string dbPath = "data.db",
                bool removeOddHour = false,
                CancellationToken ct = default)
            {
                Initialize(dbPath);

                int total = 0;

                await using var conn = NewConn();
                await conn.OpenAsync(ct).ConfigureAwait(false);

                await using var tx = conn.BeginTransaction();

                const string sql = @"
INSERT INTO AQ_readings_FWS_v2
(ts, trans_type, plant_id, configID, station, header, value, value_text, updated_at)
VALUES ($ts, $type, $plant, $cfg, $station, $hdr, $val, $valtxt, datetime('now', '+7 hours'))
ON CONFLICT(ts, configID, plant_id, trans_type)
DO UPDATE SET
  value      = COALESCE(excluded.value, value),
  value_text = COALESCE(excluded.value_text, value_text),
  updated_at = excluded.updated_at;";

                await using var cmd = conn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = sql;

                var pTs = cmd.CreateParameter(); pTs.ParameterName = "$ts"; cmd.Parameters.Add(pTs);
                var pType = cmd.CreateParameter(); pType.ParameterName = "$type"; cmd.Parameters.Add(pType);
                var pPlant = cmd.CreateParameter(); pPlant.ParameterName = "$plant"; cmd.Parameters.Add(pPlant);
                var pCfg = cmd.CreateParameter(); pCfg.ParameterName = "$cfg"; cmd.Parameters.Add(pCfg);
                var pStation = cmd.CreateParameter(); pStation.ParameterName = "$station"; cmd.Parameters.Add(pStation);
                var pHdr = cmd.CreateParameter(); pHdr.ParameterName = "$hdr"; cmd.Parameters.Add(pHdr);
                var pVal = cmd.CreateParameter(); pVal.ParameterName = "$val"; cmd.Parameters.Add(pVal);
                var pValTxt = cmd.CreateParameter(); pValTxt.ParameterName = "$valtxt"; cmd.Parameters.Add(pValTxt);

                foreach (var kv in dataDictionary)
                {
                    ct.ThrowIfCancellationRequested();

                    var items = kv.Value;
                    if (items is null) continue;

                    foreach (var anyObj in items)
                    {
                        ct.ThrowIfCancellationRequested();

                        var trans_type = GetStr(anyObj, "trans_type");
                        var cfgId = ToInt(GetStr(anyObj, "configparam_id"));
                        var plantId = ToInt(GetStr(anyObj, "plant_id"));

                        var d = GetStr(anyObj, "trans_dtm");
                        var t = GetStr(anyObj, "trans_time");
                        var ts = ToIsoTs(d, t);
                        if (ts.Length == 0) continue;

                        if (removeOddHour)
                        {
                            if (DateTime.TryParseExact(ts, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                            {
                                if (dt.Minute != 0 || (dt.Hour % 2) != 0) continue;
                            }
                        }

                        var rawVal = GetStr(anyObj, "trans_value");
                        var num = ToDoubleFlexible(rawVal);

                        var station = GetStr(anyObj, "station");
                        var header = trans_type;

                        pTs.Value = ts;
                        pType.Value = trans_type ?? "";
                        pPlant.Value = plantId;
                        pCfg.Value = cfgId;
                        pStation.Value = station ?? "";
                        pHdr.Value = header ?? "";

                        if (num.HasValue)
                        {
                            pVal.Value = num.Value;
                            pValTxt.Value = DBNull.Value;
                        }
                        else
                        {
                            pVal.Value = DBNull.Value;
                            pValTxt.Value = rawVal ?? "";
                        }

                        _ = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                        total++;
                    }
                }

                await tx.CommitAsync(ct).ConfigureAwait(false);
                return total;
            }

            // ==============================
            // WAL Utilities
            // ==============================
            public static void EnsureWal(string dbPath)
            {
                if (string.IsNullOrWhiteSpace(dbPath)) dbPath = "data.db";

                if (!File.Exists(dbPath))
                {
                    var dir = Path.GetDirectoryName(dbPath);
                    if (!string.IsNullOrEmpty(dir) && !Directory.Exists(dir))
                        Directory.CreateDirectory(dir);

                    using (var c = new SqliteConnection($"Data Source={dbPath};Cache=Shared;"))
                    {
                        c.Open();
                    }
                }

                using var conn = new SqliteConnection($"Data Source={dbPath};Cache=Shared;Pooling=True;");
                conn.Open();

                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "PRAGMA journal_mode=WAL;";
                    _ = cmd.ExecuteScalar();
                }
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = @"
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=3000;
PRAGMA wal_autocheckpoint=1000;";
                    cmd.ExecuteNonQuery();
                }
            }

            // ==============================
            // Maintenance
            // ==============================
            public static void ClearAllNarrowV2(string dbPath = "data.db")
            {
                using var conn = new SqliteConnection($"Data Source={dbPath};Cache=Shared;Pooling=True;");
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "DELETE FROM AQ_readings_narrow_v2;";
                cmd.ExecuteNonQuery();
            }

            public static void VacuumDb(string dbPath = "data.db")
            {
                using var conn = new SqliteConnection($"Data Source={dbPath};Cache=Shared;Pooling=True;");
                conn.Open();
                using var cmd = conn.CreateCommand();
                cmd.CommandText = "VACUUM;";
                cmd.ExecuteNonQuery();
            }

            public static async Task<int> ClearAllNarrowV2Async(string dbPath = "data.db", CancellationToken ct = default)
            {
                await using var conn = new SqliteConnection($"Data Source={dbPath};Cache=Shared;Pooling=True;");
                await conn.OpenAsync(ct).ConfigureAwait(false);

                await using var tx = conn.BeginTransaction();
                await using var cmd = conn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "DELETE FROM AQ_readings_narrow_v2;";

                var affected = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                await tx.CommitAsync(ct).ConfigureAwait(false);
                return affected;
            }

            public static async Task<int> ClearAllFwsV2Async(string dbPath = "data.db", CancellationToken ct = default)
            {
                await using var conn = new SqliteConnection($"Data Source={dbPath};Cache=Shared;Pooling=True;");
                await conn.OpenAsync(ct).ConfigureAwait(false);

                await using var tx = conn.BeginTransaction();
                await using var cmd = conn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText = "DELETE FROM AQ_readings_FWS_v2;";

                var affected = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                await tx.CommitAsync(ct).ConfigureAwait(false);
                return affected;
            }

            // ==============================
            // Latest View (aligned for Thailand) + raw view
            // ==============================
            public static void EnsureLatestView(string dbPath = "data.db")
            {
                using var conn = new SqliteConnection($"Data Source={dbPath};Cache=Shared;Pooling=True;");
                conn.Open();
                EnsureLatestView(conn);
            }

            private static void EnsureLatestView(SqliteConnection conn)
            {
                using (var drop1 = conn.CreateCommand())
                {
                    drop1.CommandText = "DROP VIEW IF EXISTS v_aq_latest;";
                    drop1.ExecuteNonQuery();
                }
                using (var drop2 = conn.CreateCommand())
                {
                    drop2.CommandText = "DROP VIEW IF EXISTS v_aq_latest_raw;";
                    drop2.ExecuteNonQuery();
                }

                using (var createRaw = conn.CreateCommand())
                {
                    createRaw.CommandText = @"
CREATE VIEW v_aq_latest_raw AS
SELECT t.*
FROM AQ_readings_narrow_v2 AS t
JOIN (
  SELECT trans_type, plant_id, configID, MAX(ts) AS max_ts
  FROM AQ_readings_narrow_v2
  WHERE value IS NOT NULL OR value_text IS NOT NULL
  GROUP BY trans_type, plant_id, configID
) g
ON  t.trans_type = g.trans_type
AND t.plant_id   = g.plant_id
AND t.configID   = g.configID
AND t.ts         = g.max_ts
WHERE t.value IS NOT NULL OR t.value_text IS NOT NULL;";
                    createRaw.ExecuteNonQuery();
                }

                var alignedSql = @"
CREATE VIEW v_aq_latest AS
WITH nowz AS (
  SELECT
    datetime('now','localtime') AS now_local,
    date('now','localtime')     AS d,
    CAST(strftime('%H','now','localtime') AS INTEGER) AS h
),
bucket AS (
  SELECT
    datetime(strftime('%Y-%m-%d %H:00:00', now_local)) AS hour_cut,
    datetime(d || ' ' || printf('%02d:00:00',
             CASE WHEN (h % 2) = 0 THEN h ELSE h-1 END)) AS even_cut
  FROM nowz
),
t AS (
  SELECT *
  FROM AQ_readings_narrow_v2
  WHERE value IS NOT NULL OR value_text IS NOT NULL
),
g_hour AS (
  SELECT x.trans_type, x.plant_id, x.configID, MAX(x.ts) AS max_ts
  FROM t x, bucket b
  WHERE x.trans_type IN ('TPS','DPS','FWS') AND x.ts <= b.hour_cut
  GROUP BY x.trans_type, x.plant_id, x.configID
),
g_even AS (
  SELECT x.trans_type, x.plant_id, x.configID, MAX(x.ts) AS max_ts
  FROM t x, bucket b
  WHERE x.trans_type IN ('RWS','CDS') AND x.ts <= b.even_cut
  GROUP BY x.trans_type, x.plant_id, x.configID
),
pick AS (
  SELECT * FROM g_hour
  UNION ALL
  SELECT * FROM g_even
)
SELECT t.*
FROM t
JOIN pick p
  ON t.trans_type = p.trans_type
 AND t.plant_id   = p.plant_id
 And t.configID   = p.configID
 AND t.ts         = p.max_ts;";

                using (var createAligned = conn.CreateCommand())
                {
                    createAligned.CommandText = alignedSql;
                    createAligned.ExecuteNonQuery();
                }
            }
        }
    }

    public sealed class AqFastApiImpl : AqApiModule.IAqFastApi
    {
        private readonly AquadatFastCli.AquadatFastQuery _aq;

        public AqFastApiImpl(AquadatFastCli.AquadatFastQuery aq)
        {
            _aq = aq ?? throw new ArgumentNullException(nameof(aq));
        }

        public Task<bool> VerifyAsync(AqApiModule.VerifyRequest req, CancellationToken ct)
        {
            return Task.FromResult(!string.IsNullOrWhiteSpace(req.Token));
        }

        public async Task<AqApiModule.ProcessResult> ProcessAsync(AqApiModule.ProcessRequest req, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(req.Begin) || string.IsNullOrWhiteSpace(req.End))
                throw new InvalidOperationException("begin/end is required");
            if (string.IsNullOrWhiteSpace(req.ExternalConfigsJson))
                throw new InvalidOperationException("configs is required");

            var enrichedConfigsJson = EnrichConfigsWithMeta(req.ExternalConfigsJson);

            var baseDir = AppContext.BaseDirectory;
            var mediaDir = Path.Combine(baseDir, "media");
            Directory.CreateDirectory(mediaDir);

            var safeName = string.IsNullOrWhiteSpace(req.CsvFileName) ? "output.csv" : req.CsvFileName.Trim();
            if (!safeName.EndsWith(".csv", StringComparison.OrdinalIgnoreCase)) safeName += ".csv";

            var useTemp = !safeName.Equals("output.csv", StringComparison.OrdinalIgnoreCase);
            var actualName = useTemp ? $"{Path.GetFileNameWithoutExtension(safeName)}_{DateTime.Now:yyyyMMdd_HHmmss_fff}.csv" : safeName;
            var outPath = Path.Combine(mediaDir, actualName);

            var mreq = new AquadatFastCli.MultiOutputRequest
            {
                BeginDtString = req.Begin,
                EndDtString = req.End,
                RemoveOddHour = req.RemoveOddHour,
                Token = req.Token ?? "",
                ForcePlantText = "",
                Mode = AquadatFastCli.RunMode.ExportCsvOnly,
                IncludeDateTimeColumn = true
            };

            const string key = "MAIN";
            mreq.ExternalJsonByKey[key] = enrichedConfigsJson;
            mreq.CsvFileByKey[key] = outPath;

            await _aq.ProcessMultiAsync(mreq, ct).ConfigureAwait(false);

            if (!File.Exists(outPath))
                throw new InvalidOperationException("csv not produced: " + outPath);

            var bytes = await File.ReadAllBytesAsync(outPath, ct).ConfigureAwait(false);

            if (useTemp)
            {
                try { File.Delete(outPath); } catch { }
            }

            return new AqApiModule.ProcessResult(
                FileName: safeName,
                Bytes: bytes,
                FilePath: useTemp ? null : outPath,
                DeleteAfterSend: false,
                ContentType: "text/csv; charset=utf-8"
            );
        }

        private static string EnrichConfigsWithMeta(string rawConfigsJson)
        {
            using var doc = JsonDocument.Parse(rawConfigsJson);
            if (doc.RootElement.ValueKind != JsonValueKind.Array)
                return rawConfigsJson;

            var list = new List<Dictionary<string, string>>();

            foreach (var el in doc.RootElement.EnumerateArray())
            {
                if (el.ValueKind != JsonValueKind.Object) continue;

                var cfg = el.TryGetProperty("configID", out var pCfg) ? (pCfg.GetString() ?? "") : "";
                var plant = el.TryGetProperty("plant", out var pPlant) ? (pPlant.GetString() ?? "") : "";
                var station = el.TryGetProperty("station", out var pStation) ? (pStation.GetString() ?? "") : "";

                var item = new Dictionary<string, string>(StringComparer.Ordinal)
                {
                    ["configID"] = cfg
                };

                if (!string.IsNullOrWhiteSpace(plant)) item["plant"] = plant;
                if (!string.IsNullOrWhiteSpace(station)) item["station"] = station;

                list.Add(item);
            }

            return JsonSerializer.Serialize(list, new JsonSerializerOptions(JsonSerializerDefaults.Web));
        }
    }
}
