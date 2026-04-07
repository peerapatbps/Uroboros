#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace Uroboros
{
    // =========================================================
    // Lookup handlers (SQLite: chem.db)
    // =========================================================
    public static class LookupHandlers
    {
        // =========================
        // Public HTTP Handlers
        // =========================
        public static async Task HandleLookupProductsAsync(HttpListenerContext hc, CancellationToken ct)
        {
            var resp = hc.Response;
            try
            {
                var baseDir = AppContext.BaseDirectory;
                var iniPath = Path.Combine(baseDir, "config_", "config.ini");

                var dbPath = ResolveChemDbPathFromIni(baseDir, iniPath);
                if (!File.Exists(dbPath))
                    throw new FileNotFoundException("chem.db not found", dbPath);

                // defaults (SQLite schema you created)
                var tableProduct = ReadIni(iniPath, "CHEM", "table_product", "PRODUCT");
                var colName = ReadIni(iniPath, "CHEM", "name_field", "NAME");
                var colCode = ReadIni(iniPath, "CHEM", "code_field", "CODE");

                var wdataProductField = ReadIni(iniPath, "CHEM", "wdata_product_field", "PRODUCT");

                var pairs = SqliteLookup.ReadDistinctCodeNamePreferMaster(
                    dbPath,
                    masterTable: tableProduct,
                    masterCodeColumn: colCode,
                    masterNameColumn: colName,
                    fallbackDataTable: "WDATA",
                    fallbackCodeColumn: wdataProductField
                );

                var items = pairs
                    .Where(x => !string.IsNullOrWhiteSpace(x.code) && !string.IsNullOrWhiteSpace(x.name))
                    .Select(x => new { code = x.code, name = x.name })
                    .ToList();

                await WriteJsonAsync(resp, items, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await WriteErrorAsync(resp, 500, ex.Message, ct).ConfigureAwait(false);
            }
            finally
            {
                SafeClose(resp);
            }
        }

        public static async Task HandleLookupCompaniesAsync(HttpListenerContext hc, CancellationToken ct)
        {
            var resp = hc.Response;
            try
            {
                var baseDir = AppContext.BaseDirectory;
                var iniPath = Path.Combine(baseDir, "config_", "config.ini");

                var dbPath = ResolveChemDbPathFromIni(baseDir, iniPath);
                if (!File.Exists(dbPath))
                    throw new FileNotFoundException("chem.db not found", dbPath);

                // defaults
                var tableCompany = ReadIni(iniPath, "CHEM", "table_company", "COMPANY");
                var colName = ReadIni(iniPath, "CHEM", "name_field", "NAME");
                var colCode = ReadIni(iniPath, "CHEM", "code_field", "CODE");

                var wdataCompanyField = ReadIni(iniPath, "CHEM", "wdata_company_field", "COMPANY");

                var pairs = SqliteLookup.ReadDistinctCodeNamePreferMaster(
                    dbPath,
                    masterTable: tableCompany,
                    masterCodeColumn: colCode,
                    masterNameColumn: colName,
                    fallbackDataTable: "WDATA",
                    fallbackCodeColumn: wdataCompanyField
                );

                var items = pairs
                    .Where(x => !string.IsNullOrWhiteSpace(x.code) && !string.IsNullOrWhiteSpace(x.name))
                    .Select(x => new { code = x.code, name = x.name })
                    .ToList();

                await WriteJsonAsync(resp, items, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await WriteErrorAsync(resp, 500, ex.Message, ct).ConfigureAwait(false);
            }
            finally
            {
                SafeClose(resp);
            }
        }

        // =========================
        // chem.db path from config.ini
        // =========================
        public static string ResolveChemDbPathFromIni(string baseDir, string iniPath)
        {
            // 1) explicit path
            var p = ReadIni(iniPath, "Production", "chem_db_path", "");
            p = Unquote((p ?? "").Trim());
            if (!string.IsNullOrWhiteSpace(p))
            {
                if (!Path.IsPathRooted(p))
                    p = Path.Combine(baseDir, p);

                // ถ้าระบุเป็นโฟลเดอร์ และเป็น media ของโปรแกรม ให้ใช้ active snapshot
                if (Directory.Exists(p))
                {
                    var mediaDir = Path.Combine(baseDir, "media");
                    if (Path.GetFullPath(p).TrimEnd('\\')
                        .Equals(Path.GetFullPath(mediaDir).TrimEnd('\\'), StringComparison.OrdinalIgnoreCase))
                    {
                        return MdbReaderSafe.ResolveActiveChemDbPath(baseDir);
                    }

                    return Path.Combine(p, "chem.db");
                }

                // ถ้าระบุเป็นไฟล์ .db โดยตรง ก็ใช้ไฟล์นั้น
                if (p.EndsWith(".db", StringComparison.OrdinalIgnoreCase))
                    return p;

                return p;
            }

            // 2) legacy: download_mdb_file_location
            var dir = ReadIni(iniPath, "Production", "download_mdb_file_location", "");
            dir = Unquote((dir ?? "").Trim());
            if (!string.IsNullOrWhiteSpace(dir))
            {
                if (!Path.IsPathRooted(dir))
                    dir = Path.Combine(baseDir, dir);

                if (Directory.Exists(dir))
                {
                    var mediaDir = Path.Combine(baseDir, "media");
                    if (Path.GetFullPath(dir).TrimEnd('\\')
                        .Equals(Path.GetFullPath(mediaDir).TrimEnd('\\'), StringComparison.OrdinalIgnoreCase))
                    {
                        return MdbReaderSafe.ResolveActiveChemDbPath(baseDir);
                    }

                    return Path.Combine(dir, "chem.db");
                }

                if (dir.EndsWith(".db", StringComparison.OrdinalIgnoreCase))
                    return dir;
            }

            // 3) default: ใช้ active snapshot เสมอ
            return MdbReaderSafe.ResolveActiveChemDbPath(baseDir);
        }

        private static string Unquote(string s)
        {
            if (s.Length >= 2 && ((s[0] == '"' && s[^1] == '"') || (s[0] == '\'' && s[^1] == '\'')))
                return s.Substring(1, s.Length - 2);
            return s;
        }

        // =========================
        // INI reader (minimal)
        // =========================
        private static string ReadIni(string iniPath, string section, string key, string def)
        {
            if (!File.Exists(iniPath)) return def;

            string curSection = "";
            foreach (var raw in File.ReadAllLines(iniPath, Encoding.UTF8))
            {
                var line = raw.Trim();
                if (line.Length == 0 || line.StartsWith(";") || line.StartsWith("#")) continue;

                if (line.StartsWith("[") && line.EndsWith("]"))
                {
                    curSection = line.Substring(1, line.Length - 2).Trim();
                    continue;
                }

                if (!curSection.Equals(section, StringComparison.OrdinalIgnoreCase)) continue;

                var eq = line.IndexOf('=');
                if (eq <= 0) continue;

                var k = line.Substring(0, eq).Trim();
                if (!k.Equals(key, StringComparison.OrdinalIgnoreCase)) continue;

                return line.Substring(eq + 1).Trim();
            }

            return def;
        }

        // =========================
        // Write helpers
        // =========================
        public static async Task WriteJsonAsync(HttpListenerResponse resp, object obj, CancellationToken ct)
        {
            var json = JsonSerializer.Serialize(obj, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });

            var bytes = Encoding.UTF8.GetBytes(json);
            resp.StatusCode = 200;
            resp.ContentType = "application/json; charset=utf-8";
            resp.ContentEncoding = Encoding.UTF8;
            resp.ContentLength64 = bytes.Length;

            await resp.OutputStream.WriteAsync(bytes, 0, bytes.Length, ct).ConfigureAwait(false);
            await resp.OutputStream.FlushAsync(ct).ConfigureAwait(false);
        }

        public static async Task WriteErrorAsync(HttpListenerResponse resp, int statusCode, string message, CancellationToken ct)
        {
            var json = JsonSerializer.Serialize(new { error = message }, new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            });

            var bytes = Encoding.UTF8.GetBytes(json);
            resp.StatusCode = statusCode;
            resp.ContentType = "application/json; charset=utf-8";
            resp.ContentEncoding = Encoding.UTF8;
            resp.ContentLength64 = bytes.Length;

            await resp.OutputStream.WriteAsync(bytes, 0, bytes.Length, ct).ConfigureAwait(false);
            await resp.OutputStream.FlushAsync(ct).ConfigureAwait(false);
        }

        public static void SafeClose(HttpListenerResponse resp)
        {
            try { resp.OutputStream.Close(); } catch { }
            try { resp.Close(); } catch { }
        }
    }

    // =========================================================
    // SQLite lookup (prefer master table, fallback used codes from WDATA)
    // =========================================================
    public static class SqliteLookup
    {
        public static List<(string code, string name)> ReadDistinctCodeNamePreferMaster(
            string dbPath,
            string masterTable,
            string masterCodeColumn,
            string masterNameColumn,
            string fallbackDataTable,
            string fallbackCodeColumn)
        {
            if (string.IsNullOrWhiteSpace(dbPath)) throw new ArgumentNullException(nameof(dbPath));
            if (!File.Exists(dbPath)) throw new FileNotFoundException("chem.db not found.", dbPath);

            using var conn = SqliteUtil.OpenReadOnly(dbPath);

            // 1) codes used in WDATA (optional filter)
            var usedCodes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            if (SqliteUtil.TableExists(conn, fallbackDataTable) && SqliteUtil.ColumnExists(conn, fallbackDataTable, fallbackCodeColumn))
            {
                foreach (var c in ReadDistinctString(conn, fallbackDataTable, fallbackCodeColumn))
                    if (!string.IsNullOrWhiteSpace(c)) usedCodes.Add(c);
            }

            // 2) Prefer master CODE+NAME
            if (SqliteUtil.TableExists(conn, masterTable) &&
                SqliteUtil.ColumnExists(conn, masterTable, masterCodeColumn) &&
                SqliteUtil.ColumnExists(conn, masterTable, masterNameColumn))
            {
                var all = ReadCodeName(conn, masterTable, masterCodeColumn, masterNameColumn);

                var filtered = (usedCodes.Count > 0)
                    ? all.Where(x => usedCodes.Contains(x.code)).ToList()
                    : all;

                return filtered
                    .Where(x => x.code.Length > 0 && x.name.Length > 0)
                    .GroupBy(x => x.code, StringComparer.OrdinalIgnoreCase)
                    .Select(g => g.First())
                    .OrderBy(x => x.name, StringComparer.OrdinalIgnoreCase)
                    .ToList();
            }

            // 3) fallback: only codes from WDATA => name=code
            return usedCodes
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .Select(s => (code: s.Trim(), name: s.Trim()))
                .OrderBy(x => x.name, StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        private static List<(string code, string name)> ReadCodeName(
            SqliteConnection conn,
            string table,
            string colCode,
            string colName)
        {
            var list = new List<(string code, string name)>();

            var sql = $"SELECT {Q(colCode)} AS c, {Q(colName)} AS n FROM {Q(table)} " +
                      $"WHERE {Q(colCode)} IS NOT NULL AND {Q(colName)} IS NOT NULL";

            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;

            using var rdr = cmd.ExecuteReader();
            while (rdr.Read())
            {
                var c = (rdr["c"]?.ToString() ?? "").Trim();
                var n = (rdr["n"]?.ToString() ?? "").Trim();
                if (c.Length > 0 && n.Length > 0)
                    list.Add((c, n));
            }

            return list;
        }

        private static List<string> ReadDistinctString(
            SqliteConnection conn,
            string table,
            string column)
        {
            var results = new List<string>();

            var sql = $"SELECT DISTINCT {Q(column)} AS v FROM {Q(table)} WHERE {Q(column)} IS NOT NULL";
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;

            using var rdr = cmd.ExecuteReader();
            while (rdr.Read())
            {
                var s = (rdr["v"]?.ToString() ?? "").Trim();
                if (s.Length > 0) results.Add(s);
            }

            return results
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        private static string Q(string ident)
        {
            ident ??= "";
            ident = ident.Trim();
            if (ident.Length == 0) throw new ArgumentException("Empty identifier.");
            return "\"" + ident.Replace("\"", "\"\"") + "\"";
        }
    }

    public static class SqliteUtil
    {
        public static SqliteConnection OpenReadOnly(string dbPath)
        {
            if (!File.Exists(dbPath))
                throw new FileNotFoundException("SQLite DB not found.", dbPath);

            // ReadOnly + disable pooling ลดปัญหา lock/handle ค้าง
            var cs = $"Data Source={dbPath};Mode=ReadOnly;Cache=Shared;Pooling=False;";
            var conn = new SqliteConnection(cs);
            conn.Open();
            return conn;
        }

        public static bool TableExists(SqliteConnection conn, string tableName)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT 1 FROM sqlite_master WHERE type='table' AND name=$n LIMIT 1;";
            cmd.Parameters.AddWithValue("$n", tableName);
            var o = cmd.ExecuteScalar();
            return o != null && o != DBNull.Value;
        }

        public static bool ColumnExists(SqliteConnection conn, string tableName, string columnName)
        {
            try
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"PRAGMA table_info({Q(tableName)});";
                using var rdr = cmd.ExecuteReader();
                while (rdr.Read())
                {
                    var col = (rdr["name"]?.ToString() ?? "").Trim();
                    if (col.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                        return true;
                }
            }
            catch { }
            return false;

            static string Q(string ident) => "\"" + (ident ?? "").Replace("\"", "\"\"") + "\"";
        }
    }

    // =========================================================
    // ResolveCodeFromNameOrCode (SQLite) (used by chem_report)
    // =========================================================
    public static class ChemLookup
    {
        public static string ResolveCodeFromNameOrCode(string dbPath, string tableName, string? nameOrCode)
        {
            var s = (nameOrCode ?? "").Trim();

            if (s.Length == 0 ||
                s.Equals("(All)", StringComparison.OrdinalIgnoreCase) ||
                s.Equals("(ทั้งหมด)", StringComparison.OrdinalIgnoreCase))
                return "";

            // old heuristic
            var looksLikeCode = s.Length <= 6 && s.AsSpan().IndexOfAnyExceptInRange('0', '9') < 0;
            if (looksLikeCode) return s;

            using var conn = SqliteUtil.OpenReadOnly(dbPath);

            // exact
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT CODE FROM {Q(tableName)} WHERE NAME = $n LIMIT 1;";
                cmd.Parameters.AddWithValue("$n", s);

                var o = cmd.ExecuteScalar();
                var code = (o?.ToString() ?? "").Trim();
                if (code.Length > 0) return code;
            }

            // like
            {
                using var cmd = conn.CreateCommand();
                cmd.CommandText = $"SELECT CODE FROM {Q(tableName)} WHERE NAME LIKE $n LIMIT 1;";
                cmd.Parameters.AddWithValue("$n", "%" + s + "%");

                var o = cmd.ExecuteScalar();
                var code = (o?.ToString() ?? "").Trim();
                if (code.Length > 0) return code;
            }

            return "";

            static string Q(string ident) => "\"" + (ident ?? "").Replace("\"", "\"\"") + "\"";
        }
    }

    // =========================================================
    // CHEM report handlers (SQLite: chem.db)
    // =========================================================
    public static class ChemReportHandlers
    {
        // POST /api/chem_report/export  -> stream CSV (UTF-8 BOM)
        public static async Task HandleChemReportExportAsync(HttpListenerContext hc, CancellationToken ct)
        {
            var resp = hc.Response;

            try
            {
                // 0) Read body JSON
                string body;
                using (var sr = new StreamReader(hc.Request.InputStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true))
                    body = await sr.ReadToEndAsync().ConfigureAwait(false);

                if (string.IsNullOrWhiteSpace(body))
                    throw new Exception("Empty request body.");

                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;

                var startDateStr = JsonGetStringSafe(root, "startDate");
                var endDateStr = JsonGetStringSafe(root, "endDate");

                var startDate = ParseYmd(startDateStr, "startDate");
                var endDate = ParseYmd(endDateStr, "endDate");
                if (endDate < startDate) throw new Exception("endDate must be >= startDate");

                // default DAYIN/DAYOUT
                var dateField = JsonGetStringSafe(root, "dateField");
                if (string.IsNullOrWhiteSpace(dateField)) dateField = "DAYIN";
                dateField = dateField.Trim().ToUpperInvariant();
                if (dateField != "DAYIN" && dateField != "DAYOUT") dateField = "DAYIN";

                var rawProduct = NormalizeOptCode(JsonGetStringSafe(root, "productCode"));
                var rawCompany = NormalizeOptCode(JsonGetStringSafe(root, "companyCode"));

                var productText = JsonGetStringSafe(root, "productText");
                var companyText = JsonGetStringSafe(root, "companyText");
                if (string.IsNullOrWhiteSpace(productText)) productText = string.IsNullOrWhiteSpace(rawProduct) ? "(All)" : rawProduct;
                if (string.IsNullOrWhiteSpace(companyText)) companyText = string.IsNullOrWhiteSpace(rawCompany) ? "(All)" : rawCompany;

                // 1) Resolve paths
                var baseDir = AppContext.BaseDirectory;
                var iniPath = Path.Combine(baseDir, "config_", "config.ini");
                var dbPath = LookupHandlers.ResolveChemDbPathFromIni(baseDir, iniPath);
                if (!File.Exists(dbPath))
                    throw new FileNotFoundException("media\\chem.db not found.", dbPath);

                // 1.1) Resolve NAME -> CODE (SQLite)
                var productCode = ChemLookup.ResolveCodeFromNameOrCode(dbPath, "PRODUCT", rawProduct);
                var companyCode = ChemLookup.ResolveCodeFromNameOrCode(dbPath, "COMPANY", rawCompany);

                // 2) Build SQL + params (SQLite)
                // NOTE: dates stored as 'yyyy-MM-dd' TEXT in your converter.
                var sql =
                    "SELECT \n" +
                    $"  w.{dateField} AS d,\n" +
                    "  p.NAME AS ProductName,\n" +
                    "  co.NAME AS CompanyName,\n" +
                    "  w.TRUCK,\n" +
                    "  ct.NAME AS CartypeName,\n" +
                    "  w.W1, w.W2\n" +
                    "FROM WDATA AS w\n" +
                    "LEFT JOIN CARTYPE AS ct ON w.CARTYPE = ct.CODE\n" +
                    "LEFT JOIN COMPANY AS co ON w.COMPANY = co.CODE\n" +
                    "LEFT JOIN PRODUCT AS p ON w.PRODUCT = p.CODE\n" +
                    "WHERE w.STAT = $stat\n" +
                    $"  AND w.{dateField} BETWEEN $d1 AND $d2\n";

                if (!string.IsNullOrWhiteSpace(productCode))
                    sql += "  AND w.PRODUCT = $prod\n";
                if (!string.IsNullOrWhiteSpace(companyCode))
                    sql += "  AND w.COMPANY = $comp\n";

                sql += $"ORDER BY w.{dateField} ASC, w.TMIN ASC;";

                // 3) Prepare response
                var fileName = $"CHEM_receive_{startDate:yyyyMMdd}_{endDate:yyyyMMdd}.csv";

                resp.StatusCode = 200;
                resp.ContentType = "text/csv; charset=utf-8";
                resp.ContentEncoding = Encoding.UTF8;
                resp.Headers["Content-Disposition"] = $"attachment; filename=\"{fileName}\"";

                // 4) Stream CSV (UTF-8 BOM)
                var exportedAt = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                var dateRangeText = $"{startDate:yyyy-MM-dd} to {endDate:yyyy-MM-dd}";

                await using var os = resp.OutputStream;
                await using var sw = new StreamWriter(os, new UTF8Encoding(encoderShouldEmitUTF8Identifier: true), bufferSize: 64 * 1024, leaveOpen: true);

                WriteHeaderLineRef(sw, "Export Type", "Chemical Receive Report");
                WriteHeaderLineRef(sw, "Exported At", exportedAt);
                WriteHeaderLineRef(sw, "Exported By", "CHEM Module (WebListener)");
                WriteHeaderLineRef(sw, "Source Database", dbPath);
                WriteHeaderLineRef(sw, "Date Field", dateField);
                WriteHeaderLineRef(sw, "Date Range", dateRangeText);
                WriteHeaderLineRef(sw, "Product", productText);
                WriteHeaderLineRef(sw, "Company", companyText);
                WriteHeaderLineRef(sw, "Encoding", "UTF-8 with BOM");
                WriteHeaderLineRef(sw, "------------------------------------------------------------", "");

                sw.WriteLine(new string(',', 8));
                sw.WriteLine("No.,Date,Product,Company,Truck,Type,Gross(In),Gross(Out),NetWeight");

                int no = 0;

                using var conn = SqliteUtil.OpenReadOnly(dbPath);
                using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$stat", "2");
                cmd.Parameters.AddWithValue("$d1", startDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                cmd.Parameters.AddWithValue("$d2", endDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                if (!string.IsNullOrWhiteSpace(productCode)) cmd.Parameters.AddWithValue("$prod", productCode);
                if (!string.IsNullOrWhiteSpace(companyCode)) cmd.Parameters.AddWithValue("$comp", companyCode);

                using var rdr = cmd.ExecuteReader();
                while (rdr.Read())
                {
                    ct.ThrowIfCancellationRequested();
                    no++;

                    var dText = (rdr["d"]?.ToString() ?? "").Trim(); // yyyy-MM-dd
                    var dateText = "";
                    if (DateTime.TryParseExact(dText, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                        dateText = dt.ToString("d/M/yyyy", CultureInfo.InvariantCulture);

                    var prodName = (rdr["ProductName"]?.ToString() ?? "").Trim();
                    var compName = (rdr["CompanyName"]?.ToString() ?? "").Trim();
                    var truck = (rdr["TRUCK"]?.ToString() ?? "").Trim();
                    var typ = (rdr["CartypeName"]?.ToString() ?? "").Trim();

                    var w1 = ReadDouble(rdr["W1"]);
                    var w2 = ReadDouble(rdr["W2"]);
                    var net = w1 - w2;

                    var line =
                        CsvCell(no.ToString(CultureInfo.InvariantCulture)) + "," +
                        CsvCell(dateText) + "," +
                        CsvCell(prodName) + "," +
                        CsvCell(compName) + "," +
                        CsvCell(truck) + "," +
                        CsvCell(typ) + "," +
                        CsvCell(Math.Round(w1, 0).ToString("0", CultureInfo.InvariantCulture)) + "," +
                        CsvCell(Math.Round(w2, 0).ToString("0", CultureInfo.InvariantCulture)) + "," +
                        CsvCell(Math.Round(net, 0).ToString("0", CultureInfo.InvariantCulture));

                    sw.WriteLine(line);

                    if ((no % 200) == 0) await sw.FlushAsync().ConfigureAwait(false);
                }

                await sw.FlushAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                try
                {
                    if (!resp.HeadersWritten() && !resp.ContentType.StartsWith("text/csv", StringComparison.OrdinalIgnoreCase))
                        await LookupHandlers.WriteErrorAsync(resp, 400, ex.Message, ct).ConfigureAwait(false);
                }
                catch { }
            }
            finally
            {
                LookupHandlers.SafeClose(resp);
            }
        }

        // helpers
        private static string JsonGetStringSafe(JsonElement root, string name)
        {
            if (root.ValueKind != JsonValueKind.Object) return "";
            if (!root.TryGetProperty(name, out var p)) return "";
            if (p.ValueKind == JsonValueKind.Null) return "";
            return p.ValueKind == JsonValueKind.String ? (p.GetString() ?? "") : p.ToString();
        }

        private static string NormalizeOptCode(string? s)
        {
            var t = (s ?? "").Trim();
            if (t.Equals("(All)", StringComparison.OrdinalIgnoreCase) ||
                t.Equals("(ทั้งหมด)", StringComparison.OrdinalIgnoreCase))
                return "";
            return t;
        }

        private static DateTime ParseYmd(string s, string fieldName)
        {
            s = (s ?? "").Trim();
            if (!DateTime.TryParseExact(s, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                throw new Exception($"{fieldName} must be yyyy-MM-dd");
            return dt.Date;
        }

        private static void WriteHeaderLineRef(StreamWriter sw, string k, string v)
            => sw.WriteLine($"{CsvCell(k)},{CsvCell(v)}");

        private static string CsvCell(string? s)
        {
            s ??= "";
            s = s.Replace("\r\n", " ").Replace("\r", " ").Replace("\n", " ");
            var mustQuote = s.Contains(',') || s.Contains('"') || s.Contains('\n') || s.Contains('\r');
            if (s.Contains('"')) s = s.Replace("\"", "\"\"");
            return mustQuote ? $"\"{s}\"" : s;
        }

        private static double ReadDouble(object? v)
        {
            if (v == null || v == DBNull.Value) return 0d;
            if (v is double d) return d;
            if (v is float f) return f;
            if (v is decimal m) return (double)m;
            if (double.TryParse(v.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var x)) return x;
            if (double.TryParse(v.ToString(), NumberStyles.Any, CultureInfo.CurrentCulture, out var y)) return y;
            return 0d;
        }

        private static bool HeadersWritten(this HttpListenerResponse resp)
        {
            if (resp.ContentLength64 > 0) return true;
            return resp.Headers["Content-Disposition"] != null;
        }
    }

    public static class ChemReportQueryHandlers
    {
        // POST /api/chem_report  -> JSON rows
        public static async Task HandleChemReportQueryAsync(HttpListenerContext hc, CancellationToken ct)
        {
            var resp = hc.Response;

            try
            {
                // 0) Read body JSON
                string body;
                using (var sr = new StreamReader(hc.Request.InputStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true))
                    body = await sr.ReadToEndAsync().ConfigureAwait(false);

                if (string.IsNullOrWhiteSpace(body))
                    throw new Exception("Empty request body.");

                using var doc = JsonDocument.Parse(body);
                var root = doc.RootElement;

                var startDateStr = JsonGetStringSafe(root, "startDate");
                var endDateStr = JsonGetStringSafe(root, "endDate");

                var rawProduct = NormalizeOptCode(JsonGetStringSafe(root, "productCode"));
                var rawCompany = NormalizeOptCode(JsonGetStringSafe(root, "companyCode"));

                var dateField = JsonGetStringSafe(root, "dateField");
                if (string.IsNullOrWhiteSpace(dateField)) dateField = "DAYIN";
                dateField = dateField.Trim().ToUpperInvariant();
                if (dateField != "DAYIN" && dateField != "DAYOUT") dateField = "DAYIN";

                var startDate = ParseYmd(startDateStr, "startDate");
                var endDate = ParseYmd(endDateStr, "endDate");
                if (endDate < startDate) throw new Exception("endDate must be >= startDate");

                // 1) Resolve paths
                var baseDir = AppContext.BaseDirectory;
                var iniPath = Path.Combine(baseDir, "config_", "config.ini");

                var dbPath = LookupHandlers.ResolveChemDbPathFromIni(baseDir, iniPath);
                if (!File.Exists(dbPath))
                    throw new FileNotFoundException("media\\chem.db not found.", dbPath);

                // 1.1) Resolve NAME -> CODE (SQLite)
                var productCode = ChemLookup.ResolveCodeFromNameOrCode(dbPath, "PRODUCT", rawProduct);
                var companyCode = ChemLookup.ResolveCodeFromNameOrCode(dbPath, "COMPANY", rawCompany);

                // 2) Build SQL + params (SQLite)
                var sql =
                    "SELECT \n" +
                    $"  w.{dateField} AS d,\n" +
                    "  p.NAME AS ProductName,\n" +
                    "  co.NAME AS CompanyName,\n" +
                    "  w.TRUCK,\n" +
                    "  ct.NAME AS CartypeName,\n" +
                    "  w.W1, w.W2\n" +
                    "FROM WDATA AS w\n" +
                    "LEFT JOIN CARTYPE AS ct ON w.CARTYPE = ct.CODE\n" +
                    "LEFT JOIN COMPANY AS co ON w.COMPANY = co.CODE\n" +
                    "LEFT JOIN PRODUCT AS p ON w.PRODUCT = p.CODE\n" +
                    "WHERE w.STAT = $stat\n" +
                    $"  AND w.{dateField} BETWEEN $d1 AND $d2\n";

                if (!string.IsNullOrWhiteSpace(productCode))
                    sql += "  AND w.PRODUCT = $prod\n";
                if (!string.IsNullOrWhiteSpace(companyCode))
                    sql += "  AND w.COMPANY = $comp\n";

                sql += $"ORDER BY w.{dateField} ASC, w.TMIN ASC;";

                // 3) rows
                var rows = new List<Dictionary<string, object?>>();
                var no = 0;

                using var conn = SqliteUtil.OpenReadOnly(dbPath);
                using var cmd = conn.CreateCommand();
                cmd.CommandText = sql;
                cmd.Parameters.AddWithValue("$stat", "2");
                cmd.Parameters.AddWithValue("$d1", startDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                cmd.Parameters.AddWithValue("$d2", endDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
                if (!string.IsNullOrWhiteSpace(productCode)) cmd.Parameters.AddWithValue("$prod", productCode);
                if (!string.IsNullOrWhiteSpace(companyCode)) cmd.Parameters.AddWithValue("$comp", companyCode);

                using var rdr = cmd.ExecuteReader();
                while (rdr.Read())
                {
                    ct.ThrowIfCancellationRequested();
                    no++;

                    var dText = (rdr["d"]?.ToString() ?? "").Trim();
                    var dIso = "";
                    if (DateTime.TryParseExact(dText, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                        dIso = dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

                    var prodName = (rdr["ProductName"]?.ToString() ?? "").Trim();
                    var compName = (rdr["CompanyName"]?.ToString() ?? "").Trim();
                    var truck = (rdr["TRUCK"]?.ToString() ?? "").Trim();
                    var typ = (rdr["CartypeName"]?.ToString() ?? "").Trim();

                    var w1 = ReadDouble(rdr["W1"]);
                    var w2 = ReadDouble(rdr["W2"]);
                    var net = w1 - w2;

                    rows.Add(new Dictionary<string, object?>
                    {
                        ["No"] = no,
                        ["Date"] = dIso,
                        ["Product"] = prodName,
                        ["Company"] = compName,
                        ["Truck"] = truck,
                        ["Type"] = typ,
                        ["GrossIn"] = (long)Math.Round(w1, 0, MidpointRounding.AwayFromZero),
                        ["GrossOut"] = (long)Math.Round(w2, 0, MidpointRounding.AwayFromZero),
                        ["NetWeight"] = (long)Math.Round(net, 0, MidpointRounding.AwayFromZero),
                    });
                }

                // 4) JSON response
                var payloadObj = new Dictionary<string, object?>
                {
                    ["ok"] = true,
                    ["meta"] = new Dictionary<string, object?>
                    {
                        ["startDate"] = startDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                        ["endDate"] = endDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                        ["dateField"] = dateField,
                        ["rawProduct"] = rawProduct,
                        ["rawCompany"] = rawCompany,
                        ["productCode"] = productCode ?? "",
                        ["companyCode"] = companyCode ?? "",
                        ["rows"] = rows.Count
                    },
                    ["rows"] = rows
                };

                await LookupHandlers.WriteJsonAsync(resp, payloadObj, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await LookupHandlers.WriteErrorAsync(resp, 400, ex.Message, ct).ConfigureAwait(false);
            }
            finally
            {
                LookupHandlers.SafeClose(resp);
            }
        }

        // helpers (same logic)
        private static string JsonGetStringSafe(JsonElement root, string name)
        {
            if (root.ValueKind != JsonValueKind.Object) return "";
            if (!root.TryGetProperty(name, out var p)) return "";
            if (p.ValueKind == JsonValueKind.Null) return "";
            return p.ValueKind == JsonValueKind.String ? (p.GetString() ?? "") : p.ToString();
        }

        private static string NormalizeOptCode(string? s)
        {
            var t = (s ?? "").Trim();
            if (t.Equals("(All)", StringComparison.OrdinalIgnoreCase) ||
                t.Equals("(ทั้งหมด)", StringComparison.OrdinalIgnoreCase))
                return "";
            return t;
        }

        private static DateTime ParseYmd(string s, string fieldName)
        {
            s = (s ?? "").Trim();
            if (!DateTime.TryParseExact(s, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out var dt))
                throw new Exception($"{fieldName} must be yyyy-MM-dd");
            return dt.Date;
        }

        private static double ReadDouble(object? v)
        {
            if (v == null || v == DBNull.Value) return 0d;
            if (v is double d) return d;
            if (v is float f) return f;
            if (v is decimal m) return (double)m;
            if (double.TryParse(v.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var x)) return x;
            if (double.TryParse(v.ToString(), NumberStyles.Any, CultureInfo.CurrentCulture, out var y)) return y;
            return 0d;
        }
    }
}
