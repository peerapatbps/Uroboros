using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace Uroboros
{
    // ==============================
    // Singleton HttpClient holder
    // ==============================
    public static class HttpClientBox
    {
        // Handler ตั้งค่าเพื่อให้เหมาะกับงาน interval + CGI
        private static readonly SocketsHttpHandler _handler = new SocketsHttpHandler
        {
            AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
            MaxConnectionsPerServer = 8
        };

        public static readonly HttpClient Client = new HttpClient(_handler, disposeHandler: false)
        {
            Timeout = TimeSpan.FromSeconds(20)
        };
    }

    // ==============================
    // HTML helper
    // ==============================
    public static class HtmlText
    {
        private static readonly Regex _rxTags =
            new Regex("<.*?>", RegexOptions.Singleline | RegexOptions.Compiled);

        public static string StripTags(string? s)
        {
            if (string.IsNullOrEmpty(s)) return "";
            var x = _rxTags.Replace(s, "");
            x = WebUtility.HtmlDecode(x); // decode ที่นี่จุดเดียวพอ
            return x.Trim();
        }
    }

    // ==============================
    // Main module: HTML hook fetch + parse
    // ==============================
    public static class HtmlHookClient
    {
        // regex precompiled ช่วยลด CPU สำหรับงาน loop/interval
        private static readonly Regex RxNameTdB =
            new Regex("<td\\s+align=\"center\"[^>]*>\\s*<b>(.*?)</b>\\s*</td>",
                RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);

        private static readonly Regex RxValueTdFont =
            new Regex("<td\\s+align=\"center\">\\s*<font[^>]*>(.*?)</font>\\s*</td>",
                RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);

        private static readonly Regex RxTr =
            new Regex("<tr[^>]*>(.*?)</tr>",
                RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);

        private static readonly Regex RxTd =
            new Regex("<td[^>]*>(.*?)</td>",
                RegexOptions.IgnoreCase | RegexOptions.Singleline | RegexOptions.Compiled);

        private static readonly Regex RxLeadingNumber =
            new Regex(@"[-+]?\d+(\.\d+)?",
                RegexOptions.CultureInvariant | RegexOptions.Compiled);

        /// <summary>
        /// Fetch HTML from url and extract channel data.
        /// rows: list of row indices to return; if null/empty => return all
        /// itemtypes: per-row selector: "value" | "unit" | "both"
        /// returns list of (channelName, resultText)
        /// </summary>
        public static async Task<List<Tuple<string, string>>> FetchChannelDataAsync(
            string url,
            List<int>? rows = null,
            List<string>? itemtypes = null,
            CancellationToken ct = default)
        {
            var results = new List<Tuple<string, string>>();
            if (string.IsNullOrWhiteSpace(url)) return results;

            // ---- fetch html (safe) ----
            string html;
            try
            {
                using var req = new HttpRequestMessage(HttpMethod.Get, url);

                // กัน CGI/embedded web server เรื่องมาก
                req.Headers.TryAddWithoutValidation("User-Agent", "Uroboros/1.0");
                req.Headers.TryAddWithoutValidation("Accept", "text/html,*/*");

                using var resp = await HttpClientBox.Client
                    .SendAsync(req, HttpCompletionOption.ResponseHeadersRead, ct)
                    .ConfigureAwait(false);

                resp.EnsureSuccessStatusCode();
                html = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                return results; // empty
            }

            if (string.IsNullOrWhiteSpace(html)) return results;

            // PATH A: old logic (<b>/<font>)
            var channelNames = new List<string>();
            var rawValues = new List<string>(); // จะพยายามทำให้เป็น val,unit,val,unit...

            try
            {
                foreach (Match m in RxNameTdB.Matches(html))
                    channelNames.Add((m.Groups[1].Value ?? "").Trim());

                // ⚠️ fontMatches บางหน้าอาจให้มาแค่ value หรือ value+unit ปนกัน
                // เราจะเก็บทั้งหมดก่อน แล้วค่อย normalize เป็นคู่ภายหลัง
                var tmp = new List<string>();
                foreach (Match m in RxValueTdFont.Matches(html))
                    tmp.Add((m.Groups[1].Value ?? "").Trim());

                // Normalize: ถ้าจำนวนเป็นคู่ -> assume val/unit สลับ
                // ถ้าจำนวนเป็นคี่ -> ตัดตัวท้ายทิ้ง เพื่อไม่ทำให้ pair เพี้ยน
                if (tmp.Count >= 2)
                {
                    if (tmp.Count % 2 != 0)
                        tmp.RemoveAt(tmp.Count - 1);

                    rawValues.AddRange(tmp);
                }
            }
            catch
            {
                channelNames.Clear();
                rawValues.Clear();
            }

            // PATH B: fallback generic <tr><td>...</td></tr>
            if (channelNames.Count == 0 || rawValues.Count == 0)
            {
                channelNames.Clear();
                rawValues.Clear();

                try
                {
                    foreach (Match tr in RxTr.Matches(html))
                    {
                        var inner = tr.Groups[1].Value;
                        var tds = RxTd.Matches(inner);
                        if (tds.Count == 0) continue;

                        // skip header / meta rows
                        var first = HtmlText.StripTags(tds[0].Groups[1].Value);
                        if (first.Equals("Channel", StringComparison.OrdinalIgnoreCase) ||
                            first.IndexOf("Creation date", StringComparison.OrdinalIgnoreCase) >= 0 ||
                            first.IndexOf("Refresh rate", StringComparison.OrdinalIgnoreCase) >= 0)
                            continue;

                        int chIdx, rdIdx, unIdx;
                        if (tds.Count >= 8)
                        {
                            chIdx = 1; rdIdx = 6; unIdx = 7;
                        }
                        else if (tds.Count >= 3)
                        {
                            chIdx = 0; rdIdx = tds.Count - 2; unIdx = tds.Count - 1;
                        }
                        else
                        {
                            continue;
                        }

                        if (chIdx >= tds.Count || rdIdx >= tds.Count || unIdx >= tds.Count) continue;

                        var ch = HtmlText.StripTags(tds[chIdx].Groups[1].Value);
                        var rd = HtmlText.StripTags(tds[rdIdx].Groups[1].Value);
                        var un = HtmlText.StripTags(tds[unIdx].Groups[1].Value);
                        if (string.IsNullOrWhiteSpace(ch)) continue;

                        channelNames.Add(ch);

                        // ทำให้เหมือน path A: val, unit, val, unit...
                        rawValues.Add(rd);
                        rawValues.Add(un);
                    }
                }
                catch
                {
                    channelNames.Clear();
                    rawValues.Clear();
                }
            }

            // Build (value, unit) pairs
            var valuePairs = new List<Tuple<string, string>>();
            if (rawValues.Count >= 2)
            {
                // กันคี่อีกชั้น
                if (rawValues.Count % 2 != 0)
                    rawValues.RemoveAt(rawValues.Count - 1);

                for (int i = 0; i < rawValues.Count; i += 2)
                {
                    var val = HtmlText.StripTags(rawValues[i]).Trim();
                    var unit = HtmlText.StripTags(rawValues[i + 1]).Trim();
                    valuePairs.Add(Tuple.Create(val, unit));
                }
            }

            var safeCount = Math.Min(channelNames.Count, valuePairs.Count);
            if (safeCount <= 0) return results;

            // If rows specified -> return selected
            if (rows != null && rows.Count > 0)
            {
                for (int i = 0; i < rows.Count; i++)
                {
                    var r = rows[i];
                    var t = (itemtypes != null && i < itemtypes.Count) ? itemtypes[i] : "value";
                    t = (t ?? "value").Trim().ToLowerInvariant();

                    if (r >= 0 && r < safeCount)
                    {
                        var name = channelNames[r];
                        var pair = valuePairs[r];

                        var result = t == "unit"
                            ? pair.Item2
                            : (t == "both"
                                ? (pair.Item1 + " " + pair.Item2).Trim()
                                : pair.Item1);

                        results.Add(Tuple.Create(name, result));
                    }
                    else
                    {
                        results.Add(Tuple.Create($"row:{r}", ""));
                    }
                }
                return results;
            }

            // Else return all (both)
            for (int i = 0; i < safeCount; i++)
            {
                var name = channelNames[i];
                var pair = valuePairs[i];
                results.Add(Tuple.Create(name, (pair.Item1 + " " + pair.Item2).Trim()));
            }

            return results;
        }

        // ----------------------------
        // Helpers
        // ----------------------------
        public static string GetItem2(List<Tuple<string, string>> list, int index)
            => (index >= 0 && index < list.Count) ? (list[index].Item2 ?? "").Trim() : "";

        public static void AddIfValid(
            List<(string Param, string? ValueText, string? Unit)> payload,
            string param,
            string raw)
        {
            var s = (raw ?? "").Trim();
            if (string.IsNullOrWhiteSpace(s)) return;

            if (!TryParseDoubleSafe(s, out _)) return;

            // ถ้ายังไม่ได้แยก unit จริง ๆ ก็ใส่ null
            payload.Add((param, s, null));
        }

        private static bool TryParseDoubleSafe(string s, out double v)
        {
            s = (s ?? "").Trim();

            // เอาเลขนำหน้า
            var m = RxLeadingNumber.Match(s);
            if (m.Success) s = m.Value;

            s = s.Replace(",", "").Replace(" ", "");

            return double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out v)
                   && !double.IsNaN(v) && !double.IsInfinity(v);
        }
    }

    // ==============================
    // Metrics DB: current-only upsert
    // ==============================
    public static class MetricsDb
    {
        public static string DbPath
            => Path.Combine(AppContext.BaseDirectory, "data.db");

        public static void EnsureDb(string? path = null)
        {
            var p = string.IsNullOrWhiteSpace(path) ? DbPath : path!;
            Directory.CreateDirectory(Path.GetDirectoryName(p)!);

            using var conn = new SqliteConnection($"Data Source={p};");
            conn.Open();

            Exec(conn, "PRAGMA journal_mode=WAL;");
            Exec(conn, "PRAGMA synchronous=NORMAL;");
            Exec(conn, "PRAGMA busy_timeout=3000;");
            Exec(conn, "PRAGMA wal_autocheckpoint=1000;");

            Exec(conn, @"
CREATE TABLE IF NOT EXISTS metrics_current (
  stream     TEXT NOT NULL,
  param      TEXT NOT NULL,
  value_text TEXT,
  value_real REAL,
  unit       TEXT,
  ts_utc     TEXT NOT NULL,
  PRIMARY KEY (stream, param)
);
CREATE INDEX IF NOT EXISTS idx_metrics_current_stream ON metrics_current(stream);
CREATE INDEX IF NOT EXISTS idx_metrics_current_ts     ON metrics_current(ts_utc);
");
        }

        public static void UpsertCurrent(
            string stream,
            string param,
            string? valueText,
            string? unit,
            DateTimeOffset? tsUtc = null,
            string? path = null)
        {
            if (string.IsNullOrWhiteSpace(stream)) return;
            if (string.IsNullOrWhiteSpace(param)) return;

            var p = string.IsNullOrWhiteSpace(path) ? DbPath : path!;
            EnsureDb(p);

            var s = stream.Trim().ToUpperInvariant();
            var prm = param.Trim();

            var ts = (tsUtc ?? DateTimeOffset.UtcNow).ToOffset(TimeSpan.FromHours(7)); 
            var tsIso = ts.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);


            var text = (valueText ?? "").Trim();
            var u = (unit ?? "").Trim();

            double? real = TryParseDoubleInvariant(text, out var d) ? d : (double?)null;

            using var conn = new SqliteConnection($"Data Source={p};");
            conn.Open();
            Exec(conn, "PRAGMA busy_timeout=3000;");

            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
INSERT INTO metrics_current (stream, param, value_text, value_real, unit, ts_utc)
VALUES ($s, $p, $txt, $real, $unit, $ts)
ON CONFLICT(stream, param) DO UPDATE SET
  value_text = excluded.value_text,
  value_real = excluded.value_real,
  unit       = excluded.unit,
  ts_utc     = excluded.ts_utc;
";
            cmd.Parameters.AddWithValue("$s", s);
            cmd.Parameters.AddWithValue("$p", prm);
            cmd.Parameters.AddWithValue("$txt", string.IsNullOrEmpty(text) ? (object)DBNull.Value : text);
            cmd.Parameters.AddWithValue("$real", real.HasValue ? real.Value : (object)DBNull.Value);
            cmd.Parameters.AddWithValue("$unit", string.IsNullOrEmpty(u) ? (object)DBNull.Value : u);
            cmd.Parameters.AddWithValue("$ts", tsIso);

            cmd.ExecuteNonQuery();
        }

        public static void UpsertCurrentBatch(
            string stream,
            IEnumerable<(string Param, string? ValueText, string? Unit)> items,
            DateTimeOffset? tsUtc = null,
            string? path = null)
        {
            if (string.IsNullOrWhiteSpace(stream)) return;
            if (items == null) return;

            var p = string.IsNullOrWhiteSpace(path) ? DbPath : path!;
            EnsureDb(p);

            var s = stream.Trim().ToUpperInvariant();
            var ts = (tsUtc ?? DateTimeOffset.UtcNow).ToOffset(TimeSpan.FromHours(7));  
            var tsIso = ts.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);


            using var conn = new SqliteConnection($"Data Source={p};");
            conn.Open();
            Exec(conn, "PRAGMA busy_timeout=3000;");

            using var tx = conn.BeginTransaction();
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;

            cmd.CommandText = @"
INSERT INTO metrics_current (stream, param, value_text, value_real, unit, ts_utc)
VALUES ($s, $p, $txt, $real, $unit, $ts)
ON CONFLICT(stream, param) DO UPDATE SET
  value_text = excluded.value_text,
  value_real = excluded.value_real,
  unit       = excluded.unit,
  ts_utc     = excluded.ts_utc;
";

            var ps = cmd.CreateParameter(); ps.ParameterName = "$s"; cmd.Parameters.Add(ps);
            var pp = cmd.CreateParameter(); pp.ParameterName = "$p"; cmd.Parameters.Add(pp);
            var ptxt = cmd.CreateParameter(); ptxt.ParameterName = "$txt"; cmd.Parameters.Add(ptxt);
            var preal = cmd.CreateParameter(); preal.ParameterName = "$real"; cmd.Parameters.Add(preal);
            var punit = cmd.CreateParameter(); punit.ParameterName = "$unit"; cmd.Parameters.Add(punit);
            var pts = cmd.CreateParameter(); pts.ParameterName = "$ts"; cmd.Parameters.Add(pts);

            ps.Value = s;
            pts.Value = tsIso;

            foreach (var it in items)
            {
                if (string.IsNullOrWhiteSpace(it.Param)) continue;

                var prm = it.Param.Trim();
                var text = (it.ValueText ?? "").Trim();
                var unit = (it.Unit ?? "").Trim();

                double? real = TryParseDoubleInvariant(text, out var d) ? d : (double?)null;

                pp.Value = prm;
                ptxt.Value = string.IsNullOrEmpty(text) ? (object)DBNull.Value : text;
                preal.Value = real.HasValue ? real.Value : (object)DBNull.Value;
                punit.Value = string.IsNullOrEmpty(unit) ? (object)DBNull.Value : unit;

                cmd.ExecuteNonQuery();
            }

            tx.Commit();
        }

        public static Dictionary<string, (string? ValueText, double? ValueReal, string? Unit, DateTimeOffset? TsUtc)>
            GetCurrent(string stream, IEnumerable<string> @params, string? path = null)
        {
            var res = new Dictionary<string, (string?, double?, string?, DateTimeOffset?)>(StringComparer.OrdinalIgnoreCase);

            var names = @params?.Where(x => !string.IsNullOrWhiteSpace(x)).Select(x => x.Trim()).ToArray();
            if (names == null || names.Length == 0) return res;

            var p = string.IsNullOrWhiteSpace(path) ? DbPath : path!;
            EnsureDb(p);

            using var conn = new SqliteConnection($"Data Source={p};");
            conn.Open();

            var ph = new string[names.Length];
            for (int i = 0; i < names.Length; i++) ph[i] = "$p" + i;

            using var cmd = conn.CreateCommand();
            cmd.CommandText = $@"
SELECT param, value_text, value_real, unit, ts_utc
FROM metrics_current
WHERE stream = $s AND param IN ({string.Join(",", ph)});
";
            cmd.Parameters.AddWithValue("$s", stream.Trim().ToUpperInvariant());
            for (int i = 0; i < names.Length; i++) cmd.Parameters.AddWithValue(ph[i], names[i]);

            using var rd = cmd.ExecuteReader();
            while (rd.Read())
            {
                var prm = rd.GetString(0);
                string? text = rd.IsDBNull(1) ? null : rd.GetString(1);
                double? real = rd.IsDBNull(2) ? null : rd.GetDouble(2);
                string? unit = rd.IsDBNull(3) ? null : rd.GetString(3);

                DateTimeOffset? ts = null;
                var tsStr = rd.IsDBNull(4) ? null : rd.GetString(4);
                if (!string.IsNullOrWhiteSpace(tsStr) &&
                    DateTime.TryParseExact(tsStr, "yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture,
                        DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal, out var dt))
                {
                    ts = new DateTimeOffset(dt, TimeSpan.Zero);
                }

                res[prm] = (text, real, unit, ts);
            }

            return res;
        }

        private static void Exec(SqliteConnection conn, string sql)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        private static bool TryParseDoubleInvariant(string s, out double v)
        {
            s = (s ?? "").Trim().Replace(",", "").Replace(" ", "");
            return double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out v);
        }
    }

    // ==============================
    // DB: branch_current (current-only, replace always)
    // ==============================
    public static class BranchCurrentDb
    {
        public static string DbPath => Path.Combine(AppContext.BaseDirectory, "data.db");

        public static void EnsureDb(string? path = null)
        {
            var p = string.IsNullOrWhiteSpace(path) ? DbPath : path!;
            Directory.CreateDirectory(Path.GetDirectoryName(p)!);

            using var conn = new SqliteConnection($"Data Source={p};");
            conn.Open();

            Exec(conn, "PRAGMA journal_mode=WAL;");
            Exec(conn, "PRAGMA synchronous=NORMAL;");
            Exec(conn, "PRAGMA busy_timeout=3000;");
            Exec(conn, "PRAGMA wal_autocheckpoint=1000;");

            Exec(conn, @"
CREATE TABLE IF NOT EXISTS branch_current (
  source   TEXT NOT NULL,
  key      TEXT NOT NULL,
  value    REAL,
  ts_th    TEXT NOT NULL,
  PRIMARY KEY (source, key)
);
CREATE INDEX IF NOT EXISTS idx_branch_current_source ON branch_current(source);
CREATE INDEX IF NOT EXISTS idx_branch_current_ts     ON branch_current(ts_th);
");
        }

        public static void UpsertSnapshot(
            string source,
            DateTimeOffset? tsUtc,
            double level,
            double qin,
            double inlet,
            double flowF,
            double press,
            string? path = null)
        {
            if (string.IsNullOrWhiteSpace(source)) return;

            var p = string.IsNullOrWhiteSpace(path) ? DbPath : path!;
            EnsureDb(p);

            // ✅ TH time standard
            var ts = (tsUtc ?? DateTimeOffset.UtcNow).ToOffset(TimeSpan.FromHours(7));
            var tsIso = ts.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

            using var conn = new SqliteConnection($"Data Source={p};");
            conn.Open();
            Exec(conn, "PRAGMA busy_timeout=3000;");

            using var tx = conn.BeginTransaction();
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;

            cmd.CommandText = @"
INSERT INTO branch_current (source, key, value, ts_th)
VALUES ($s, $k, $v, $ts)
ON CONFLICT(source, key) DO UPDATE SET
  value = excluded.value,
  ts_th = excluded.ts_th;
";
            var ps = cmd.CreateParameter(); ps.ParameterName = "$s"; cmd.Parameters.Add(ps);
            var pk = cmd.CreateParameter(); pk.ParameterName = "$k"; cmd.Parameters.Add(pk);
            var pv = cmd.CreateParameter(); pv.ParameterName = "$v"; cmd.Parameters.Add(pv);
            var pts = cmd.CreateParameter(); pts.ParameterName = "$ts"; cmd.Parameters.Add(pts);

            ps.Value = source.Trim().ToUpperInvariant();
            pts.Value = tsIso;

            UpsertOne("Level", level);
            UpsertOne("Qin", qin);
            UpsertOne("Inlet", inlet);
            UpsertOne("F", flowF);
            UpsertOne("P", press);

            tx.Commit();

            void UpsertOne(string key, double val)
            {
                pk.Value = key;
                pv.Value = (double.IsNaN(val) || double.IsInfinity(val)) ? (object)DBNull.Value : val;
                cmd.ExecuteNonQuery();
            }
        }

        private static void Exec(SqliteConnection conn, string sql)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }
    }

    // ==============================
    // Station API: parse JSON (Level/Qin/Inlet/F direct, P=avg of *_P*)
    // ==============================
    public static class StationApi
    {
        public static async Task<JsonDocument?> FetchDocAsync(
            string url = "http://172.16.193.162/smartmap/data_station_real.php?",
            CancellationToken ct = default)
        {
            try
            {
                var jsonText = await HttpClientBox.Client.GetStringAsync(url, ct).ConfigureAwait(false);
                if (string.IsNullOrWhiteSpace(jsonText)) return null;
                return JsonDocument.Parse(jsonText);
            }
            catch
            {
                return null;
            }
        }

        public static Dictionary<string, double> ExtractStationMetrics(
            JsonElement root,
            string stationKey)
        {
            var result = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase)
            {
                ["Level"] = double.NaN,
                ["Qin"] = double.NaN,
                ["Inlet"] = double.NaN,
                ["F"] = double.NaN,
                ["P"] = double.NaN
            };

            if (root.ValueKind != JsonValueKind.Object) return result;
            if (string.IsNullOrWhiteSpace(stationKey)) return result;

            stationKey = stationKey.Trim();

            // direct keys (เหมือน VB)
            ReadDirect("_Level", "Level");
            ReadDirect("_Qin", "Qin");
            ReadDirect("_Inlet", "Inlet");
            ReadDirect("_F", "F");

            // ✅ P = average of ALL keys like "{K}_P" / "{K}_P_*"
            result["P"] = AverageP(root, stationKey);

            return result;

            void ReadDirect(string suffix, string metricKey)
            {
                var fullKey = stationKey + suffix;
                if (root.TryGetProperty(fullKey, out var token))
                {
                    if (TryToDouble(token, out var d))
                        result[metricKey] = d;
                }
            }
        }

        private static double AverageP(JsonElement root, string stationKey)
        {
            var prefix = stationKey + "_P"; // matches: PK_P, RB_P_Ram2, TP_P_Wong, ...
            double sum = 0;
            int n = 0;

            foreach (var prop in root.EnumerateObject())
            {
                if (!prop.Name.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (TryToDouble(prop.Value, out var d) && !double.IsNaN(d) && !double.IsInfinity(d))
                {
                    sum += d;
                    n++;
                }
            }

            return n > 0 ? (sum / n) : double.NaN;
        }

        private static bool TryToDouble(JsonElement token, out double value)
        {
            value = double.NaN;

            try
            {
                if (token.ValueKind == JsonValueKind.Number)
                    return token.TryGetDouble(out value);

                if (token.ValueKind == JsonValueKind.String)
                {
                    var s = (token.GetString() ?? "").Trim();
                    if (double.TryParse(s, NumberStyles.Any, CultureInfo.InvariantCulture, out value)) return true;
                    if (double.TryParse(s, NumberStyles.Any, CultureInfo.CurrentCulture, out value)) return true;
                }
            }
            catch { }

            value = double.NaN;
            return false;
        }
    }

    // ==============================
    // BranchFetchService: fetch once, extract per branch, upsert branch_current
    // ==============================
    public static class BranchFetchService
    {
        public static async Task<int> FetchBranchesAndSaveAsync(
            IEnumerable<string> branches,
            string url = "http://172.16.193.162/smartmap/data_station_real.php?",
            int maxConcurrency = 4,
            CancellationToken ct = default)
        {
            if (branches == null) return 0;

            var branchList = branches
                .Where(b => !string.IsNullOrWhiteSpace(b))
                .Select(b => b.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            if (branchList.Count == 0) return 0;

            // ✅ fetch JSON ONCE
            using var doc = await StationApi.FetchDocAsync(url, ct).ConfigureAwait(false);
            if (doc == null) return 0;

            var root = doc.RootElement;
            if (root.ValueKind != JsonValueKind.Object) return 0;

            // concurrency gate (จริง ๆ งานนี้เบาแล้ว แต่เผื่ออนาคตมีงานประกอบ)
            var sem = new SemaphoreSlim(Math.Max(1, maxConcurrency), Math.Max(1, maxConcurrency));
            var tasks = new List<Task>();
            int success = 0;

            foreach (var br in branchList)
            {
                await sem.WaitAsync(ct).ConfigureAwait(false);

                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        ct.ThrowIfCancellationRequested();

                        var data = StationApi.ExtractStationMetrics(root, br);

                        // เอาค่าตรง ๆ (อย่า GetOrZero ไม่งั้น NaN จะโดนกลืน)
                        var level = data.TryGetValue("Level", out var v1) ? v1 : double.NaN;
                        var qin = data.TryGetValue("Qin", out var v2) ? v2 : double.NaN;
                        var inlet = data.TryGetValue("Inlet", out var v3) ? v3 : double.NaN;
                        var flowF = data.TryGetValue("F", out var v4) ? v4 : double.NaN;
                        var press = data.TryGetValue("P", out var v5) ? v5 : double.NaN;

                        BranchCurrentDb.UpsertSnapshot(
                            source: br,
                            tsUtc: DateTimeOffset.UtcNow,
                            level: level,
                            qin: qin,
                            inlet: inlet,
                            flowF: flowF,
                            press: press);

                        Interlocked.Increment(ref success);
                    }
                    catch
                    {
                        // จะ log ก็ได้ ตามสไตล์ engine ของคุณครับ
                    }
                    finally
                    {
                        sem.Release();
                    }
                }, ct));
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            return success;
        }
    }

    public static class TimeUtil
    {
        // ✅ มาตรฐานเวลาไทยที่คุณกำหนด
        public static string ToThaiIso(DateTimeOffset? tsUtc = null)
        {
            var ts = (tsUtc ?? DateTimeOffset.UtcNow).ToOffset(TimeSpan.FromHours(7));
            return ts.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        }
    }


    public static class RcvSeriesRefresher
    {
        private static readonly Regex _dataRegex = new Regex(
            @"x:'(?<x>[^']+)'\s*,\s*y:'(?<y>-?\d+(?:\.\d+)?|null)'",
            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        public static async Task<int> RefreshRcvSeriesAsync(
            string url,
            string dbPath,
            string source,
            string key,
            string? unit = null,
            CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(url)) return 0;
            if (string.IsNullOrWhiteSpace(source)) return 0;
            if (string.IsNullOrWhiteSpace(key)) return 0;

            // -------------------------------
            // 1) DELETE old series (source/key)
            // -------------------------------
            using (var connDel = new SqliteConnection($"Data Source={dbPath};"))
            {
                connDel.Open();
                using var cmdDel = connDel.CreateCommand();
                cmdDel.CommandText = "DELETE FROM OneValueSeries WHERE source=@s AND key=@k;";
                cmdDel.Parameters.AddWithValue("@s", source);
                cmdDel.Parameters.AddWithValue("@k", key);
                cmdDel.ExecuteNonQuery();
            }

            // -------------------------------
            // 2) GET raw text
            // -------------------------------
            string raw;
            using (var hc = new HttpClient())
            {
                raw = await hc.GetStringAsync(url, ct).ConfigureAwait(false);
            }

            if (string.IsNullOrWhiteSpace(raw))
                return 0;

            // -------------------------------
            // 3) INSERT series
            // -------------------------------
            int affected = 0;

            using (var conn = new SqliteConnection($"Data Source={dbPath};"))
            {
                conn.Open();
                using var tx = conn.BeginTransaction();

                const string sql = @"
    INSERT OR REPLACE INTO OneValueSeries
    (source, key, ts, value, unit, updated_at)
    VALUES (@source, @key, @ts, @value, @unit, @updated);";

                using var cmd = new SqliteCommand(sql, conn, tx);

                var pSource = cmd.Parameters.Add("@source", SqliteType.Text);
                var pKey = cmd.Parameters.Add("@key", SqliteType.Text);
                var pTs = cmd.Parameters.Add("@ts", SqliteType.Text);
                var pVal = cmd.Parameters.Add("@value", SqliteType.Real);
                var pUnit = cmd.Parameters.Add("@unit", SqliteType.Text);
                var pUpd = cmd.Parameters.Add("@updated", SqliteType.Text);

                pSource.Value = source;
                pKey.Value = key;
                pUnit.Value = string.IsNullOrWhiteSpace(unit) ? DBNull.Value : unit;

                foreach (Match m in _dataRegex.Matches(raw))
                {
                    var yStr = m.Groups["y"].Value;
                    if (string.Equals(yStr, "null", StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (!double.TryParse(yStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var yVal))
                        continue;

                    var tsRaw = m.Groups["x"].Value;
                    var tsIso = NormalizeTsToThai(tsRaw);

                    pTs.Value = tsIso;
                    pVal.Value = yVal;
                    pUpd.Value = tsIso;

                    affected += cmd.ExecuteNonQuery();
                }

                tx.Commit();
            }

            return affected;
        }

        // -------------------------------
        // TH time normalize (มาตรฐานคุณ)
        // -------------------------------
        private static string NormalizeTsToThai(string ts)
        {
            if (DateTime.TryParse(ts, CultureInfo.InvariantCulture,
                DateTimeStyles.AssumeLocal, out var dt))
            {
                var dto = new DateTimeOffset(dt);
                var th = dto.ToOffset(TimeSpan.FromHours(7));
                return th.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            }

            // fallback: now TH
            var nowTh = DateTimeOffset.UtcNow.ToOffset(TimeSpan.FromHours(7));
            return nowTh.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        }
    }

    public static class SingleValueDb
        {
            private static readonly object _initLock = new();
            private static bool _initialized;
            private static string _connStr = "";

            public static readonly string[] DefaultSources =
            {
            "CW1", "CW2", "CW3", "CW4",
            "FW1", "FW2", "FW3", "FW4",
            "RW1", "RW2",
            "TW1", "TW2", "TW3", "TW4"
        };

            // ✅ เพิ่ม default param (ไม่เปลี่ยน logic, แค่ทำให้เรียก Initialize() ได้)
            public static void Initialize(string dbPath = "data.db")
            {
                if (string.IsNullOrWhiteSpace(dbPath))
                    throw new ArgumentException("dbPath is required.", nameof(dbPath));

                lock (_initLock)
                {
                    if (_initialized) return;

                    _connStr = $"Data Source={dbPath};Cache=Shared;Pooling=True;";
                    using var conn = new SqliteConnection(_connStr);
                    conn.Open();

                    // WAL (optional แต่ช่วย performance)
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = "PRAGMA journal_mode=WAL;";
                        cmd.ExecuteNonQuery();
                    }

                    // ✅ ตารางค่าเดี่ยว (current)
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS OneValuemetrics (
    source      TEXT NOT NULL,
    key         TEXT NOT NULL,
    value       REAL,
    unit        TEXT,
    payload     TEXT,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source, key)
);
CREATE INDEX IF NOT EXISTS idx_ovm_source  ON OneValuemetrics(source);
CREATE INDEX IF NOT EXISTS idx_ovm_updated ON OneValuemetrics(updated_at);
";
                        cmd.ExecuteNonQuery();
                    }

                    // ✅ ตารางซีรีส์แบบแคบ
                    using (var cmd = conn.CreateCommand())
                    {
                        cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS OneValueSeries (
    source      TEXT NOT NULL,
    key         TEXT NOT NULL,
    ts          TEXT NOT NULL,  -- yyyy-MM-dd HH:mm:ss
    value       REAL NOT NULL,
    unit        TEXT,
    updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (source, key, ts)
);
CREATE INDEX IF NOT EXISTS idx_ovs_src_key_ts ON OneValueSeries(source, key, ts);
CREATE INDEX IF NOT EXISTS idx_ovs_updated    ON OneValueSeries(updated_at);
";
                        cmd.ExecuteNonQuery();
                    }

                    _initialized = true;
                }
            }

            // ✅ คง logic เดิม: ถ้ายังไม่ init ให้ init ด้วย path ที่ caller ส่งมา
            private static void EnsureInit(string dbPath)
            {
                if (_initialized) return;
                Initialize(dbPath);
            }

            private static SqliteConnection NewConn()
            {
                // ห้ามเรียก EnsureInit ที่นี่ (กัน recursion)
                if (!_initialized)
                    throw new InvalidOperationException("SingleValueDb not initialized. Call EnsureInit(dbPath) first.");
                return new SqliteConnection(_connStr);
            }

            // ===================== Current (OneValuemetrics) =====================

            public static void UpsertSingle(string source, string key, double? value, string? unit, string dbPath, DateTimeOffset? tsUtc = null)
            {
                EnsureInit(dbPath);

                var updatedAt = TimeUtil.ToThaiIso(tsUtc);
                using var conn = NewConn();
                conn.Open();

                using var cmd = conn.CreateCommand();
                cmd.CommandText = @"
INSERT INTO OneValuemetrics (source, key, value, unit, payload, updated_at)
VALUES ($s, $k, $v, $u, NULL, $t)
ON CONFLICT(source, key) DO UPDATE SET
    value = excluded.value,
    unit = excluded.unit,
    payload = NULL,
    updated_at = excluded.updated_at;
";
                cmd.Parameters.AddWithValue("$s", source);
                cmd.Parameters.AddWithValue("$k", key);
                cmd.Parameters.AddWithValue("$v", (object?)value ?? DBNull.Value);
                cmd.Parameters.AddWithValue("$u", (object?)unit ?? DBNull.Value);
                cmd.Parameters.AddWithValue("$t", updatedAt);

                cmd.ExecuteNonQuery();
            }

            // ===================== Series (OneValueSeries) =====================

            /// <summary>
            /// ✅ C# เวอร์ชันของ RefreshRcvSeriesAsync (คง logic เดิม)
            /// </summary>
            public static async Task<int> RefreshRcvSeriesAsync(
                string url,
                string dbPath,
                string source,
                string key,
                string? unit = null,
                CancellationToken ct = default)
            {
                if (string.IsNullOrWhiteSpace(url)) return 0;
                if (string.IsNullOrWhiteSpace(dbPath)) return 0;
                if (string.IsNullOrWhiteSpace(source)) return 0;
                if (string.IsNullOrWhiteSpace(key)) return 0;

                EnsureInit(dbPath);

                // 1) ลบของเก่าเฉพาะ source/key
                using (var connDel = NewConn())
                {
                    await connDel.OpenAsync(ct).ConfigureAwait(false);
                    using var cmdDel = connDel.CreateCommand();
                    cmdDel.CommandText = "DELETE FROM OneValueSeries WHERE source = $s AND key = $k;";
                    cmdDel.Parameters.AddWithValue("$s", source);
                    cmdDel.Parameters.AddWithValue("$k", key);
                    await cmdDel.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }

                // 2) ดึง raw
                string raw;
                using (var hc = new System.Net.Http.HttpClient())
                {
                    raw = await hc.GetStringAsync(url, ct).ConfigureAwait(false);
                }
                if (string.IsNullOrWhiteSpace(raw)) return 0;

                // 3) Regex x:'..', y:'..'
                var dataRegex = new Regex(
                    "x:'(?<x>[^']+)'[\\s]*,[\\s]*y:'(?<y>-?\\d+(?:\\.\\d+)?|null)'",
                    RegexOptions.IgnoreCase | RegexOptions.Compiled);

                // 4) Ingest -> DB
                var affected = 0;
                var updatedAt = TimeUtil.ToThaiIso(); // เวลาไทยของรอบนี้

                using var conn = NewConn();
                await conn.OpenAsync(ct).ConfigureAwait(false);

                using var tx = conn.BeginTransaction();
                using var cmd = conn.CreateCommand();
                cmd.Transaction = tx;

                cmd.CommandText = @"
INSERT OR REPLACE INTO OneValueSeries
(source, key, ts, value, unit, updated_at)
VALUES ($source, $key, $ts, $value, $unit, $updatedAt);
";

                var pSource = cmd.CreateParameter(); pSource.ParameterName = "$source"; cmd.Parameters.Add(pSource);
                var pKey = cmd.CreateParameter(); pKey.ParameterName = "$key"; cmd.Parameters.Add(pKey);
                var pTs = cmd.CreateParameter(); pTs.ParameterName = "$ts"; cmd.Parameters.Add(pTs);
                var pVal = cmd.CreateParameter(); pVal.ParameterName = "$value"; cmd.Parameters.Add(pVal);
                var pUnit = cmd.CreateParameter(); pUnit.ParameterName = "$unit"; cmd.Parameters.Add(pUnit);
                var pUpd = cmd.CreateParameter(); pUpd.ParameterName = "$updatedAt"; cmd.Parameters.Add(pUpd);

                pSource.Value = source;
                pKey.Value = key;
                pUnit.Value = string.IsNullOrWhiteSpace(unit) ? DBNull.Value : unit;
                pUpd.Value = updatedAt;

                foreach (Match m in dataRegex.Matches(raw))
                {
                    ct.ThrowIfCancellationRequested();

                    var timeStr = m.Groups["x"].Value;
                    var yStr = m.Groups["y"].Value;

                    if (string.Equals(yStr, "null", StringComparison.OrdinalIgnoreCase))
                        continue;

                    if (!double.TryParse(yStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var yVal))
                        continue;

                    // normalize ts (ถ้า parse ได้)
                    var tsOut = timeStr;
                    if (DateTime.TryParse(timeStr, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var dt))
                        tsOut = dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

                    pTs.Value = tsOut;
                    pVal.Value = yVal;

                    affected += await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }

                tx.Commit();
                return affected;
            }

            // =====================================================================
            // ✅ Methods needed by OnlineLabQuery (คง logicเดียวกับ VB ที่คุณให้มา)
            // =====================================================================

            public static void DeleteSourcesAll(IEnumerable<string>? sources)
            {
                EnsureInit("data.db"); // VB version เรียก EnsureInit() แล้ว fallback data.db

                if (sources is null) return;
                var list = sources.Where(s => !string.IsNullOrWhiteSpace(s))
                                  .Distinct(StringComparer.OrdinalIgnoreCase)
                                  .ToArray();
                if (list.Length == 0) return;

                using var conn = NewConn();
                conn.Open();
                using var tx = conn.BeginTransaction();

                var names = new List<string>(list.Length);
                var ps = new List<SqliteParameter>(list.Length);
                for (int i = 0; i < list.Length; i++)
                {
                    var pname = "@s" + i;
                    names.Add(pname);
                    ps.Add(new SqliteParameter(pname, list[i]));
                }

                var inClause = string.Join(",", names);

                using (var cmd1 = conn.CreateCommand())
                {
                    cmd1.Transaction = tx;
                    cmd1.CommandText = $"DELETE FROM OneValuemetrics WHERE source IN ({inClause});";
                    cmd1.Parameters.AddRange(ps);
                    cmd1.ExecuteNonQuery();
                }

                using (var cmd2 = conn.CreateCommand())
                {
                    cmd2.Transaction = tx;
                    cmd2.CommandText = $"DELETE FROM OneValueSeries WHERE source IN ({inClause});";
                    cmd2.Parameters.AddRange(ps);
                    cmd2.ExecuteNonQuery();
                }

                tx.Commit();
            }

            public static async Task SaveSeriesBatch(
                string source,
                string key,
                IEnumerable<(string ts, double value)>? points,
                string? unit = null)
            {
                EnsureInit("data.db");
                if (points is null) return;

                await Task.Run(() =>
                {
                    using var conn = NewConn();
                    conn.Open();
                    using var tx = conn.BeginTransaction();

                    using var cmd = conn.CreateCommand();
                    cmd.Transaction = tx;

                    cmd.CommandText = @"
                                    INSERT INTO OneValueSeries (source, key, ts, value, unit, updated_at)
                                    VALUES ($s, $k, $t, $v, $u, $upd)
                                    ON CONFLICT(source, key, ts) DO UPDATE SET
                                        value      = excluded.value,
                                        unit       = COALESCE(excluded.unit, OneValueSeries.unit),
                                        updated_at = excluded.updated_at;
                                    ";

                    var pS = cmd.CreateParameter(); pS.ParameterName = "$s"; cmd.Parameters.Add(pS);
                    var pK = cmd.CreateParameter(); pK.ParameterName = "$k"; cmd.Parameters.Add(pK);
                    var pT = cmd.CreateParameter(); pT.ParameterName = "$t"; cmd.Parameters.Add(pT);
                    var pV = cmd.CreateParameter(); pV.ParameterName = "$v"; cmd.Parameters.Add(pV);
                    var pU = cmd.CreateParameter(); pU.ParameterName = "$u"; cmd.Parameters.Add(pU);
                    var updatedAt = TimeUtil.ToThaiIso(); // ต้องเป็น +07
                    cmd.Parameters.AddWithValue("$upd", updatedAt);

                    pS.Value = source;
                    pK.Value = key;
                    pU.Value = (object?)unit ?? DBNull.Value;

                    foreach (var p in points)
                    {
                        pT.Value = p.ts;
                        pV.Value = p.value; // OneValueSeries.value NOT NULL → caller ต้องไม่ส่ง null/NaN
                        cmd.ExecuteNonQuery();
                    }

                    tx.Commit();
                }).ConfigureAwait(false);
            }

            public static async Task SaveSinglesBatch(
                IEnumerable<(string source, string key, double value, string? unit)>? items)
            {
                EnsureInit("data.db");
                if (items is null) return;

                await Task.Run(() =>
                {
                    using var conn = NewConn();
                    conn.Open();
                    using var tx = conn.BeginTransaction();

                    using var cmd = conn.CreateCommand();
                    cmd.Transaction = tx;

                    cmd.CommandText = @"
                                    INSERT INTO OneValuemetrics (source, key, value, unit, payload, updated_at)
                                    VALUES ($s, $k, $v, $u, NULL, $upd)
                                    ON CONFLICT(source, key) DO UPDATE SET
                                        value      = excluded.value,
                                        unit       = excluded.unit,
                                        payload    = NULL,
                                        updated_at = excluded.updated_at;
                                    ";

                    var pS = cmd.CreateParameter(); pS.ParameterName = "$s"; cmd.Parameters.Add(pS);
                    var pK = cmd.CreateParameter(); pK.ParameterName = "$k"; cmd.Parameters.Add(pK);
                    var pV = cmd.CreateParameter(); pV.ParameterName = "$v"; cmd.Parameters.Add(pV);
                    var pU = cmd.CreateParameter(); pU.ParameterName = "$u"; cmd.Parameters.Add(pU);
                    var updatedAt = TimeUtil.ToThaiIso(); // ต้องเป็น +07
                    cmd.Parameters.AddWithValue("$upd", updatedAt);

                    foreach (var it in items)
                    {
                        pS.Value = it.source;
                        pK.Value = it.key;
                        pV.Value = it.value;
                        pU.Value = it.unit is null ? DBNull.Value : (object)it.unit;
                        cmd.ExecuteNonQuery();
                    }

                    tx.Commit();
                }).ConfigureAwait(false);
            }

            public static async Task SavePayloadsBatch(
                IEnumerable<(string source, string key, string payload)>? items)
            {
                EnsureInit("data.db");
                if (items is null) return;

                await Task.Run(() =>
                {
                    using var conn = NewConn();
                    conn.Open();
                    using var tx = conn.BeginTransaction();

                    using var cmd = conn.CreateCommand();
                    cmd.Transaction = tx;

                    cmd.CommandText = @"
INSERT INTO OneValuemetrics (source, key, value, unit, payload, updated_at)
VALUES ($s, $k, NULL, NULL, $p, $upd)
ON CONFLICT(source, key) DO UPDATE SET
    payload    = excluded.payload,
    updated_at = excluded.updated_at;
";

                    var pS = cmd.CreateParameter(); pS.ParameterName = "$s"; cmd.Parameters.Add(pS);
                    var pK = cmd.CreateParameter(); pK.ParameterName = "$k"; cmd.Parameters.Add(pK);
                    var pP = cmd.CreateParameter(); pP.ParameterName = "$p"; cmd.Parameters.Add(pP);
                    var updatedAt = TimeUtil.ToThaiIso(); // ต้องเป็น +07
                    cmd.Parameters.AddWithValue("$upd", updatedAt);

                    foreach (var it in items)
                    {
                        pS.Value = it.source;
                        pK.Value = it.key;
                        pP.Value = it.payload;
                        cmd.ExecuteNonQuery();
                    }

                    tx.Commit();
                }).ConfigureAwait(false);
            }
        }
    
    public static class OnlineLabQuery
        {
            private const string ApiUrl = "http://172.16.198.150/api/graph";

            // ใช้ HttpClient แบบ reuse (ถ้าคุณมี HttpClientBox อยู่แล้ว จะเปลี่ยนมาใช้ HttpClientBox.Client ก็ได้)
            private static readonly HttpClient _http = new HttpClient
            {
                Timeout = TimeSpan.FromSeconds(20)
            };

            public static async Task RefreshSourcesFromHtmlAsync2(string htmlPath, CancellationToken ct = default)
            {
                // ให้ชัวร์ DB พร้อม (ตาม VB: Initialize() ไม่มีพารามิเตอร์)
                SingleValueDb.Initialize();

                // 1) ลบของเก่าทั้งหมดของแหล่งที่สนใจ
                SingleValueDb.DeleteSourcesAll(SingleValueDb.DefaultSources);

                // 2) ดึง JSON จาก API (ไม่ใช้ htmlPath แล้ว แต่คงพารามิเตอร์ไว้)
                var jsonStr = await _http.GetStringAsync(ApiUrl, ct).ConfigureAwait(false);

                // 3) parse → ingest ลง DB
                using var doc = JsonDocument.Parse(jsonStr);
                var root = doc.RootElement;

                // 4) ingest เฉพาะ source ที่ต้องการ
                var allow = new HashSet<string>(SingleValueDb.DefaultSources, StringComparer.OrdinalIgnoreCase);
                await IngestJsonRootToDbAsync_Filtered(root, allow, ct).ConfigureAwait(false);
            }

            private static async Task IngestJsonRootToDbAsync_Filtered(JsonElement root, HashSet<string> allow, CancellationToken ct)
            {
                var baseYear = DateTime.Today.Year; // คงไว้เหมือน VB (ถึงตอนนี้เราใช้ epoch เป็นหลัก)
                _ = baseYear;

                var singleItems = new List<(string source, string key, double value, string? unit)>();
                var payloadItems = new List<(string source, string key, string payload)>();

                foreach (var grp in root.EnumerateObject()) // RW1, RW2, ...
                {
                    ct.ThrowIfCancellationRequested();

                    var source = grp.Name;
                    if (allow is not null && !allow.Contains(source)) continue;

                    var grpVal = grp.Value;
                    if (grpVal.ValueKind != JsonValueKind.Object) continue;

                    foreach (var param in grpVal.EnumerateObject())
                    {
                        ct.ThrowIfCancellationRequested();

                        var key = param.Name;
                        var obj = param.Value;
                        if (obj.ValueKind != JsonValueKind.Object) continue;

                        double? cVal = TryParseDoubleOrNull(TryGetString(obj, "CValue"));
                        var paraDataStr = TryGetString(obj, "ParaData");
                        var paraMinStr = TryGetString(obj, "ParaMin");
                        var paraMaxStr = TryGetString(obj, "ParaMax");

                        int cStatus = 0;
                        if (obj.TryGetProperty("CStatus", out var tmp))
                            _ = int.TryParse(tmp.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out cStatus);

                        // payloadJson: ใช้ System.Text.Json แทน Newtonsoft
                        var payloadJson = JsonSerializer.Serialize(new
                        {
                            CStatus = cStatus,
                            HasParaData = !string.IsNullOrEmpty(paraDataStr),
                            ParaMin_len = (paraMinStr ?? "").Length,
                            ParaMax_len = (paraMaxStr ?? "").Length
                        });

                        payloadItems.Add((source: source, key: key + ".meta", payload: payloadJson));

                        if (cVal.HasValue)
                            singleItems.Add((source: source, key: key, value: cVal.Value, unit: null));

                    // ซีรีส์จริง
                    var mainPts = ExtractPointsToTsValue(paraDataStr, baseYear);
                    if (mainPts.Count > 0)
                        await SingleValueDb.SaveSeriesBatch(source, key, mainPts, unit: null).ConfigureAwait(false);

                    // เส้นอ้างอิง
                    var maxPts = ExtractPointsToTsValue(paraMaxStr, baseYear);
                    if (maxPts.Count > 0)
                        await SingleValueDb.SaveSeriesBatch(source, key + "_ParaMax", maxPts, unit: null).ConfigureAwait(false);

                    var minPts = ExtractPointsToTsValue(paraMinStr, baseYear);
                    if (minPts.Count > 0)
                        await SingleValueDb.SaveSeriesBatch(source, key + "_ParaMin", minPts, unit: null).ConfigureAwait(false);

                }
            }

                if (singleItems.Count > 0)
                    await SingleValueDb.SaveSinglesBatch(singleItems).ConfigureAwait(false);

                if (payloadItems.Count > 0)
                    await SingleValueDb.SavePayloadsBatch(payloadItems).ConfigureAwait(false);
            }

            // ==========================================================
            // Helpers
            // ==========================================================

            private static string? TryGetString(JsonElement obj, string name)
            {
                if (!obj.TryGetProperty(name, out var p)) return null;
                if (p.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined) return null;
                var s = p.ToString();
                return string.IsNullOrWhiteSpace(s) ? null : s;
            }

            private static double? TryParseDoubleOrNull(string? s)
            {
                if (string.IsNullOrWhiteSpace(s)) return null;

                if (double.TryParse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.InvariantCulture, out var v))
                    return v;

                // เผื่อบางทีใช้ culture แปลก ๆ
                if (double.TryParse(s, NumberStyles.Float | NumberStyles.AllowThousands, CultureInfo.CurrentCulture, out v))
                    return v;

                return null;
            }

            /// <summary>
            /// แปลง ParaData/ParaMin/ParaMax เป็น List(ts,value) โดย:
            /// - x เป็น epoch ms(13) / sec(10)
            /// - เก็บ ts เป็น UTC "yyyy-MM-dd HH:mm:ss" (TEXT)
            /// </summary>ExtractPointsToTsValue
           
        private static List<(string ts, double value)> ExtractPointsToTsValue(string? raw, int baseYear)
        {
            var list = new List<(string ts, double value)>();
            if (string.IsNullOrWhiteSpace(raw)) return list;

            var ms = dataRegex.Matches(raw);
            foreach (Match m in ms)
            {
                var xStr = m.Groups["x"].Value;
                var yStr = m.Groups["y"].Value;

                if (string.Equals(yStr, "null", StringComparison.OrdinalIgnoreCase))
                    continue;

                if (!double.TryParse(yStr, NumberStyles.Float, CultureInfo.InvariantCulture, out var y))
                    continue;

                var ts = ParseDayMonthHmThai(xStr, baseYear)
                    .ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

                list.Add((ts, y));
            }

            return list;
        }

        private static DateTime ParseDayMonthHmThai(string xStr, int baseYear)
        {
            if (string.IsNullOrWhiteSpace(xStr))
                return new DateTime(baseYear, 1, 1, 0, 0, 0);

            xStr = xStr.Trim();

            // 1) ลอง parse ตรง ๆ ก่อน (รองรับ "2026-02-11 10:30:00" ฯลฯ)
            if (DateTime.TryParse(
                    xStr,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeLocal,
                    out var dt0))
                return NormalizeYear(dt0, baseYear);

            // 2) ไทย/ทั่วไป: dd/MM HH:mm[:ss] หรือ dd-MM HH:mm[:ss] (ไม่มีปี)
            var th = CultureInfo.GetCultureInfo("th-TH");

            // ถ้าไม่มีปี → เติม baseYear เข้าไป
            var withYear = InsertYear(xStr, baseYear);

            // รองรับทั้ง / และ -
            string[] fmts =
            {
        "dd/MM/yyyy HH:mm",
        "dd/MM/yyyy HH:mm:ss",
        "d/M/yyyy HH:mm",
        "d/M/yyyy HH:mm:ss",
        "dd-MM-yyyy HH:mm",
        "dd-MM-yyyy HH:mm:ss",
        "d-M-yyyy HH:mm",
        "d-M-yyyy HH:mm:ss",
    };

            if (DateTime.TryParseExact(withYear, fmts, th, DateTimeStyles.AllowWhiteSpaces, out var dt1))
                return NormalizeYear(dt1, baseYear);

            // 3) fallback: parse แบบ th-TH
            if (DateTime.TryParse(withYear, th, DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeLocal, out var dt2))
                return NormalizeYear(dt2, baseYear);

            // 4) สุดท้าย: ไม่ throw
            return new DateTime(baseYear, 1, 1, 0, 0, 0);

            static string InsertYear(string s, int year)
            {
                // ถ้ามี yyyy อยู่แล้ว ไม่ต้องเติม
                // heuristic: ถ้ามีเลข 4 หลักติด ๆ อยู่ในส่วน date ถือว่ามีปี
                // (กัน "11/02/2026 10:30")
                if (System.Text.RegularExpressions.Regex.IsMatch(s, @"\b\d{4}\b"))
                    return s;

                var parts = s.Split(' ', StringSplitOptions.RemoveEmptyEntries);
                if (parts.Length < 2) return s;

                var datePart = parts[0];
                var timePart = parts[1];

                if (datePart.Contains("/")) return $"{datePart}/{year} {timePart}";
                if (datePart.Contains("-")) return $"{datePart}-{year} {timePart}";
                return s;
            }

            static DateTime NormalizeYear(DateTime dt, int baseYear)
            {
                // ถ้าเป็น พ.ศ. → แปลงเป็น ค.ศ.
                if (dt.Year >= 2400)
                    dt = dt.AddYears(-543);

                // ถ้าปีเพี้ยนมาก (เช่น 1483) → บังคับใช้ baseYear แต่คงเดือน/วัน/เวลา
                if (dt.Year < 1900 || dt.Year > 2100)
                    dt = new DateTime(baseYear, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second);

                return dt;
            }
        }

        internal static readonly Regex dataRegex =    new Regex(        @"x:\s*'(?<x>[^']+)',\s*y:\s*(?<y>null|[0-9.]+)",        RegexOptions.Compiled);







    }
    
}

