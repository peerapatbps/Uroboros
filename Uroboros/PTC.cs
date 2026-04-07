// HOW IT WORKS:
//
// ExecuteAsync
// └─ STEP1) FetchEmbedSrcListAsync(common.php) -> List<string> (relative gensvg.php?...)
// └─ STEP2) foreach rel in list
//      ├─ TryGetTypename(rel) -> code
//      ├─ ExtractUpperLowerFromGensvgRelAsync(rel) -> Dictionary<hh:mm,(Ucl,Lcl)>
//      └─ await saveFuncAsync(code, dict)  (caller เป็นคนเซฟ DB/ไฟล์/อะไรก็ได้)
// └─ end method -> objects out of scope -> GC ได้ / memory footprint ต่ำ (HttpClient เป็น static)
//
// Notes:
// - PTC.cs ทำหน้าที่ "fetch + parse" เป็นหลัก
// - การ persist (SQLite) แยกไปอยู่ใน caller ผ่าน saveFuncAsync เพื่อให้ยืดหยุ่น

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Uroboros
{
    internal static class PTC
    {
        private static readonly HttpClient _http = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(20)
        };

        // =========================================================
        // STEP1: Fetch gensvg list from common.php
        // =========================================================
        public static async Task<List<string>> FetchEmbedSrcListAsync(string commonUrl, CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(commonUrl)) return new List<string>();

            var html = await _http.GetStringAsync(commonUrl, ct).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(html)) return new List<string>();

            // commonUrl ลงท้าย .../svgtest/common.php?...
            // => folder = "svgtest"
            var baseFolder = GetLastFolderName(commonUrl);
            var prefixFolder = string.IsNullOrWhiteSpace(baseFolder) ? "" : (baseFolder.TrimEnd('/') + "/");

            // เก็บเฉพาะ gensvg.php?... (ไม่เอา ringin.wav)
            // รับทั้งแบบอยู่ใน src="..." หรือ text ธรรมดา
            var pattern = @"\b(gensvg\.php\?[^""'\s<>]+)";
            var matches = Regex.Matches(html, pattern, RegexOptions.IgnoreCase);

            if (matches.Count == 0) return new List<string>();

            var list = new List<string>(matches.Count);
            foreach (Match m in matches)
            {
                var rawUrl = m.Groups[1].Value;
                var decodedUrl = WebUtility.HtmlDecode(rawUrl).Trim();
                if (string.IsNullOrWhiteSpace(decodedUrl)) continue;

                // ensure relative like "svgtest/gensvg.php?...”
                list.Add(prefixFolder + decodedUrl);
            }

            // distinct only (keep original order-ish)
            return list
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Select(x => x.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();
        }

        private static string? GetLastFolderName(string url)
        {
            try
            {
                if (!Uri.TryCreate(url, UriKind.Absolute, out var uri))
                    return "svgtest"; // fallback safe

                var path = (uri.AbsolutePath ?? "").TrimEnd('/');
                if (path.Length == 0) return "svgtest";

                // remove last segment (file)
                var lastSlash = path.LastIndexOf('/');
                if (lastSlash < 0) return "svgtest";

                var folderPath = path.Substring(0, lastSlash).TrimEnd('/');
                if (folderPath.Length == 0) return "svgtest";

                var lastFolderSlash = folderPath.LastIndexOf('/');
                return lastFolderSlash >= 0 ? folderPath.Substring(lastFolderSlash + 1) : folderPath;
            }
            catch
            {
                return "svgtest";
            }
        }

        // =========================================================
        // STEP2: "ประกอบร่าง" จาก gensvg rel URL -> ยิง gensvg -> ยิง mysqlToClientSvg -> parse baseline
        //      - ไม่สน plan day/holiday
        //      - ยึด query จาก gensvg url เป็น source-of-truth (รวม refdate)
        // =========================================================

        private const string PtcRealtimeBase = "http://172.16.193.162/ptc/realtime/";
        private const string SvgTestBase = "http://172.16.193.162/ptc/realtime/svgtest/";

        private static string BuildFullUrl(string baseUrl, string relOrAbs)
        {
            baseUrl = (baseUrl ?? "").Trim();
            relOrAbs = (relOrAbs ?? "").Trim();

            if (string.IsNullOrWhiteSpace(baseUrl))
                baseUrl = "http://172.16.193.162/ptc/realtime/";

            // absolute already
            if (Uri.TryCreate(relOrAbs, UriKind.Absolute, out var abs))
                return abs.ToString();

            // normalize base
            if (!baseUrl.EndsWith("/", StringComparison.Ordinal)) baseUrl += "/";

            // normalize rel
            var rel = relOrAbs;

            // remove leading "./"
            while (rel.StartsWith("./", StringComparison.Ordinal))
                rel = rel.Substring(2);

            // handle "../" by Uri combine (safe)
            if (Uri.TryCreate(baseUrl, UriKind.Absolute, out var b))
            {
                var combined = new Uri(b, rel);
                return combined.ToString();
            }

            // fallback
            rel = rel.TrimStart('/');
            return baseUrl + rel;
        }

        private static Dictionary<string, string> ParseQueryLoose(Uri uri)
        {
            var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            var q = uri.Query;
            if (string.IsNullOrEmpty(q)) return dict;
            if (q.StartsWith("?", StringComparison.Ordinal)) q = q.Substring(1);

            foreach (var part in q.Split('&', StringSplitOptions.RemoveEmptyEntries))
            {
                var kv = part.Split('=', 2);
                var k = WebUtility.UrlDecode(kv[0] ?? "").Trim();
                var v = WebUtility.UrlDecode(kv.Length > 1 ? kv[1] : "").Trim();
                if (k.Length > 0) dict[k] = v;
            }
            return dict;
        }

        private static string StripScriptWrapper(string raw)
        {
            if (string.IsNullOrWhiteSpace(raw)) return raw ?? "";

            var idx = raw.LastIndexOf("</script>", StringComparison.OrdinalIgnoreCase);
            if (idx >= 0 && idx + "</script>".Length < raw.Length)
                return raw.Substring(idx + "</script>".Length).Trim();

            return raw.Trim();
        }

        private static string NormalizeHHmm(string timeToken)
        {
            timeToken = (timeToken ?? "").Trim();

            if (TimeSpan.TryParseExact(
                    timeToken,
                    new[] { @"h\:mm", @"hh\:mm", @"h\:mm\:ss", @"hh\:mm\:ss" },
                    CultureInfo.InvariantCulture,
                    out var ts))
            {
                return ts.ToString(@"hh\:mm", CultureInfo.InvariantCulture);
            }

            return timeToken;
        }

        private static bool TryParseDoubleInv(string s, out double v)
            => double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out v);

        // helper: เอา code(typename) ออกมา
        public static string? TryGetTypename(string gensvgRelOrAbs)
        {
            var full = BuildFullUrl(PtcRealtimeBase, gensvgRelOrAbs);
            if (!Uri.TryCreate(full, UriKind.Absolute, out var u)) return null;

            var q = ParseQueryLoose(u);
            return q.TryGetValue("typename", out var t) ? t : null;
        }

        // ผลลัพธ์: hhmm -> (UCL,LCL)
        public static async Task<Dictionary<string, (double Ucl, double Lcl)>> ExtractUpperLowerFromGensvgRelAsync(
            string gensvgRelOrAbs, CancellationToken ct)
        {
            var result = new Dictionary<string, (double Ucl, double Lcl)>(StringComparer.OrdinalIgnoreCase);

            if (string.IsNullOrWhiteSpace(gensvgRelOrAbs))
                return result;

            // 1) full gensvg url
            var fullGensvgUrl = BuildFullUrl(PtcRealtimeBase, gensvgRelOrAbs);
            if (!Uri.TryCreate(fullGensvgUrl, UriKind.Absolute, out var gensvgUri))
                return result;

            // 2) ยึด query จาก gensvg url
            var q = ParseQueryLoose(gensvgUri);
            q.TryGetValue("typename", out var typename);
            q.TryGetValue("sdate", out var sdate);
            q.TryGetValue("edate", out var edate);
            q.TryGetValue("refdate", out var refdate);
            q.TryGetValue("adj", out var adj);

            if (string.IsNullOrWhiteSpace(typename) ||
                string.IsNullOrWhiteSpace(sdate) ||
                string.IsNullOrWhiteSpace(edate) ||
                string.IsNullOrWhiteSpace(refdate))
            {
                return result; // required params missing
            }

            if (string.IsNullOrWhiteSpace(adj)) adj = "0";

            // 3) GET gensvg.php เพื่อ parse siteName + upper/lower
            var svgHtml = await _http.GetStringAsync(gensvgUri, ct).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(svgHtml)) return result;

            // defaults
            var siteName = "rtu3";
            var upperLimit = 0.1;
            var lowerLimit = 0.4;

            // onload="init(evt,'..','..','..','..','rtu3','2001-1-1',0.1,0.4,0)"
            // ทำให้ regex ยืดหยุ่นขึ้นนิด (ไม่ล็อกตำแหน่งด้วย [^']* มากเกินไป)
            var m = Regex.Match(
                svgHtml,
                @"onload\s*=\s*""init\(\s*evt\s*,\s*'[^']*'\s*,\s*'[^']*'\s*,\s*'[^']*'\s*,\s*'[^']*'\s*,\s*'(?<site>[^']*)'\s*,\s*'(?<ref>[^']*)'\s*,\s*(?<u>[-0-9\.]+)\s*,\s*(?<l>[-0-9\.]+)\s*,",
                RegexOptions.IgnoreCase
            );

            if (m.Success)
            {
                var s = (m.Groups["site"].Value ?? "").Trim();
                if (!string.IsNullOrWhiteSpace(s)) siteName = s;

                if (TryParseDoubleInv(m.Groups["u"].Value, out var u)) upperLimit = u;
                if (TryParseDoubleInv(m.Groups["l"].Value, out var l)) lowerLimit = l;
            }

            // 4) mysqlToClientSvg url (refdate/sdate/edate/adj มาจาก gensvg url)
            var file = string.Equals(siteName, "tb", StringComparison.OrdinalIgnoreCase)
                ? "mysqlToClientSvg_TB.php"
                : "mysqlToClientSvg.php";

            var dataUrl =
                SvgTestBase + file +
                $"?site={Uri.EscapeDataString(siteName)}" +
                $"&type={Uri.EscapeDataString(typename)}" +
                $"&sdate={Uri.EscapeDataString(sdate)}" +
                $"&edate={Uri.EscapeDataString(edate)}" +
                $"&refdate={Uri.EscapeDataString(refdate)}" +
                $"&adj={Uri.EscapeDataString(adj)}";

            var rawData = await _http.GetStringAsync(dataUrl, ct).ConfigureAwait(false);
            rawData = StripScriptWrapper(rawData);
            if (string.IsNullOrWhiteSpace(rawData)) return result;

            // 5) Parse baseline
            // format: series@series@... * something...
            var parts = rawData.Split('*');
            if (parts.Length < 1) return result;

            var seriesArr = parts[0].Split('@');
            if (seriesArr.Length == 0) return result;

            // หา baseline date token (2001/2002)
            string? baseline = null;
            foreach (var s in seriesArr)
            {
                if (string.IsNullOrWhiteSpace(s)) continue;

                var segs = s.Split(';');
                if (segs.Length <= 1) continue;

                var dateStr = (segs[0] ?? "").Trim();
                if (dateStr == "1-1-2002" || dateStr == "1-1-2001")
                {
                    baseline = s;
                    break;
                }
            }
            if (baseline == null) return result;

            // baseline: date;hh:mm,val;hh:mm,val;...
            var tokens = baseline.Split(';');
            for (int i = 1; i < tokens.Length; i++)
            {
                var pair = tokens[i];
                if (string.IsNullOrWhiteSpace(pair)) continue;

                var tv = pair.Split(',');
                if (tv.Length != 2) continue;

                var hhmm = NormalizeHHmm(tv[0]);
                if (string.IsNullOrWhiteSpace(hhmm)) continue;

                if (!TryParseDoubleInv(tv[1], out var v)) continue;

                // UCL/LCL = baseline +/- limits
                result[hhmm] = (v + upperLimit, v - lowerLimit);
            }

            // FIX 23:59 -> 00:00 + ensure 00:00
            if (result.TryGetValue("23:59", out var vv))
            {
                if (!result.ContainsKey("00:00")) result["00:00"] = vv;
                result.Remove("23:59");
            }

            if (!result.ContainsKey("00:00"))
            {
                if (result.TryGetValue("23:00", out var v2300)) result["00:00"] = v2300;
                else if (result.Count > 0) result["00:00"] = result.OrderBy(kv => kv.Key, StringComparer.OrdinalIgnoreCase).First().Value;
            }

            return result;
        }

        // =========================================================
        // STEP3: วนทั้งหมด (caller ส่ง action ไปเซฟ DB เอง)
        // =========================================================
        public static async Task ExtractAndSaveAllAsync(
            IEnumerable<string> gensvgRelList,
            Func<string, Dictionary<string, (double Ucl, double Lcl)>, Task> saveFuncAsync,
            CancellationToken ct)
        {
            if (gensvgRelList == null) return;
            if (saveFuncAsync == null) throw new ArgumentNullException(nameof(saveFuncAsync));

            // normalize once
            var list = gensvgRelList
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Select(x => x.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            foreach (var rel in list)
            {
                ct.ThrowIfCancellationRequested();

                var code = TryGetTypename(rel);
                if (string.IsNullOrWhiteSpace(code)) continue;

                var dict = await ExtractUpperLowerFromGensvgRelAsync(rel, ct).ConfigureAwait(false);
                if (dict.Count == 0) continue;

                await saveFuncAsync(code!, dict).ConfigureAwait(false);
            }
        }

        // ============================
        // DB helpers (upper_lower only)
        // ============================

        public static void EnsureDbUpperLower(string dbPath)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(dbPath)!);

            // สร้างไฟล์ถ้ายังไม่มี (Sqlite จะสร้างให้ได้อยู่แล้ว แต่ทำชัด ๆ)
            if (!File.Exists(dbPath))
                File.WriteAllBytes(dbPath, Array.Empty<byte>());

            using var conn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbPath};");
            conn.Open();

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = "PRAGMA journal_mode=WAL;"; cmd.ExecuteNonQuery();
                cmd.CommandText = "PRAGMA synchronous=NORMAL;"; cmd.ExecuteNonQuery();
                cmd.CommandText = "PRAGMA busy_timeout=3000;"; cmd.ExecuteNonQuery();
                cmd.CommandText = "PRAGMA wal_autocheckpoint=1000;"; cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS upper_lower (
  code       TEXT NOT NULL,
  hhmm       TEXT NOT NULL,
  upper      REAL NOT NULL,
  lower      REAL NOT NULL,
  scraped_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
  PRIMARY KEY (code, hhmm)
);
CREATE INDEX IF NOT EXISTS idx_upper_lower_code_scraped ON upper_lower(code, scraped_at);";
                cmd.ExecuteNonQuery();
            }
        }

        public static void SaveUpperLowerToDb(
            string dbPath,
            string code,
            Dictionary<string, (double Ucl, double Lcl)> data)
        {
            if (string.IsNullOrWhiteSpace(code)) return;
            if (data == null || data.Count == 0) return;

            using var conn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbPath};");
            conn.Open();

            using var tx = conn.BeginTransaction();

            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = @"
                                INSERT OR REPLACE INTO upper_lower (code, hhmm, upper, lower)
                                VALUES ($code, $hhmm, $upper, $lower);";

            var pCode = cmd.CreateParameter(); pCode.ParameterName = "$code"; cmd.Parameters.Add(pCode);
            var pTime = cmd.CreateParameter(); pTime.ParameterName = "$hhmm"; cmd.Parameters.Add(pTime);
            var pUp = cmd.CreateParameter(); pUp.ParameterName = "$upper"; cmd.Parameters.Add(pUp);
            var pLo = cmd.CreateParameter(); pLo.ParameterName = "$lower"; cmd.Parameters.Add(pLo);

            pCode.Value = code.Trim();

            foreach (var kv in data)
            {
                var hhmm = NormalizeHHmm_CSharp(kv.Key);
                if (string.IsNullOrWhiteSpace(hhmm)) continue;

                pTime.Value = hhmm;
                pUp.Value = kv.Value.Ucl;
                pLo.Value = kv.Value.Lcl;

                cmd.ExecuteNonQuery();
            }

            tx.Commit();
        }

        private static string NormalizeHHmm_CSharp(string s)
        {
            s = (s ?? "").Trim();
            if (TimeSpan.TryParseExact(
                    s,
                    new[] { @"h\:mm", @"hh\:mm", @"h\:mm\:ss", @"hh\:mm\:ss" },
                    CultureInfo.InvariantCulture,
                    out var ts))
            {
                return ts.ToString(@"hh\:mm", CultureInfo.InvariantCulture);
            }
            return s;
        }
    }
}
