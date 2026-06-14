// HOW IT WORKS:
//
// Legacy mode:
// ExecuteAsync
// └─ STEP1) FetchEmbedSrcListAsync(common.php) -> List<string> (relative gensvg.php?...)
// └─ STEP2) foreach rel in list
//      ├─ TryGetTypename(rel) -> code
//      ├─ ExtractUpperLowerFromGensvgRelAsync(rel) -> Dictionary<hh:mm,(Ucl,Lcl)>
//      └─ await saveFuncAsync(code, dict)  (caller เป็นคนเซฟ DB/ไฟล์/อะไรก็ได้)
//
// NewGraph mode:
// └─ FetchEmbedSrcListAsync(...) fallback / FetchNewGraphUrlList() / ExtractAndSaveAllNewGraphAsync(...)
//      ├─ GET http://172.16.193.162/smartmap/newgraph/index.php?rtu={KEY}
//      ├─ parse const DATASETS
//      ├─ select _ptcRole upper/lower + ACTIVE_BASELINE_MODE (wd/hd)
//      └─ output Dictionary<hh:mm,(Ucl,Lcl)> เหมือนเดิม
//
// Notes:
// - PTC.cs ทำหน้าที่ "fetch + parse" เป็นหลัก
// - การ persist (SQLite) แยกไปอยู่ใน caller ผ่าน saveFuncAsync เพื่อให้ยืดหยุ่น

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
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
        // NEWGRAPH endpoint config
        // =========================================================
        private const string NewGraphBaseUrl = "http://172.16.193.162/smartmap/newgraph/index.php?rtu=";

        private static readonly string[] PtcRtuKeys =
        {
            "UZ5411",
            "U017",
            "U022",
            "UZ5412",
            "U040",
            "UZ0106",
            "UZ5601",
            "UZ0108",
            "P046",
            "U039",
            "UZ5410",
            "UZ5413",
            "U018",
            "UZ5415",
            "UZ5408",
            "UZ5407",
            "P050",
            "U029"
        };

        public static IReadOnlyList<string> GetNewGraphRtuKeys()
            => PtcRtuKeys;

        public static List<string> FetchNewGraphUrlList()
            => PtcRtuKeys
                .Select(k => NewGraphBaseUrl + Uri.EscapeDataString(k))
                .ToList();

        // =========================================================
        // STEP1: Fetch source list
        //        IMPORTANT:
        //        - Caller เดิมยังเรียก method นี้อยู่
        //        - ถ้า common.php หา gensvg ไม่เจอ ให้ fallback เป็น NewGraph 18 RTU ทันที
        //        - ทำให้ไม่เกิด log: "No gensvg links found (srcList=0)" อีก
        // =========================================================
        public static async Task<List<string>> FetchEmbedSrcListAsync(string commonUrl, CancellationToken ct)
        {
            if (IsNewGraphSeed(commonUrl))
                return FetchNewGraphUrlList();

            // endpoint ใหม่ไม่จำเป็นต้องพึ่ง common.php แล้ว
            // ถ้า config ส่งค่าว่างมา ให้ใช้ NewGraph 18 รายการเป็น default
            if (string.IsNullOrWhiteSpace(commonUrl))
                return FetchNewGraphUrlList();

            string html;
            try
            {
                html = await _http.GetStringAsync(commonUrl, ct).ConfigureAwait(false);
            }
            catch
            {
                // ถ้า common.php เก่าล่ม/เข้าไม่ได้ ให้เดิน endpoint ใหม่ต่อ
                return FetchNewGraphUrlList();
            }

            if (string.IsNullOrWhiteSpace(html))
                return FetchNewGraphUrlList();

            // commonUrl ลงท้าย .../svgtest/common.php?...
            // => folder = "svgtest"
            var baseFolder = GetLastFolderName(commonUrl);
            var prefixFolder = string.IsNullOrWhiteSpace(baseFolder) ? "" : (baseFolder.TrimEnd('/') + "/");

            // เก็บเฉพาะ gensvg.php?... (ไม่เอา ringin.wav)
            // รับทั้งแบบอยู่ใน src="..." หรือ text ธรรมดา
            var pattern = @"\b(gensvg\.php\?[^""'\s<>]+)";
            var matches = Regex.Matches(html, pattern, RegexOptions.IgnoreCase);

            // จุดแก้หลัก: ของเดิม return empty ทำให้ caller log srcList=0 แล้วจบงาน
            // ของใหม่ให้ fallback เป็น 18 newgraph URLs แทน
            if (matches.Count == 0)
                return FetchNewGraphUrlList();

            var list = new List<string>(matches.Count);
            foreach (Match m in matches)
            {
                var rawUrl = m.Groups[1].Value;
                var decodedUrl = WebUtility.HtmlDecode(rawUrl).Trim();
                if (string.IsNullOrWhiteSpace(decodedUrl)) continue;

                // ensure relative like "svgtest/gensvg.php?..."
                list.Add(prefixFolder + decodedUrl);
            }

            var legacyList = list
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .Select(x => x.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            return legacyList.Count > 0 ? legacyList : FetchNewGraphUrlList();
        }

        private static bool IsNewGraphSeed(string? s)
        {
            if (string.IsNullOrWhiteSpace(s)) return false;
            s = s.Trim();

            // รองรับการตั้งค่า config แบบง่าย เช่น "newgraph" / "newgraph:auto"
            if (s.Equals("newgraph", StringComparison.OrdinalIgnoreCase)) return true;
            if (s.Equals("newgraph:auto", StringComparison.OrdinalIgnoreCase)) return true;

            // รองรับการส่ง base URL หรือ URL ใด ๆ ของ newgraph มาเป็น seed
            return s.IndexOf("/smartmap/newgraph/", StringComparison.OrdinalIgnoreCase) >= 0;
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
        // LEGACY PTC endpoint config
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

        // helper: เอา code ออกมา
        // - NewGraph: code = legacy pressure code (rtu + "P"), e.g. UZ5411P
        // - Legacy: code = typename
        public static string? TryGetTypename(string gensvgRelOrAbs)
        {
            var rtu = TryGetNewGraphRtuKey(gensvgRelOrAbs);
            if (!string.IsNullOrWhiteSpace(rtu)) return BuildNewGraphPressureCode(rtu);

            var full = BuildFullUrl(PtcRealtimeBase, gensvgRelOrAbs);
            if (!Uri.TryCreate(full, UriKind.Absolute, out var u)) return null;

            var q = ParseQueryLoose(u);
            return q.TryGetValue("typename", out var t) ? t : null;
        }

        // ผลลัพธ์: hhmm -> (UCL,LCL)
        // รองรับทั้ง legacy gensvg และ newgraph URL/RTU key
        public static async Task<Dictionary<string, (double Ucl, double Lcl)>> ExtractUpperLowerFromGensvgRelAsync(
            string gensvgRelOrAbs, CancellationToken ct)
        {
            if (IsNewGraphInput(gensvgRelOrAbs))
                return await ExtractUpperLowerFromNewGraphAsync(gensvgRelOrAbs, ct).ConfigureAwait(false);

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

            NormalizeMidnightPoint(result);
            return result;
        }

        // =========================================================
        // NEWGRAPH parser
        // =========================================================
        public static async Task<Dictionary<string, (double Ucl, double Lcl)>> ExtractUpperLowerFromNewGraphAsync(
            string rtuOrUrl, CancellationToken ct)
        {
            var result = new Dictionary<string, (double Ucl, double Lcl)>(StringComparer.OrdinalIgnoreCase);

            var url = BuildNewGraphUrl(rtuOrUrl);
            if (string.IsNullOrWhiteSpace(url)) return result;

            var html = await _http.GetStringAsync(url, ct).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(html)) return result;

            var activeMode = ExtractJsConstString(html, "ACTIVE_BASELINE_MODE");
            if (string.IsNullOrWhiteSpace(activeMode)) activeMode = "wd";

            var datasetsLiteral = ExtractJsArrayLiteral(html, "DATASETS");
            if (string.IsNullOrWhiteSpace(datasetsLiteral)) return result;

            var datasets = SplitTopLevelObjectsFromArray(datasetsLiteral);
            if (datasets.Count == 0) return result;

            var ptcSets = new List<PtcDataset>();
            foreach (var dsObj in datasets)
            {
                var role = ExtractJsStringProperty(dsObj, "_ptcRole");
                if (!string.Equals(role, "upper", StringComparison.OrdinalIgnoreCase) &&
                    !string.Equals(role, "lower", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                var trendType = ExtractJsStringProperty(dsObj, "_trendType");
                var data = ExtractPointDictionary(dsObj);
                if (data.Count == 0) continue;

                ptcSets.Add(new PtcDataset
                {
                    Role = role.Trim().ToLowerInvariant(),
                    TrendType = string.IsNullOrWhiteSpace(trendType) ? "" : trendType.Trim().ToLowerInvariant(),
                    Data = data
                });
            }

            if (ptcSets.Count == 0) return result;

            var upper = FindPtcDataset(ptcSets, "upper", activeMode!);
            var lower = FindPtcDataset(ptcSets, "lower", activeMode!);

            // fallback: ถ้า activeMode หาไม่ครบ ให้เลือก trendType ที่ upper/lower มีร่วมกัน
            if (upper == null || lower == null)
            {
                var upperModes = ptcSets
                    .Where(x => string.Equals(x.Role, "upper", StringComparison.OrdinalIgnoreCase))
                    .Select(x => x.TrendType)
                    .ToHashSet(StringComparer.OrdinalIgnoreCase);

                var fallbackMode = ptcSets
                    .Where(x => string.Equals(x.Role, "lower", StringComparison.OrdinalIgnoreCase))
                    .Select(x => x.TrendType)
                    .FirstOrDefault(m => upperModes.Contains(m));

                if (!string.IsNullOrWhiteSpace(fallbackMode))
                {
                    upper = FindPtcDataset(ptcSets, "upper", fallbackMode);
                    lower = FindPtcDataset(ptcSets, "lower", fallbackMode);
                }
            }

            // final fallback: อย่างน้อยมี upper/lower คู่แรก
            upper ??= ptcSets.FirstOrDefault(x => string.Equals(x.Role, "upper", StringComparison.OrdinalIgnoreCase));
            lower ??= ptcSets.FirstOrDefault(x => string.Equals(x.Role, "lower", StringComparison.OrdinalIgnoreCase));

            if (upper == null || lower == null) return result;

            foreach (var hhmm in upper.Data.Keys.OrderBy(x => x, StringComparer.OrdinalIgnoreCase))
            {
                if (!lower.Data.TryGetValue(hhmm, out var lo)) continue;
                result[hhmm] = (upper.Data[hhmm], lo);
            }

            // Keep source points first, then fill any missing whole-hour points by linear interpolation.
            // Example: if source has 00:00 and 02:00 but no 01:00, create 01:00 from the two endpoints.
            NormalizeMidnightPoint(result);
            FillMissingHourlyByInterpolation(result);

            return result;
        }

        public static async Task ExtractAndSaveAllNewGraphAsync(
            Func<string, Dictionary<string, (double Ucl, double Lcl)>, Task> saveFuncAsync,
            CancellationToken ct)
        {
            if (saveFuncAsync == null) throw new ArgumentNullException(nameof(saveFuncAsync));

            foreach (var key in PtcRtuKeys)
            {
                ct.ThrowIfCancellationRequested();

                var dict = await ExtractUpperLowerFromNewGraphAsync(key, ct).ConfigureAwait(false);
                if (dict.Count == 0) continue;

                await saveFuncAsync(BuildNewGraphPressureCode(key), dict).ConfigureAwait(false);
            }
        }

        public static async Task ExtractAndSaveAllNewGraphToDbAsync(string dbPath, CancellationToken ct)
        {
            EnsureDbUpperLower(dbPath);

            await ExtractAndSaveAllNewGraphAsync(
                (code, dict) =>
                {
                    SaveUpperLowerToDb(dbPath, code, dict);
                    return Task.CompletedTask;
                },
                ct).ConfigureAwait(false);
        }

        private sealed class PtcDataset
        {
            public string Role { get; set; } = "";
            public string TrendType { get; set; } = "";
            public Dictionary<string, double> Data { get; set; } = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
        }

        private static PtcDataset? FindPtcDataset(List<PtcDataset> sets, string role, string trendType)
            => sets.FirstOrDefault(x =>
                string.Equals(x.Role, role, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(x.TrendType, trendType, StringComparison.OrdinalIgnoreCase));

        private static bool IsNewGraphInput(string? input)
        {
            if (string.IsNullOrWhiteSpace(input)) return false;
            var s = input.Trim();

            if (s.IndexOf("/smartmap/newgraph/", StringComparison.OrdinalIgnoreCase) >= 0)
                return true;

            if (s.IndexOf("rtu=", StringComparison.OrdinalIgnoreCase) >= 0)
                return true;

            if (PtcRtuKeys.Any(k => string.Equals(k, s, StringComparison.OrdinalIgnoreCase)))
                return true;

            return PtcRtuKeys.Any(k => string.Equals(BuildNewGraphPressureCode(k), s, StringComparison.OrdinalIgnoreCase));
        }

        private static string BuildNewGraphUrl(string rtuOrUrl)
        {
            rtuOrUrl = (rtuOrUrl ?? "").Trim();
            if (string.IsNullOrWhiteSpace(rtuOrUrl)) return "";

            if (Uri.TryCreate(rtuOrUrl, UriKind.Absolute, out var abs))
                return abs.ToString();

            // กรณีส่งมาเป็น relative หรือ query string ที่มี rtu=...
            var rtu = TryGetNewGraphRtuKey(rtuOrUrl);
            if (!string.IsNullOrWhiteSpace(rtu))
                return NewGraphBaseUrl + Uri.EscapeDataString(rtu);

            // กรณีส่งมาเป็น key ตรง ๆ
            return NewGraphBaseUrl + Uri.EscapeDataString(rtuOrUrl);
        }

        private static string BuildNewGraphPressureCode(string rtuKey)
        {
            var rtu = (rtuKey ?? "").Trim();
            if (string.IsNullOrWhiteSpace(rtu)) return rtu;

            // Legacy DB/code convention uses the pressure typename, e.g. UZ5411P.
            // NewGraph URL uses only rtu=UZ5411, so keep URL key and DB code separated.
            return rtu.EndsWith("P", StringComparison.OrdinalIgnoreCase) ? rtu : (rtu + "P");
        }

        private static string? TryGetNewGraphRtuKey(string? input)
        {
            if (string.IsNullOrWhiteSpace(input)) return null;
            var s = input.Trim();

            if (PtcRtuKeys.Any(k => string.Equals(k, s, StringComparison.OrdinalIgnoreCase)))
                return PtcRtuKeys.First(k => string.Equals(k, s, StringComparison.OrdinalIgnoreCase));

            // รองรับกรณี caller ส่ง legacy pressure code เช่น UZ5411P กลับมา
            foreach (var k in PtcRtuKeys)
            {
                if (string.Equals(BuildNewGraphPressureCode(k), s, StringComparison.OrdinalIgnoreCase))
                    return k;
            }

            if (Uri.TryCreate(s, UriKind.Absolute, out var uri))
            {
                var q = ParseQueryLoose(uri);
                if (q.TryGetValue("rtu", out var rtu) && !string.IsNullOrWhiteSpace(rtu))
                    return rtu.Trim();
            }

            // รองรับ string เช่น "index.php?rtu=UZ5411" หรือ "?rtu=UZ5411"
            var m = Regex.Match(s, @"(?:^|[?&])rtu=(?<rtu>[^&\s]+)", RegexOptions.IgnoreCase);
            if (m.Success)
            {
                var rtu = WebUtility.UrlDecode(m.Groups["rtu"].Value ?? "").Trim();
                if (!string.IsNullOrWhiteSpace(rtu)) return rtu;
            }

            return null;
        }

        private static string? ExtractJsConstString(string jsOrHtml, string constName)
        {
            var m = Regex.Match(
                jsOrHtml ?? "",
                @"\bconst\s+" + Regex.Escape(constName) + @"\s*=\s*['""`](?<v>[^'""`]*)['""`]",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);

            return m.Success ? m.Groups["v"].Value : null;
        }

        private static string? ExtractJsStringProperty(string jsObj, string propName)
        {
            var m = Regex.Match(
                jsObj ?? "",
                @"(?:^|[,{]\s*)" + Regex.Escape(propName) + @"\s*:\s*['""`](?<v>[^'""`]*)['""`]",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);

            return m.Success ? m.Groups["v"].Value : null;
        }

        private static double? ExtractJsNumberProperty(string jsObj, string propName)
        {
            var m = Regex.Match(
                jsObj ?? "",
                @"(?:^|[,{]\s*)" + Regex.Escape(propName) + @"\s*:\s*(?<v>[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?)",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);

            if (!m.Success) return null;
            return TryParseDoubleInv(m.Groups["v"].Value, out var v) ? v : null;
        }

        private static string? ExtractJsArrayLiteral(string jsOrHtml, string constName)
        {
            var m = Regex.Match(
                jsOrHtml ?? "",
                @"\bconst\s+" + Regex.Escape(constName) + @"\s*=\s*\[",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);

            if (!m.Success) return null;

            var openIdx = (jsOrHtml ?? "").IndexOf('[', m.Index);
            if (openIdx < 0) return null;

            var closeIdx = FindMatchingToken(jsOrHtml, openIdx, '[', ']');
            if (closeIdx < 0 || closeIdx <= openIdx) return null;

            return jsOrHtml.Substring(openIdx, closeIdx - openIdx + 1);
        }

        private static string? ExtractDataArrayLiteral(string datasetObject)
        {
            var m = Regex.Match(datasetObject ?? "", @"\bdata\s*:", RegexOptions.IgnoreCase);
            if (!m.Success) return null;

            var openIdx = (datasetObject ?? "").IndexOf('[', m.Index);
            if (openIdx < 0) return null;

            var closeIdx = FindMatchingToken(datasetObject, openIdx, '[', ']');
            if (closeIdx < 0 || closeIdx <= openIdx) return null;

            return datasetObject.Substring(openIdx, closeIdx - openIdx + 1);
        }

        private static Dictionary<string, double> ExtractPointDictionary(string datasetObject)
        {
            var result = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);

            var dataArray = ExtractDataArrayLiteral(datasetObject);
            if (string.IsNullOrWhiteSpace(dataArray)) return result;

            var pointObjects = SplitTopLevelObjectsFromArray(dataArray);
            foreach (var point in pointObjects)
            {
                var y = ExtractJsNumberProperty(point, "y");
                if (!y.HasValue) continue;

                var x = ExtractJsStringProperty(point, "x");
                var realX = ExtractJsStringProperty(point, "_real_x");

                var hhmm = ExtractHHmmFromTimestamp(x) ?? ExtractHHmmFromTimestamp(realX);
                if (string.IsNullOrWhiteSpace(hhmm)) continue;

                result[hhmm] = y.Value;
            }

            return result;
        }

        private static string? ExtractHHmmFromTimestamp(string? token)
        {
            if (string.IsNullOrWhiteSpace(token)) return null;

            var m = Regex.Match(token.Trim(), @"(?<h>\d{1,2}):(?<m>\d{2})(?::\d{2})?");
            if (!m.Success) return null;

            var raw = m.Groups["h"].Value + ":" + m.Groups["m"].Value;
            var hhmm = NormalizeHHmm(raw);
            return string.IsNullOrWhiteSpace(hhmm) ? null : hhmm;
        }

        private static List<string> SplitTopLevelObjectsFromArray(string arrayLiteral)
        {
            var result = new List<string>();
            if (string.IsNullOrWhiteSpace(arrayLiteral)) return result;

            var s = arrayLiteral.Trim();
            if (s.StartsWith("[", StringComparison.Ordinal) && s.EndsWith("]", StringComparison.Ordinal))
                s = s.Substring(1, s.Length - 2);

            char quote = '\0';
            var escape = false;
            var lineComment = false;
            var blockComment = false;
            var braceDepth = 0;
            var start = -1;

            for (var i = 0; i < s.Length; i++)
            {
                var c = s[i];
                var next = i + 1 < s.Length ? s[i + 1] : '\0';

                if (lineComment)
                {
                    if (c == '\n' || c == '\r') lineComment = false;
                    continue;
                }

                if (blockComment)
                {
                    if (c == '*' && next == '/')
                    {
                        blockComment = false;
                        i++;
                    }
                    continue;
                }

                if (quote != '\0')
                {
                    if (escape)
                    {
                        escape = false;
                        continue;
                    }
                    if (c == '\\')
                    {
                        escape = true;
                        continue;
                    }
                    if (c == quote)
                    {
                        quote = '\0';
                        continue;
                    }
                    continue;
                }

                if (c == '/' && next == '/')
                {
                    lineComment = true;
                    i++;
                    continue;
                }

                if (c == '/' && next == '*')
                {
                    blockComment = true;
                    i++;
                    continue;
                }

                if (c == '\'' || c == '"' || c == '`')
                {
                    quote = c;
                    continue;
                }

                if (c == '{')
                {
                    if (braceDepth == 0) start = i;
                    braceDepth++;
                    continue;
                }

                if (c == '}')
                {
                    if (braceDepth > 0) braceDepth--;
                    if (braceDepth == 0 && start >= 0)
                    {
                        result.Add(s.Substring(start, i - start + 1));
                        start = -1;
                    }
                }
            }

            return result;
        }

        private static int FindMatchingToken(string s, int openIdx, char openChar, char closeChar)
        {
            if (string.IsNullOrEmpty(s) || openIdx < 0 || openIdx >= s.Length || s[openIdx] != openChar)
                return -1;

            char quote = '\0';
            var escape = false;
            var lineComment = false;
            var blockComment = false;
            var depth = 0;

            for (var i = openIdx; i < s.Length; i++)
            {
                var c = s[i];
                var next = i + 1 < s.Length ? s[i + 1] : '\0';

                if (lineComment)
                {
                    if (c == '\n' || c == '\r') lineComment = false;
                    continue;
                }

                if (blockComment)
                {
                    if (c == '*' && next == '/')
                    {
                        blockComment = false;
                        i++;
                    }
                    continue;
                }

                if (quote != '\0')
                {
                    if (escape)
                    {
                        escape = false;
                        continue;
                    }
                    if (c == '\\')
                    {
                        escape = true;
                        continue;
                    }
                    if (c == quote)
                    {
                        quote = '\0';
                        continue;
                    }
                    continue;
                }

                if (c == '/' && next == '/')
                {
                    lineComment = true;
                    i++;
                    continue;
                }

                if (c == '/' && next == '*')
                {
                    blockComment = true;
                    i++;
                    continue;
                }

                if (c == '\'' || c == '"' || c == '`')
                {
                    quote = c;
                    continue;
                }

                if (c == openChar)
                {
                    depth++;
                    continue;
                }

                if (c == closeChar)
                {
                    depth--;
                    if (depth == 0) return i;
                }
            }

            return -1;
        }

        private static void NormalizeMidnightPoint(Dictionary<string, (double Ucl, double Lcl)> result)
        {
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
        }


        private static void FillMissingHourlyByInterpolation(Dictionary<string, (double Ucl, double Lcl)> result)
        {
            if (result == null || result.Count < 2) return;

            // Build a clean map of whole-hour points only. Non-hour points are ignored for interpolation source,
            // but the caller normally has only HH:00 values after NormalizeMidnightPoint().
            var hourPoints = new SortedDictionary<int, (double Ucl, double Lcl)>();
            foreach (var kv in result)
            {
                if (!TryParseHHmmToMinuteOfDay(kv.Key, out var minuteOfDay)) continue;
                if (minuteOfDay % 60 != 0) continue;

                var hour = minuteOfDay / 60;
                if (hour >= 0 && hour <= 23)
                    hourPoints[hour] = kv.Value;
            }

            if (hourPoints.Count < 2) return;

            for (var h = 0; h < 24; h++)
            {
                var hhmm = $"{h:00}:00";
                if (result.ContainsKey(hhmm)) continue;

                if (!TryFindInterpolationBounds(hourPoints, h, out var leftHour, out var leftValue, out var rightHour, out var rightValue))
                    continue;

                var span = rightHour - leftHour;
                var pos = h - leftHour;

                // Cross-midnight fallback, e.g. left=23, right=0 means right is next-day 24.
                if (span <= 0) span += 24;
                if (pos < 0) pos += 24;
                if (span <= 0) continue;

                var ratio = pos / (double)span;
                var ucl = leftValue.Ucl + ((rightValue.Ucl - leftValue.Ucl) * ratio);
                var lcl = leftValue.Lcl + ((rightValue.Lcl - leftValue.Lcl) * ratio);

                result[hhmm] = (RoundPtcValue(ucl), RoundPtcValue(lcl));
            }
        }

        private static bool TryFindInterpolationBounds(
            SortedDictionary<int, (double Ucl, double Lcl)> hourPoints,
            int targetHour,
            out int leftHour,
            out (double Ucl, double Lcl) leftValue,
            out int rightHour,
            out (double Ucl, double Lcl) rightValue)
        {
            leftHour = -1;
            rightHour = -1;
            leftValue = default;
            rightValue = default;

            if (hourPoints == null || hourPoints.Count < 2) return false;
            if (hourPoints.TryGetValue(targetHour, out var exact))
            {
                leftHour = targetHour;
                rightHour = targetHour;
                leftValue = exact;
                rightValue = exact;
                return true;
            }

            // Prefer normal same-day bounds: previous existing hour and next existing hour.
            var prev = hourPoints.Keys.Where(h => h < targetHour).DefaultIfEmpty(-1).Max();
            var next = hourPoints.Keys.Where(h => h > targetHour).DefaultIfEmpty(-1).Min();

            if (prev >= 0 && next >= 0)
            {
                leftHour = prev;
                rightHour = next;
                leftValue = hourPoints[prev];
                rightValue = hourPoints[next];
                return true;
            }

            // Edge fallback: wrap around midnight.
            if (prev < 0)
            {
                prev = hourPoints.Keys.Max();
                next = hourPoints.Keys.Where(h => h > targetHour).DefaultIfEmpty(hourPoints.Keys.Min()).Min();
            }
            else if (next < 0)
            {
                next = hourPoints.Keys.Min();
            }

            if (prev < 0 || next < 0 || prev == next) return false;

            leftHour = prev;
            rightHour = next;
            leftValue = hourPoints[prev];
            rightValue = hourPoints[next];
            return true;
        }

        private static bool TryParseHHmmToMinuteOfDay(string hhmm, out int minuteOfDay)
        {
            minuteOfDay = -1;
            if (string.IsNullOrWhiteSpace(hhmm)) return false;

            if (!TimeSpan.TryParseExact(
                    hhmm.Trim(),
                    new[] { @"h\:mm", @"hh\:mm", @"h\:mm\:ss", @"hh\:mm\:ss" },
                    CultureInfo.InvariantCulture,
                    out var ts))
            {
                return false;
            }

            if (ts.TotalMinutes < 0 || ts.TotalMinutes >= 24 * 60) return false;
            minuteOfDay = ((int)ts.TotalHours * 60) + ts.Minutes;
            return true;
        }

        private static double RoundPtcValue(double v)
            => Math.Round(v, 3, MidpointRounding.AwayFromZero);

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

            // NewGraph returns a complete PTC set per RTU/code.
            // Delete old rows first so stale minute-expanded rows or old 23:59/01:00 rows do not remain.
            using (var del = conn.CreateCommand())
            {
                del.Transaction = tx;
                del.CommandText = "DELETE FROM upper_lower WHERE code = $code;";
                var pDelCode = del.CreateParameter();
                pDelCode.ParameterName = "$code";
                pDelCode.Value = code.Trim();
                del.Parameters.Add(pDelCode);
                del.ExecuteNonQuery();
            }

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
