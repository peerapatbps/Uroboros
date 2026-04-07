using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Uroboros
{
    public sealed class AquadatRemarkHelper
    {
        private readonly AquadatFastCli.AquadatFastQuery _aq;
        private readonly HttpClient _http;
        private readonly string _dbPath;

        private const string GraphqlUrl = "https://aquadat.mwa.co.th/graphql";

        public AquadatRemarkHelper(
            AquadatFastCli.AquadatFastQuery aq,
            string dbPath = "data.db",
            HttpClient? http = null)
        {
            _aq = aq ?? throw new ArgumentNullException(nameof(aq));
            _http = http ?? new HttpClient { Timeout = TimeSpan.FromSeconds(60) };
            _dbPath = string.IsNullOrWhiteSpace(dbPath) ? "data.db" : dbPath;
        }

        public sealed class RemarkFetchRequest
        {
            public int PlantId { get; set; } = 2;
            public int StationId { get; set; }
            public string DateYmd8 { get; set; } = "";
            public bool LoadFileData { get; set; } = true;
            public string TransType { get; set; } = "";
        }

        public sealed class RemarkItem
        {
            public int TransRemarkId { get; set; }
            public int PlantId { get; set; }
            public int StationId { get; set; }
            public string StationNameTh { get; set; } = "";
            public string DescriptionRaw { get; set; } = "";
            public RemarkParseResult Parsed { get; set; } = new();
        }

        public sealed class RemarkParseResult
        {
            public Dictionary<string, List<string>> Values { get; set; } = new(StringComparer.Ordinal)
            {
                ["Rec"] = new List<string>(),
                ["Ret"] = new List<string>(),
                ["Unl"] = new List<string>(),
                ["Tiu"] = new List<string>()
            };
        }

        public sealed class MultiStationRemarkSummary
        {
            public string DateYmd8 { get; set; } = "";
            public List<RemarkItem> Items { get; set; } = new();
        }

        public static void Initialize(string dbPath = "data.db")
        {
            EnsureWal(dbPath);

            using var conn = new SqliteConnection($"Data Source={dbPath};Cache=Shared;Pooling=True;");
            conn.Open();

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"DROP TABLE IF EXISTS AQ_transaction_remark_summary;";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS AQ_transaction_remark_detail(
    work_date_ymd8   TEXT    NOT NULL,
    plant_id         INTEGER NOT NULL,
    station_id       INTEGER NOT NULL,
    trans_remark_id  INTEGER NOT NULL DEFAULT 0,
    type             TEXT    NOT NULL,   -- Rec / Ret / Unl / Tiu
    value            TEXT    NOT NULL,   -- เช่น 5819
    updated_at       TEXT    NOT NULL DEFAULT (datetime('now', '+7 hours'))
);";
                cmd.ExecuteNonQuery();
            }

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = @"
CREATE INDEX IF NOT EXISTS idx_remark_detail_date_station_type
ON AQ_transaction_remark_detail(work_date_ymd8, station_id, type);

CREATE INDEX IF NOT EXISTS idx_remark_detail_value
ON AQ_transaction_remark_detail(value);";
                cmd.ExecuteNonQuery();
            }
        }

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

        public static async Task<int> ClearAllAsync(string dbPath = "data.db", CancellationToken ct = default)
        {
            await using var conn = new SqliteConnection($"Data Source={dbPath};Cache=Shared;Pooling=True;");
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var tx = conn.BeginTransaction();

            int affected = 0;

            await using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tx;
                cmd.CommandText = "DELETE FROM AQ_transaction_remark_detail;";
                affected += await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            await tx.CommitAsync(ct).ConfigureAwait(false);
            return affected;
        }

        public async Task<RemarkItem?> FetchLatestRemarkAsync(RemarkFetchRequest req, CancellationToken ct = default)
        {
            var token = await _aq.LoginToAPI(ct).ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(token))
                throw new InvalidOperationException("LoginToAPI failed. Token is empty.");

            var payload = BuildGraphqlPayload(req);

            using var httpReq = new HttpRequestMessage(HttpMethod.Post, GraphqlUrl);
            httpReq.Headers.Authorization =
                new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);
            httpReq.Content = new StringContent(payload, Encoding.UTF8, "application/json");

            using var resp = await _http.SendAsync(httpReq, ct).ConfigureAwait(false);
            var body = await resp.Content.ReadAsStringAsync(ct).ConfigureAwait(false);

            if (!resp.IsSuccessStatusCode)
                throw new InvalidOperationException($"GraphQL failed: {(int)resp.StatusCode} {resp.ReasonPhrase}\n{body}");

            using var doc = JsonDocument.Parse(body);
            var root = doc.RootElement;

            if (!root.TryGetProperty("data", out var dataEl) || dataEl.ValueKind != JsonValueKind.Object)
                return null;

            if (!dataEl.TryGetProperty("get_transaction_remark", out var arrEl) || arrEl.ValueKind != JsonValueKind.Array)
                return null;

            JsonElement? picked = null;
            foreach (var el in arrEl.EnumerateArray())
            {
                picked = el;
                break;
            }

            if (picked is null) return null;

            var item = new RemarkItem
            {
                TransRemarkId = GetInt(picked.Value, "trans_remark_id"),
                PlantId = GetInt(picked.Value, "plant_id"),
                StationId = GetInt(picked.Value, "station_id"),
                StationNameTh = GetString(picked.Value, "station_name_th"),
                DescriptionRaw = GetString(picked.Value, "description")
            };

            item.Parsed = ParseRemarkDescription(item.DescriptionRaw);
            return item;
        }

        public async Task<MultiStationRemarkSummary> FetchLatestRemarksForCdsAsync(string dateYmd8, CancellationToken ct = default)
        {
            var result = new MultiStationRemarkSummary
            {
                DateYmd8 = dateYmd8
            };

            var a = await FetchLatestRemarkAsync(new RemarkFetchRequest
            {
                PlantId = 2,
                StationId = 15,
                DateYmd8 = dateYmd8,
                LoadFileData = true
            }, ct).ConfigureAwait(false);

            if (a != null) result.Items.Add(a);

            var b = await FetchLatestRemarkAsync(new RemarkFetchRequest
            {
                PlantId = 2,
                StationId = 62,
                DateYmd8 = dateYmd8,
                LoadFileData = true
            }, ct).ConfigureAwait(false);

            if (b != null) result.Items.Add(b);

            return result;
        }

        public static string NormalizeDescription(string? rawDescription)
        {
            return (rawDescription ?? "")
                .Replace("|||||", "\n")
                .Replace("|", "\n")
                .Replace("\r", "")
                .Replace("Does", "Dose", StringComparison.OrdinalIgnoreCase)
                .Trim();
        }

        public static List<string> Extract4DigitNumbers(string? text)
        {
            if (string.IsNullOrWhiteSpace(text))
                return new List<string>();

            var matches = Regex.Matches(text, @"(?<!\d)\d{4}(?!\d)", RegexOptions.CultureInvariant);
            return matches.Cast<Match>().Select(m => m.Value).ToList();
        }

        public static RemarkParseResult ParseRemarkDescription(string? description)
        {
            var normalized = NormalizeDescription(description);

            var lines = normalized
                .Split('\n', StringSplitOptions.RemoveEmptyEntries)
                .Select(x => x.Trim())
                .Where(x => !string.IsNullOrWhiteSpace(x))
                .ToList();

            var result = new RemarkParseResult();

            foreach (var line in lines)
            {
                var numbers = Extract4DigitNumbers(line);

                if (line.Contains("รับ", StringComparison.Ordinal))
                    result.Values["Rec"].AddRange(numbers);

                if (line.Contains("คืน", StringComparison.Ordinal))
                    result.Values["Ret"].AddRange(numbers);

                if (line.Contains("ลง", StringComparison.Ordinal))
                    result.Values["Unl"].AddRange(numbers);

                if (line.Contains("ขึ้น", StringComparison.Ordinal))
                    result.Values["Tiu"].AddRange(numbers);
            }

            return result;
        }

        public async Task<int> ReplaceAllAsync(MultiStationRemarkSummary multi, CancellationToken ct = default)
        {
            Initialize(_dbPath);

            await using var conn = new SqliteConnection($"Data Source={_dbPath};Cache=Shared;Pooling=True;");
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var tx = conn.BeginTransaction();

            int affected = 0;

            await using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tx;
                cmd.CommandText = "DELETE FROM AQ_transaction_remark_detail;";
                affected += await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            const string sql = @"
INSERT INTO AQ_transaction_remark_detail
(
    work_date_ymd8,
    plant_id,
    station_id,
    trans_remark_id,
    type,
    value,
    updated_at
)
VALUES
(
    $work_date_ymd8,
    $plant_id,
    $station_id,
    $trans_remark_id,
    $type,
    $value,
    datetime('now', '+7 hours')
);";

            await using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tx;
                cmd.CommandText = sql;

                var pDate = cmd.CreateParameter(); pDate.ParameterName = "$work_date_ymd8"; cmd.Parameters.Add(pDate);
                var pPlant = cmd.CreateParameter(); pPlant.ParameterName = "$plant_id"; cmd.Parameters.Add(pPlant);
                var pStation = cmd.CreateParameter(); pStation.ParameterName = "$station_id"; cmd.Parameters.Add(pStation);
                var pRemarkId = cmd.CreateParameter(); pRemarkId.ParameterName = "$trans_remark_id"; cmd.Parameters.Add(pRemarkId);
                var pType = cmd.CreateParameter(); pType.ParameterName = "$type"; cmd.Parameters.Add(pType);
                var pValue = cmd.CreateParameter(); pValue.ParameterName = "$value"; cmd.Parameters.Add(pValue);

                foreach (var item in multi.Items)
                {
                    ct.ThrowIfCancellationRequested();

                    affected += await InsertTypeValuesAsync(cmd, multi.DateYmd8, item, "Rec", item.Parsed.Values["Rec"], ct).ConfigureAwait(false);
                    affected += await InsertTypeValuesAsync(cmd, multi.DateYmd8, item, "Ret", item.Parsed.Values["Ret"], ct).ConfigureAwait(false);
                    affected += await InsertTypeValuesAsync(cmd, multi.DateYmd8, item, "Unl", item.Parsed.Values["Unl"], ct).ConfigureAwait(false);
                    affected += await InsertTypeValuesAsync(cmd, multi.DateYmd8, item, "Tiu", item.Parsed.Values["Tiu"], ct).ConfigureAwait(false);
                }
            }

            await tx.CommitAsync(ct).ConfigureAwait(false);
            return affected;
        }

        private static async Task<int> InsertTypeValuesAsync(
            SqliteCommand cmd,
            string dateYmd8,
            RemarkItem item,
            string type,
            List<string> values,
            CancellationToken ct)
        {
            int affected = 0;

            foreach (var value in values)
            {
                ct.ThrowIfCancellationRequested();

                cmd.Parameters["$work_date_ymd8"].Value = dateYmd8;
                cmd.Parameters["$plant_id"].Value = item.PlantId;
                cmd.Parameters["$station_id"].Value = item.StationId;
                cmd.Parameters["$trans_remark_id"].Value = item.TransRemarkId;
                cmd.Parameters["$type"].Value = type;
                cmd.Parameters["$value"].Value = value;

                affected += await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            return affected;
        }

        public async Task<int> RefreshCdsRemarksAsync(string dateYmd8, CancellationToken ct = default)
        {
            Initialize(_dbPath);
            var multi = await FetchLatestRemarksForCdsAsync(dateYmd8, ct).ConfigureAwait(false);
            return await ReplaceAllAsync(multi, ct).ConfigureAwait(false);
        }

        private static string BuildGraphqlPayload(RemarkFetchRequest req)
        {
            var query = $@"
{{
  get_transaction_remark(
    trans_remark_id: 0,
    user_id: 0,
    plant_id: {req.PlantId},
    station_id: {req.StationId},
    trans_remark_date: ""{req.DateYmd8}"",
    trans_remark_date_end: ""{req.DateYmd8}"",
    trans_type: ""{EscapeGraphql(req.TransType ?? "")}"",
    load_file_data: {(req.LoadFileData ? "true" : "false")}
  ) {{
    trans_remark_id
    plant_id
    station_id
    station_name_th
    description
  }}
}}";

            var payload = new
            {
                operationName = (string?)null,
                variables = new { },
                query
            };

            return JsonSerializer.Serialize(payload);
        }

        private static string EscapeGraphql(string s)
        {
            return s.Replace("\\", "\\\\").Replace("\"", "\\\"");
        }

        private static string GetString(JsonElement el, string name)
        {
            if (!el.TryGetProperty(name, out var p)) return "";
            return p.ValueKind switch
            {
                JsonValueKind.String => p.GetString() ?? "",
                JsonValueKind.Number => p.ToString(),
                JsonValueKind.True => "true",
                JsonValueKind.False => "false",
                _ => p.ToString()
            };
        }

        private static int GetInt(JsonElement el, string name)
        {
            if (!el.TryGetProperty(name, out var p)) return 0;
            if (p.ValueKind == JsonValueKind.Number && p.TryGetInt32(out var n)) return n;

            var s = GetString(el, name);
            return int.TryParse(s, NumberStyles.Integer, CultureInfo.InvariantCulture, out n) ? n : 0;
        }
    }
}