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

public static class OnlineLabHandlers
{
    private static readonly JsonSerializerOptions JsonOpt = new(JsonSerializerDefaults.Web);

    // ts format in DB (string sortable)
    private const string TsFormat = "yyyy-MM-dd HH:mm:ss";

    // ===== DTO =====
    public sealed class OnlineLabReq
    {
        public int? HourWindow { get; set; }                     // เช่น 4
        public List<OnlineLabSourceReq>? Sources { get; set; }   // list ของ sources
    }

    public sealed class OnlineLabSourceReq
    {
        public string? Source { get; set; }      // "TW1", "RW2"
        public List<string>? Keys { get; set; }  // keys ที่อยากได้
    }

    // ===== Response models =====
    public sealed record PointDto(string ts, double value);

    // ===== Allowed keys per source =====
    private static readonly Dictionary<string, HashSet<string>> AllowedKeysBySource =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["TW1"] = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            { "Post_Chlor", "Post_Chlor_ParaMax", "Post_Chlor_ParaMin" },
            ["TW2"] = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            { "Post_Chlor", "Post_Chlor_ParaMax", "Post_Chlor_ParaMin" },
            ["TW3"] = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            { "Post_Chlor", "Post_Chlor_ParaMax", "Post_Chlor_ParaMin" },
            ["TW4"] = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            { "Post_Chlor", "Post_Chlor_ParaMax", "Post_Chlor_ParaMin" },

            ["RW2"] = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            {
                "NTU", "NTU_ParaMax",
                "Cond",
                "DO", "DO_ParaMin",
                "Temp"
            }
        };

    // ===== Default keys per source =====
    private static readonly Dictionary<string, string[]> DefaultKeysBySource =
        new(StringComparer.OrdinalIgnoreCase)
        {
            ["TW1"] = new[] { "Post_Chlor", "Post_Chlor_ParaMax", "Post_Chlor_ParaMin" },
            ["TW2"] = new[] { "Post_Chlor", "Post_Chlor_ParaMax", "Post_Chlor_ParaMin" },
            ["TW3"] = new[] { "Post_Chlor", "Post_Chlor_ParaMax", "Post_Chlor_ParaMin" },
            ["TW4"] = new[] { "Post_Chlor", "Post_Chlor_ParaMax", "Post_Chlor_ParaMin" },

            ["RW2"] = new[] { "NTU", "NTU_ParaMax", "Cond", "DO", "DO_ParaMin", "Temp" }
        };

    // ===== Public: preflight =====
    public static void WritePreflight(HttpListenerContext hc)
    {
        var res = hc.Response;
        ApplyCors(res);

        res.StatusCode = 204;
        res.ContentType = "text/plain; charset=utf-8";
        res.ContentLength64 = 0;
        try { res.OutputStream.Close(); } catch { }
    }

    // ===== Public handler =====
    public static async Task HandleOnlineLabAsync(HttpListenerContext hc, EngineContext ctx, CancellationToken ct)
    {
        try
        {
            var req = hc.Request;

            // read body
            var body = await ReadBodyAsync(req, ct).ConfigureAwait(false);

            OnlineLabReq dto;
            try { dto = JsonSerializer.Deserialize<OnlineLabReq>(body, JsonOpt) ?? new OnlineLabReq(); }
            catch { dto = new OnlineLabReq(); }

            // normalize hourWindow
            var hourWindow = dto.HourWindow.GetValueOrDefault(4);
            if (hourWindow < 1) hourWindow = 1;
            if (hourWindow > 48) hourWindow = 48;

            // 5 นาที/จุด => 12 จุด/ชั่วโมง
            var limit = hourWindow * 12;

            // sources default: TW1..TW4
            var sources = (dto.Sources is { Count: > 0 })
                ? dto.Sources
                : new List<OnlineLabSourceReq>
                {
                    new() { Source = "TW1", Keys = new List<string>{ "Post_Chlor","Post_Chlor_ParaMax","Post_Chlor_ParaMin" } },
                    new() { Source = "TW2", Keys = new List<string>{ "Post_Chlor","Post_Chlor_ParaMax","Post_Chlor_ParaMin" } },
                    new() { Source = "TW3", Keys = new List<string>{ "Post_Chlor","Post_Chlor_ParaMax","Post_Chlor_ParaMin" } },
                    new() { Source = "TW4", Keys = new List<string>{ "Post_Chlor","Post_Chlor_ParaMax","Post_Chlor_ParaMin" } },
                };

            // normalize + allowlist + auto default keys
            for (int i = 0; i < sources.Count; i++)
                sources[i] = NormalizeSourceReq(sources[i]);

            // db path
            var dbPath = Path.Combine(AppContext.BaseDirectory, "data.db");
            if (!File.Exists(dbPath))
            {
                await WriteJsonAsync(hc, 500, new
                {
                    ok = false,
                    error = "data.db not found",
                    dbPath,
                    serverTsUtc = ctx.Clock.UtcNow
                }).ConfigureAwait(false);
                return;
            }

            var graphs = new Dictionary<string, Dictionary<string, List<PointDto>>>(StringComparer.OrdinalIgnoreCase);

            await using var conn = NewConn(dbPath);
            await conn.OpenAsync(ct).ConfigureAwait(false);

            foreach (var s in sources)
            {
                if (string.IsNullOrWhiteSpace(s.Source)) continue;
                if (s.Keys is null || s.Keys.Count == 0) continue;

                // ✅ anchorTs = latest ts in DB for this source (not DateTime.Now)
                // ถ้า anchor หาไม่ได้ -> fromTs = null (fallback: no time filter)
                var anchorTs = await QueryLatestTsForSourceAsync(conn, s.Source!, ct).ConfigureAwait(false);
                string? fromTs = null;

                if (anchorTs is not null &&
                    DateTime.TryParseExact(anchorTs, TsFormat, CultureInfo.InvariantCulture,
                        DateTimeStyles.None, out var anchorDt))
                {
                    fromTs = anchorDt.AddHours(-hourWindow).ToString(TsFormat);
                }

                var perKey = new Dictionary<string, List<PointDto>>(StringComparer.OrdinalIgnoreCase);

                foreach (var key in s.Keys)
                {
                    if (AllowedKeysBySource.TryGetValue(s.Source!, out var allowed) && !allowed.Contains(key))
                        continue;

                    // ✅ ถ้า fromTs แล้วได้ 0 แถว -> fallback ดึงแบบไม่มี fromTs (กันกราฟพัง)
                    var pts = await QuerySeriesAsync(conn, s.Source!, key, limit, fromTs, ct).ConfigureAwait(false);
                    if (pts.Count == 0 && fromTs is not null)
                    {
                        pts = await QuerySeriesAsync(conn, s.Source!, key, limit, null, ct).ConfigureAwait(false);
                    }

                    perKey[key] = pts; // DESC ล่าสุด -> เก่า (หน้า reverse เอง)
                }

                graphs[s.Source!] = perKey;
            }

            await WriteJsonAsync(hc, 200, new
            {
                ok = true,
                hourWindow,
                limit,
                serverTsUtc = ctx.Clock.UtcNow,
                graphs
            }).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            ctx.Log.Error(ex, "[online_lab] handler error");
            try
            {
                await WriteJsonAsync(hc, 500, new
                {
                    ok = false,
                    error = "Internal error",
                    serverTsUtc = ctx.Clock.UtcNow
                }).ConfigureAwait(false);
            }
            catch { }
        }
    }

    // ===== Normalize per source =====
    private static OnlineLabSourceReq NormalizeSourceReq(OnlineLabSourceReq s)
    {
        s.Source = (s.Source ?? "").Trim().ToUpperInvariant();
        if (string.IsNullOrWhiteSpace(s.Source))
        {
            s.Keys = new List<string>();
            return s;
        }

        var keys = (s.Keys is { Count: > 0 })
            ? s.Keys
            : (DefaultKeysBySource.TryGetValue(s.Source, out var def) ? def.ToList() : new List<string>());

        keys = keys
            .Where(k => !string.IsNullOrWhiteSpace(k))
            .Select(k => k.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToList();

        if (AllowedKeysBySource.TryGetValue(s.Source, out var allowed))
            keys = keys.Where(allowed.Contains).ToList();

        s.Keys = keys;
        return s;
    }

    // ===== Anchor: latest ts per source =====
    private static async Task<string?> QueryLatestTsForSourceAsync(SqliteConnection conn, string source, CancellationToken ct)
    {
        // เร็วสุด: หา ts ล่าสุดของ source จากทุก key
        const string sql = @"
SELECT ts
FROM OneValueSeries
WHERE source = $source
ORDER BY ts DESC
LIMIT 1;
";
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("$source", source);

        var obj = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        var ts = obj?.ToString();
        return string.IsNullOrWhiteSpace(ts) ? null : ts;
    }

    // ===== Query series (optional time filter) =====
    private static async Task<List<PointDto>> QuerySeriesAsync(
        SqliteConnection conn,
        string source,
        string key,
        int limit,
        string? fromTs,
        CancellationToken ct)
    {
        const string sql = @"
SELECT ts, value
FROM OneValueSeries
WHERE source = $source
  AND key    = $key
  AND ($fromTs IS NULL OR ts >= $fromTs)
ORDER BY ts DESC
LIMIT $limit;
";
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        cmd.Parameters.AddWithValue("$source", source);
        cmd.Parameters.AddWithValue("$key", key);
        cmd.Parameters.AddWithValue("$limit", limit);
        cmd.Parameters.AddWithValue("$fromTs", (object?)fromTs ?? DBNull.Value);

        var list = new List<PointDto>(capacity: Math.Clamp(limit, 8, 4096));

        await using var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await r.ReadAsync(ct).ConfigureAwait(false))
        {
            var ts = r.IsDBNull(0) ? "" : r.GetString(0);
            if (string.IsNullOrWhiteSpace(ts)) continue;

            if (r.IsDBNull(1)) continue;

            double v;
            try { v = r.GetDouble(1); }
            catch
            {
                var sVal = r.GetValue(1)?.ToString();
                if (!double.TryParse(sVal, NumberStyles.Float, CultureInfo.InvariantCulture, out v) &&
                    !double.TryParse(sVal, NumberStyles.Float, CultureInfo.CurrentCulture, out v))
                    continue;
            }

            if (!double.IsFinite(v)) continue;

            list.Add(new PointDto(ts, v));
        }

        return list; // DESC ล่าสุด -> เก่า
    }

    // ===== DB =====
    private static SqliteConnection NewConn(string dbPath)
    {
        var cs = new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadOnly,
            Cache = SqliteCacheMode.Shared,
            Pooling = true
        }.ToString();
        return new SqliteConnection(cs);
    }

    // ===== Helpers =====
    private static async Task<string> ReadBodyAsync(HttpListenerRequest req, CancellationToken ct)
    {
        if (!req.HasEntityBody) return "{}";
        using var sr = new StreamReader(req.InputStream, req.ContentEncoding ?? Encoding.UTF8);
        var s = await sr.ReadToEndAsync().ConfigureAwait(false);
        return string.IsNullOrWhiteSpace(s) ? "{}" : s;
    }

    private static void ApplyCors(HttpListenerResponse res)
    {
        res.Headers["Access-Control-Allow-Origin"] = "*";
        res.Headers["Access-Control-Allow-Methods"] = "POST,OPTIONS";
        res.Headers["Access-Control-Allow-Headers"] = "Content-Type, Accept";
        res.Headers["Access-Control-Max-Age"] = "600";
    }

    private static async Task WriteJsonAsync(HttpListenerContext hc, int statusCode, object payload)
    {
        var res = hc.Response;

        ApplyCors(res);
        res.StatusCode = statusCode;
        res.ContentType = "application/json; charset=utf-8";

        var json = JsonSerializer.Serialize(payload, JsonOpt);
        var bytes = Encoding.UTF8.GetBytes(json);

        res.ContentLength64 = bytes.Length;
        await res.OutputStream.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false);
        try { res.OutputStream.Close(); } catch { }
    }
}