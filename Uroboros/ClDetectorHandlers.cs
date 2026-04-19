#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace Uroboros
{
    /// <summary>
    /// อ่านค่า chlorine detector / sensor จาก metrics_current เท่านั้น
    /// ตัด AQ_readings_narrow_v2 ออก เพื่อให้เบาและตรงงาน cl detector โดยเฉพาะ
    /// </summary>
    public static class ClDetectorHandlers
    {
        // metrics_current keys ที่จำเป็นสำหรับงาน cl detector
        private static readonly (string stream, string param)[] ClDetectorMetricKeys = new[]
        {
            ("CHEM1", "cl_D1"),
            ("CHEM1", "cl_D2"),
            ("CHEM1", "cl_S1"),
            ("CHEM1", "cl_S2"),
            ("CHEM1", "cl_S3"),
            ("CHEM1", "cl_S4"),
            ("CHEM2", "cl_D1"),
            ("CHEM2", "cl_D2"),
            ("CHEM2", "cl_S1"),
            ("CHEM2", "cl_S2"),
            ("CHEM2", "cl_S3"),
            ("CHEM2", "cl_S4"),
        };

        public static async Task HandleClDetectorSummaryAsync(HttpListenerContext hc, EngineContext ctx, CancellationToken ct)
        {
            var resp = hc.Response;

            try
            {
                resp.StatusCode = 200;
                resp.ContentType = "application/json; charset=utf-8";
                resp.Headers["Access-Control-Allow-Origin"] = "*";
                resp.Headers["Cache-Control"] = "no-store";

                var dbPath = ResolveDbPath(ctx);
                if (!System.IO.File.Exists(dbPath))
                    throw new InvalidOperationException($"db not found: {dbPath}");

                var nowLocal = DateTime.Now;

                await using var conn = new SqliteConnection($"Data Source={dbPath};Mode=ReadOnly;Cache=Shared;Pooling=True;");
                await conn.OpenAsync(ct).ConfigureAwait(false);

                var m = await ReadMetricsCurrentAsync(conn, ClDetectorMetricKeys, ct).ConfigureAwait(false);

                var payload = new
                {
                    ok = true,

                    // grouped by stream => { "CHEM1": { "cl_D1": ..., "cl_S1": ... }, "CHEM2": ... }
                    metrics_current = m.ByStream,

                    // flat map => { "CHEM1.cl_D1": ..., "CHEM1.cl_S1": ..., ... }
                    flat = m.Flat,

                    TsUtc = m.TsUtc,
                    ServerTsLocal = nowLocal.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
                    ServerTsUtc = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
                };

                await WriteJsonAsync(resp, payload, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // ignore
            }
            catch (Exception ex)
            {
                try { ctx.Log.Warn($"[CL DETECTOR] /api/cldetector/summary error: {ex.Message}"); } catch { }
                WriteJsonError(resp, 500, "cl_detector_summary_error", ex.Message);
            }
            finally
            {
                try { resp.OutputStream.Close(); } catch { }
            }
        }

        // ===================== helpers =====================

        private static string ResolveDbPath(EngineContext ctx)
        {
            var baseDir = AppContext.BaseDirectory;
            return System.IO.Path.Combine(baseDir, "data.db");
        }

        private sealed class MetricsRead
        {
            public Dictionary<string, Dictionary<string, double?>> ByStream { get; } =
                new(StringComparer.OrdinalIgnoreCase);

            public Dictionary<string, double?> Flat { get; } =
                new(StringComparer.OrdinalIgnoreCase);

            public string? TsUtc { get; set; }

            public void Set(string stream, string param, double? value)
            {
                if (!ByStream.TryGetValue(stream, out var map))
                {
                    map = new Dictionary<string, double?>(StringComparer.OrdinalIgnoreCase);
                    ByStream[stream] = map;
                }

                map[param] = value;
                Flat[$"{stream}.{param}"] = value;
            }
        }

        private static async Task<MetricsRead> ReadMetricsCurrentAsync(
            SqliteConnection conn,
            (string stream, string param)[] keys,
            CancellationToken ct)
        {
            var res = new MetricsRead();

            await using var cmd = conn.CreateCommand();

            var ors = new List<string>(keys.Length);
            for (int i = 0; i < keys.Length; i++)
            {
                var sn = $"@s{i}";
                var pn = $"@p{i}";
                ors.Add($"(stream = {sn} AND param = {pn})");
                cmd.Parameters.AddWithValue(sn, keys[i].stream);
                cmd.Parameters.AddWithValue(pn, keys[i].param);
            }

            cmd.CommandText = $@"
SELECT stream, param, value_real, value_text, ts_utc
FROM metrics_current
WHERE {string.Join(" OR ", ors)};
";

            await using var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                var stream = r.GetString(0);
                var param = r.GetString(1);

                double? v = null;
                if (!r.IsDBNull(2))
                {
                    v = r.GetDouble(2);
                }
                else if (!r.IsDBNull(3))
                {
                    var s = r.GetString(3);
                    if (double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var dv))
                        v = dv;
                    else if (double.TryParse(s, NumberStyles.Float, CultureInfo.GetCultureInfo("th-TH"), out dv))
                        v = dv;
                }

                res.Set(stream, param, v);

                var ts = r.IsDBNull(4) ? null : r.GetString(4);
                if (!string.IsNullOrWhiteSpace(ts))
                {
                    if (res.TsUtc == null || string.CompareOrdinal(ts, res.TsUtc) > 0)
                        res.TsUtc = ts;
                }
            }

            // เติม key ที่ไม่มีใน DB ให้ครบเป็น null เพื่อให้ frontend ใช้ง่าย
            foreach (var key in keys)
            {
                if (!res.ByStream.TryGetValue(key.stream, out var map))
                {
                    map = new Dictionary<string, double?>(StringComparer.OrdinalIgnoreCase);
                    res.ByStream[key.stream] = map;
                }

                if (!map.ContainsKey(key.param))
                    map[key.param] = null;

                var flatKey = $"{key.stream}.{key.param}";
                if (!res.Flat.ContainsKey(flatKey))
                    res.Flat[flatKey] = null;
            }

            return res;
        }

        private static async Task WriteJsonAsync(HttpListenerResponse resp, object payload, CancellationToken ct)
        {
            var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions
            {
                PropertyNamingPolicy = null,
                WriteIndented = false
            });

            var bytes = Encoding.UTF8.GetBytes(json);
            resp.ContentLength64 = bytes.Length;
            await resp.OutputStream.WriteAsync(bytes, 0, bytes.Length, ct).ConfigureAwait(false);
        }

        private static void WriteJsonError(HttpListenerResponse resp, int status, string code, string message)
        {
            try
            {
                resp.StatusCode = status;
                resp.ContentType = "application/json; charset=utf-8";
                resp.Headers["Access-Control-Allow-Origin"] = "*";
                resp.Headers["Cache-Control"] = "no-store";

                var payload = new { ok = false, code, message };
                var json = JsonSerializer.Serialize(payload);
                var bytes = Encoding.UTF8.GetBytes(json);
                resp.ContentLength64 = bytes.Length;
                resp.OutputStream.Write(bytes, 0, bytes.Length);
            }
            catch { }
        }
    }
}
