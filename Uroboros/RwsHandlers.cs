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
    public static class RwsHandlers
    {
        // ----------------------------
        // AQ configIDs (pump status)
        // ----------------------------

        // RWS1: 1P01A(10), 1P01B(15), 1P01C(20), 1P02A(25), 1P02B(29)
        // RWS2: Pump1(46), Pump2(52), Pump3(58), Pump4(63), Pump5(69)
        private static readonly int[] PumpCfgIds = new[] { 10, 15, 20, 25, 29, 46, 52, 58, 63, 69 };

        // mapping สำหรับ frontend data-pump
        private static readonly Dictionary<int, string> PumpCfgToName = new()
        {
            { 10, "1P01A" },
            { 15, "1P01B" },
            { 20, "1P01C" },
            { 25, "1P02A" },
            { 29, "1P02B" },

            { 46, "Pump1" },
            { 52, "Pump2" },
            { 58, "Pump3" },
            { 63, "Pump4" },
            { 69, "Pump5" },
        };

        // ----------------------------
        // metrics_current params
        // ----------------------------
        // stream like '%RWP%' and param IN (...)
        private static readonly string[] RwpParams = new[]
        {
            "rwp1_flowp1", "rwp1_flowp2", "rwp1_sumflowp1", "rwp1_sumflowp2",
            "rwp2_flowp3", "rwp2_flowp4", "rwp2_sumflowp3", "rwp2_sumflowp4"
        };

        public static void WritePreflight(HttpListenerContext hc)
        {
            var resp = hc.Response;
            resp.StatusCode = 204;
            resp.Headers["Access-Control-Allow-Origin"] = "*";
            resp.Headers["Access-Control-Allow-Methods"] = "GET, OPTIONS";
            resp.Headers["Access-Control-Allow-Headers"] = "Content-Type";
            resp.Headers["Access-Control-Max-Age"] = "600";
            resp.Headers["Cache-Control"] = "no-store";
            resp.OutputStream.Close();
        }

        // NOTE: คุณเรียกผิดชื่อใน router (HandleTpsSummaryAsync) -> ใช้ชื่อ HandleRwsSummaryAsync แล้วไปแก้ router ให้ตรง
        public static async Task HandleRwsSummaryAsync(HttpListenerContext hc, EngineContext ctx, CancellationToken ct)
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
                var targetHour = nowLocal.Hour; // AQ pick HH:00:00
                await using var conn = new SqliteConnection($"Data Source={dbPath};Mode=ReadOnly;Cache=Shared;Pooling=True;");
                await conn.OpenAsync(ct).ConfigureAwait(false);

                // 1) metrics_current: RWP metrics (flow/sumflow)
                var m = await ReadRwpMetricsAsync(conn, RwpParams, ct).ConfigureAwait(false);

                // 2) Pump statuses from AQ_readings_narrow_v2 (by hour, ignore date)
                var aqByCfg = await ReadAqByHourAsync(conn, PumpCfgIds, targetHour, ct).ConfigureAwait(false);
                var pumps = BuildPumpStatusMap(aqByCfg);

                // 3) TsUtc: max(ts_utc) จาก metrics_current ที่อ่านมา
                var tsUtc = m.TsUtc;

                // payload (ชื่อคีย์ให้สอดคล้องกับ frontend ที่คุณจะ map เอง)
                var payload = new
                {
                    // raw metrics_current by param name
                    Metrics = m.Map,

                    // convenience fields (ตรงกับตัวอย่างที่คุณเล่า: rwp1_flowp1 ฯลฯ)
                    rwp1_flowp1 = m.GetDouble("rwp1_flowp1"),
                    rwp1_flowp2 = m.GetDouble("rwp1_flowp2"),
                    rwp1_sumflowp1 = m.GetDouble("rwp1_sumflowp1"),
                    rwp1_sumflowp2 = m.GetDouble("rwp1_sumflowp2"),
                    rwp2_flowp3 = m.GetDouble("rwp2_flowp3"),
                    rwp2_flowp4 = m.GetDouble("rwp2_flowp4"),
                    rwp2_sumflowp3 = m.GetDouble("rwp2_sumflowp3"),
                    rwp2_sumflowp4 = m.GetDouble("rwp2_sumflowp4"),

                    // pumps map for frontend: { "1P01A":"ON", ... }
                    Pumps = pumps,

                    // meta
                    PickHour = $"{targetHour:00}:00:00",
                    TsUtc = tsUtc,
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
                try { ctx.Log.Warn($"[RWS] /api/rws/summary error: {ex.Message}"); } catch { }
                WriteJsonError(resp, 500, "rws_summary_error", ex.Message);
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
            public Dictionary<string, double?> Map { get; } = new(StringComparer.OrdinalIgnoreCase);
            public string? TsUtc { get; set; }
            public double? GetDouble(string key) => Map.TryGetValue(key, out var v) ? v : null;
        }

        private static async Task<MetricsRead> ReadRwpMetricsAsync(
            SqliteConnection conn,
            string[] paramList,
            CancellationToken ct)
        {
            var res = new MetricsRead();

            await using var cmd = conn.CreateCommand();

            var inParams = new List<string>(paramList.Length);
            for (int i = 0; i < paramList.Length; i++)
            {
                var pn = $"@p{i}";
                inParams.Add(pn);
                cmd.Parameters.AddWithValue(pn, paramList[i]);
            }

            // ใช้ stream LIKE '%RWP%' ตามที่คุณระบุ
            cmd.CommandText = $@"
SELECT param, value_real, value_text, ts_utc
FROM metrics_current
WHERE stream LIKE '%RWP%'
  AND param IN ({string.Join(",", inParams)});
";

            await using var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                var param = r.GetString(0);

                double? v = null;
                if (!r.IsDBNull(1))
                {
                    v = r.GetDouble(1);
                }
                else if (!r.IsDBNull(2))
                {
                    var s = r.GetString(2);
                    if (double.TryParse(s, NumberStyles.Float, CultureInfo.InvariantCulture, out var dv))
                        v = dv;
                    else if (double.TryParse(s, NumberStyles.Float, CultureInfo.GetCultureInfo("th-TH"), out dv))
                        v = dv;
                }

                res.Map[param] = v;

                var ts = r.IsDBNull(3) ? null : r.GetString(3);
                if (!string.IsNullOrWhiteSpace(ts))
                {
                    if (res.TsUtc == null || string.CompareOrdinal(ts, res.TsUtc) > 0)
                        res.TsUtc = ts;
                }
            }

            return res;
        }

        private sealed class AqPick
        {
            public string? ts { get; set; }
            public double? value { get; set; }
            public string? value_text { get; set; }
            public string? updated_at { get; set; }
        }

        // AQ: เลือก HH:00:00 ของชั่วโมงปัจจุบัน (ไม่สนวันที่) + เลือก MAX(ts) ต่อ configID
        private static async Task<Dictionary<int, AqPick>> ReadAqByHourAsync(
            SqliteConnection conn,
            int[] cfgIds,
            int targetHour,
            CancellationToken ct)
        {
            var map = new Dictionary<int, AqPick>();

            await using var cmd = conn.CreateCommand();
            var inParams = new List<string>(cfgIds.Length);
            for (int i = 0; i < cfgIds.Length; i++)
            {
                var pn = $"@id{i}";
                inParams.Add(pn);
                cmd.Parameters.AddWithValue(pn, cfgIds[i]);
            }
            cmd.Parameters.AddWithValue("@hh", targetHour);

            cmd.CommandText = $@"
WITH candidates AS (
  SELECT configID, ts, value, value_text, updated_at
  FROM AQ_readings_narrow_v2
  WHERE configID IN ({string.Join(",", inParams)})
    AND CAST(strftime('%H', ts) AS INTEGER) = @hh
    AND strftime('%M', ts) = '00'
    AND strftime('%S', ts) = '00'
),
latest_per_id AS (
  SELECT c.*
  FROM candidates c
  JOIN (
    SELECT configID, MAX(ts) AS max_ts
    FROM candidates
    GROUP BY configID
  ) m
  ON m.configID = c.configID AND m.max_ts = c.ts
)
SELECT configID, ts, value, value_text, updated_at
FROM latest_per_id;
";

            await using var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                var id = r.GetInt32(0);
                map[id] = new AqPick
                {
                    ts = r.IsDBNull(1) ? null : r.GetString(1),
                    value = r.IsDBNull(2) ? null : r.GetDouble(2),
                    value_text = r.IsDBNull(3) ? null : r.GetString(3),
                    updated_at = r.IsDBNull(4) ? null : r.GetString(4)
                };
            }

            return map;
        }

        private static Dictionary<string, string> BuildPumpStatusMap(Dictionary<int, AqPick> aqByCfg)
        {
            var pumps = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var kv in PumpCfgToName)
            {
                var cfg = kv.Key;
                var name = kv.Value;

                if (aqByCfg.TryGetValue(cfg, out var pick))
                {
                    var raw = (pick.value_text ?? "").Trim().ToUpperInvariant();
                    if (raw == "STANDBY") raw = "STBY";
                    pumps[name] = string.IsNullOrEmpty(raw) ? "-" : raw;
                }
                else
                {
                    pumps[name] = "-";
                }
            }

            return pumps;
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