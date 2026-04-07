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
    public static class DpsHandlers
    {
        // ---- AQ configIDs ที่ต้องพ่วงใน response ----
        private static readonly int[] AqConfigIds = new[] { 816, 829, 842, 857 };

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

        public static async Task HandleDpsSummaryAsync(HttpListenerContext hc, EngineContext ctx, CancellationToken ct)
        {
            var resp = hc.Response;

            try
            {
                resp.StatusCode = 200;
                resp.ContentType = "application/json; charset=utf-8";
                resp.Headers["Access-Control-Allow-Origin"] = "*";
                resp.Headers["Cache-Control"] = "no-store";

                var dbPath = ResolveDbPath(ctx);

                // 1) metrics_current (เดิม)
                var (flowAB, sumFlowAB, AirP1, AirP2, dbTsUtc) = await ReadDpsSummaryAsync(dbPath, ct).ConfigureAwait(false);

                // 2) AQ_readings_narrow_v2 (เพิ่มใหม่)
                //    เลือก record ที่ "ชั่วโมงปัจจุบันแบบปัดลง" (ไม่สนวันที่) => HH:00:00
                var nowLocal = DateTime.Now;
                var targetHour = nowLocal.Hour; // 0..23 (05:16 / 05:50 => 5)
                var aq = await ReadAqByHourAsync(dbPath, targetHour, ct).ConfigureAwait(false);

                var payload = new
                {
                    FlowAB = flowAB,               // double?  DPS_flowA + DPS_flowB
                    SumFlowAB = sumFlowAB,         // double?  DPS_sumflow
                    AirP1 = AirP1,
                    AirP2 = AirP2,
                    TsUtc = dbTsUtc,               // ts_utc from DB rows (if available)

                    // เพิ่มใหม่
                    Aq = aq,                       // Dictionary<int, { ts, value, value_text, updated_at }>
                    PickHour = $"{targetHour:00}:00:00",
                    ServerTsLocal = nowLocal.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),

                    // ของเดิม
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
                try { ctx.Log.Warn($"[DPS] /api/dps/summary error: {ex.Message}"); } catch { }
                WriteJsonError(resp, 500, "dps_summary_error", ex.Message);
            }
            finally
            {
                try { resp.OutputStream.Close(); } catch { }
            }
        }

        // ---------------- helpers ----------------

        private static string ResolveDbPath(EngineContext ctx)
        {
            // ✅ ถ้ามี path จริงใน ctx ให้คุณปรับมาใช้ตรงนั้น
            // ตอนนี้ fallback แบบ "อยู่ข้าง exe" ก่อน:
            var baseDir = AppContext.BaseDirectory;

            var p1 = System.IO.Path.Combine(baseDir, "data.db");
            if (System.IO.File.Exists(p1)) return p1;

            return p1; // ให้ error ชัด
        }

        private static async Task<(double? FlowAB, double? SumFlowAB, double? AirP1, double? AirP2, string? TsUtc)> ReadDpsSummaryAsync(string dbPath, CancellationToken ct)
        {
            if (!System.IO.File.Exists(dbPath))
                throw new InvalidOperationException($"db not found: {dbPath}");

            double? flowA = null, flowB = null, sumFlow = null;
            double? AirP1 = null, AirP2 = null;
            string? tsUtc = null;

            await using var conn = new SqliteConnection($"Data Source={dbPath};Mode=ReadOnly;Cache=Shared;Pooling=True;");
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
            SELECT param, value_real, value_text, ts_utc
            FROM metrics_current
            WHERE stream = 'DPS'
              AND param IN ('DPS_flowA','DPS_flowB','DPS_sumflow', 'AirP1', 'AirP2');
            ";

            await using var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                var param = r.GetString(0);

                // value_real preferred
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

                var ts = r.IsDBNull(3) ? null : r.GetString(3);
                if (!string.IsNullOrWhiteSpace(ts)) tsUtc = ts;

                switch (param)
                {
                    case "DPS_flowA": flowA = v; break;
                    case "DPS_flowB": flowB = v; break;
                    case "DPS_sumflow": sumFlow = v; break;
                    case "AirP1": AirP1 = v; break;
                    case "AirP2": AirP2 = v; break;
                }
            }

            double? flowAB = (flowA.HasValue && flowB.HasValue) ? flowA.Value + flowB.Value : (double?)null;
            return (flowAB, sumFlow, AirP1, AirP2, tsUtc);
        }

        /// <summary>
        /// AQ_readings_narrow_v2:
        /// - configID IN (816,829,842,857)
        /// - เลือก record ที่ hour == targetHour และ นาที/วินาที = 00 (HH:00:00)
        /// - ไม่สนวันที่: ถ้ามีหลายวัน ให้เลือก ts ล่าสุดต่อ configID (MAX(ts))
        /// </summary>
        private static async Task<Dictionary<int, AqPick>> ReadAqByHourAsync(string dbPath, int targetHour, CancellationToken ct)
        {
            if (!System.IO.File.Exists(dbPath))
                throw new InvalidOperationException($"db not found: {dbPath}");

            var map = new Dictionary<int, AqPick>();

            await using var conn = new SqliteConnection($"Data Source={dbPath};Mode=ReadOnly;Cache=Shared;Pooling=True;");
            await conn.OpenAsync(ct).ConfigureAwait(false);

            await using var cmd = conn.CreateCommand();

            // สร้าง IN-list แบบ param เพื่อกัน SQL injection และรองรับเปลี่ยนชุด config ได้ง่าย
            var inParams = new List<string>(AqConfigIds.Length);
            for (int i = 0; i < AqConfigIds.Length; i++)
            {
                var pn = $"@id{i}";
                inParams.Add(pn);
                cmd.Parameters.AddWithValue(pn, AqConfigIds[i]);
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
                var configId = r.GetInt32(0);
                var ts = r.IsDBNull(1) ? null : r.GetString(1);

                double? value = null;
                if (!r.IsDBNull(2))
                {
                    // value เป็น REAL
                    value = r.GetDouble(2);
                }

                string? valueText = null;
                if (!r.IsDBNull(3))
                {
                    // value_text เช่น ON / STANDBY
                    valueText = r.GetString(3);
                }

                var updatedAt = r.IsDBNull(4) ? null : r.GetString(4);

                map[configId] = new AqPick
                {
                    ts = ts,
                    value = value,
                    value_text = valueText,
                    updated_at = updatedAt
                };
            }

            // หมายเหตุ: ถ้าไม่มี record ของบาง configID map จะไม่ใส่ key นั้น (frontend ค่อยเช็คมี/ไม่มี)
            return map;
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

        // ---------------- DTO ----------------
        // ใช้ class/struct แบบนี้เพื่อให้ JSON key เป็น ts/value/value_text/updated_at ตามที่ต้องการ
        private sealed class AqPick
        {
            public string? ts { get; set; }
            public double? value { get; set; }
            public string? value_text { get; set; }
            public string? updated_at { get; set; }
        }
    }
}