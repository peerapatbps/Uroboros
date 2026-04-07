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
    public static class ChemHandlers
    {
        // ----------------------------
        // AQ configIDs ที่ต้องอ่าน (status + value + setpoints)
        // ----------------------------
        private static readonly int[] AqCfgIds = new[]
        {
            // Flow setpoint (สำหรับคำนวณ dose/rate ที่ frontend)
            1, 2, 39, 40,

            // ===== ALUM1 =====
            81, 83, 85,       // pump status
            95, 98,           // tank status A/B
            93, 96,           // tank value A/B
            89, 92,           // DOSE ALUM setpoint P1,P2

            // ===== PACL1 =====
            8395, 8397, 8399, // pump status
            8406, 8408,       // tank status A/B
            8405, 8407,       // tank value A/B
            8402, 8404,       // DOSE PACL setpoint P1,P2

            // ===== CHLORINE#1 =====
            109, 111, 113, 115, 117,          // EVAP 1..5 status
            118, 119, 120, 121, 122,          // Chlorinator 1..5 status
            4325, 4326,                       // tank line A/B status (phase 1-2)
            129, 131, 133, 135,               // PRE/POST dose setpoint P1,P2

            // ===== ALUM2 =====
            137, 139, 141,    // pump status P3
            143, 145, 147,        // pump status P4
            151, 154,         // DOSE ALUM setpoint P3,P4
            155, 158,           // tank status C/D
            157, 160,           // tank value C/D

            // ===== PACL2 =====
            8409, 8411, 8413,    // pump status
            8416, 8418,        // DOSE PACL setpoint P3,P4
            8420, 8422,       // tank status A/B
            8419, 8421,       // tank value A/B

            // ===== CHLORINE#2 =====
            171, 173, 175, 177, 179,          // EVAP 1..5 status
            181, 182, 183, 184, 185,          // Chlorinator 1..5 status
            4327, 4328,                       // tank line A/B status (phase 3-4)
            192, 194, 196, 198,               // PRE/POST dose setpoint P3,P4
        };

        // ----------------------------
        // metrics_current (chlorine tank read) ที่ต้องอ่าน
        // ----------------------------
        private static readonly (string stream, string param)[] ChemMetricKeys = new[]
        {
            ("CHEM1", "cl_lineA"),
            ("CHEM1", "cl_lineB"),
            ("CHEM2", "cl_lineC"),
            ("CHEM2", "cl_lineD"),
        };

        public static async Task HandleChemSummaryAsync(HttpListenerContext hc, EngineContext ctx, CancellationToken ct)
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
                var targetHour = nowLocal.Hour & ~1;

                await using var conn = new SqliteConnection($"Data Source={dbPath};Mode=ReadOnly;Cache=Shared;Pooling=True;");
                await conn.OpenAsync(ct).ConfigureAwait(false);

                // 1) metrics_current: chlorine tank read (CHEM1/CHEM2 lineA..D)
                var m = await ReadChemMetricsCurrentAsync(conn, ChemMetricKeys, ct).ConfigureAwait(false);

                // 2) AQ_readings_narrow_v2: status/value/setpoints (by hour HH:00:00, ignore date)
                var aqByCfg = await ReadAqByHourAsync(conn, AqCfgIds, targetHour, ct).ConfigureAwait(false);

                // TsUtc: max(ts_utc) จาก metrics_current ที่อ่านมา
                var tsUtc = m.TsUtc;

                var payload = new
                {
                    ok = true,

                    // AQ map by cfgId => { ts, value, value_text, updated_at }
                    Aq = aqByCfg,

                    // metrics_current grouped by stream => { "CHEM1": { "cl_lineA": 5036, ... }, "CHEM2": ... }
                    metrics_current = m.ByStream,

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
                try { ctx.Log.Warn($"[CHEM] /api/chem/summary error: {ex.Message}"); } catch { }
                WriteJsonError(resp, 500, "chem_summary_error", ex.Message);
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

            public string? TsUtc { get; set; }

            public void Set(string stream, string param, double? value)
            {
                if (!ByStream.TryGetValue(stream, out var map))
                {
                    map = new Dictionary<string, double?>(StringComparer.OrdinalIgnoreCase);
                    ByStream[stream] = map;
                }
                map[param] = value;
            }
        }

        private static async Task<MetricsRead> ReadChemMetricsCurrentAsync(
            SqliteConnection conn,
            (string stream, string param)[] keys,
            CancellationToken ct)
        {
            var res = new MetricsRead();

            await using var cmd = conn.CreateCommand();

            // สร้าง WHERE (stream=@s0 AND param=@p0) OR ...
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

            return res;
        }

        public sealed class AqPick
        {
            public string? ts { get; set; }
            public double? value { get; set; }
            public string? value_text { get; set; }
            public string? updated_at { get; set; }
        }

        // AQ: เลือก HH:00:00 ของชั่วโมงปัจจุบัน (ไม่สนวันที่) + เลือก MAX(ts) ต่อ configID
        private static async Task<Dictionary<string, AqPick>> ReadAqByHourAsync(
            SqliteConnection conn,
            int[] cfgIds,
            int targetHour,
            CancellationToken ct)
        {
            var map = new Dictionary<string, AqPick>(StringComparer.OrdinalIgnoreCase);

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

                map[id.ToString(CultureInfo.InvariantCulture)] = new AqPick
                {
                    ts = r.IsDBNull(1) ? null : r.GetString(1),
                    value = r.IsDBNull(2) ? null : r.GetDouble(2),
                    value_text = r.IsDBNull(3) ? null : r.GetString(3),
                    updated_at = r.IsDBNull(4) ? null : r.GetString(4)
                };
            }

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
    }
}