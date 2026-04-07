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
    public static class EventHandlers
    {
        // AQ configIDs ที่ต้องใช้จริง
        private static readonly int[] AqCfgIds = new[]
        {
            // Flow setpoint
            1, 2, 39, 40,

            // Alum dose P1,P2
            89, 92,

            // PACL dose P1,P2
            8402, 8404,

            // Chlorine PRE/POST P1,P2
            129, 131, 133, 135,

            // Alum dose P3,P4
            151, 154,

            // PACL dose P3,P4
            8416, 8418,

            // Chlorine PRE/POST P3,P4
            192, 194, 196, 198,

            // Tank values
            93, 96,       // Alum A/B
            155, 158,     // Alum C/D
            8405, 8407,   // PACL A/B
            8419, 8421,   // PACL C/D
            127, 128,     // Chlorine line A/B (phase 1-2)
            190, 191,     // Chlorine line C/D (phase 3-4)
            6972, 6973    // Chlorine stock
        };

        public static async Task HandleEventSummaryAsync(HttpListenerContext hc, EngineContext ctx, CancellationToken ct)
        {
            var resp = hc.Response;

            try
            {
                resp.StatusCode = 200;
                resp.ContentType = "application/json; charset=utf-8";
                resp.Headers["Access-Control-Allow-Origin"] = "*";
                resp.Headers["Cache-Control"] = "no-store";

                var nowLocal = DateTime.Now;

                // chem.db ยัง fix วันไว้ก่อนตามที่นายใช้อยู่
                //var dayin = "2025-04-28";
                var dayin = nowLocal.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                var todayYmd = nowLocal.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                //var todayYmd = "2026-03-30";
                var midnightTs = $"{todayYmd} 00:00:00";
                var targetHour = nowLocal.Hour & ~1;
                var pickHourTs = $"{todayYmd} {targetHour:00}:00:00";

                var chemDbPath = ResolveChemDbPath();
                var dataDbPath = ResolveDataDbPath();

                if (!System.IO.File.Exists(chemDbPath))
                    throw new InvalidOperationException($"chem db not found: {chemDbPath}");

                if (!System.IO.File.Exists(dataDbPath))
                    throw new InvalidOperationException($"data db not found: {dataDbPath}");

                List<ChemDailyRow> chemData;
                await using (var chemConn = new SqliteConnection($"Data Source={chemDbPath};Mode=ReadOnly;Cache=Shared;Pooling=False;"))
                {
                    await chemConn.OpenAsync(ct).ConfigureAwait(false);
                    chemData = await ReadChemDailySummaryAsync(chemConn, dayin, ct).ConfigureAwait(false);
                }

                Dictionary<string, AqPick> aqByCfg;
                Dictionary<string, AqPick> aqMidnightByCfg;
                List<RemarkSummaryRow> remarkSummary;
                var workDateYmd8 = nowLocal.ToString("yyyyMMdd", CultureInfo.InvariantCulture);

                await using (var dataConn = new SqliteConnection($"Data Source={dataDbPath};Mode=ReadOnly;Cache=Shared;Pooling=True;"))
                {
                    await dataConn.OpenAsync(ct).ConfigureAwait(false);

                    // ค่ารอบชั่วโมงปัจจุบันของ "วันนี้"
                    aqByCfg = await ReadAqByHourAsync(dataConn, AqCfgIds, todayYmd, targetHour, ct).ConfigureAwait(false);

                    // ค่า 00:00:00 ของวันตั้งต้น
                    aqMidnightByCfg = await ReadAqAtExactTsAsync(dataConn, AqCfgIds, midnightTs, ct).ConfigureAwait(false);

                    // summary remark ของวันปัจจุบัน
                    remarkSummary = await ReadRemarkSummaryAsync(dataConn, workDateYmd8, ct).ConfigureAwait(false);
                }

                var payload = new
                {
                    ok = true,
                    DayIn = dayin,

                    PickHour = $"{targetHour:00}:00:00",
                    PickHourTs = pickHourTs,
                    Aq = aqByCfg,

                    MidnightTs = midnightTs,
                    AqMidnight = aqMidnightByCfg,

                    ChemData = chemData,

                    RemarkWorkDateYmd8 = workDateYmd8,
                    RemarkSummary = remarkSummary,

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
                try { ctx.Log.Warn($"[EVENT] /api/event/summary error: {ex.Message}"); } catch { }
                WriteJsonError(resp, 500, "event_summary_error", ex.Message);
            }
            finally
            {
                try { resp.OutputStream.Close(); } catch { }
            }
        }

        public static void WritePreflight(HttpListenerContext hc)
        {
            try
            {
                var resp = hc.Response;
                resp.StatusCode = 204;
                resp.Headers["Access-Control-Allow-Origin"] = "*";
                resp.Headers["Access-Control-Allow-Methods"] = "GET, OPTIONS";
                resp.Headers["Access-Control-Allow-Headers"] = "Content-Type";
                resp.Headers["Access-Control-Max-Age"] = "86400";
                resp.OutputStream.Close();
            }
            catch { }
        }

        // ===================== models =====================

        public sealed class ChemDailyRow
        {
            public string? d { get; set; }
            public string? ProductCode { get; set; }
            public string? ProductName { get; set; }
            public int qty { get; set; }
            public double? total_net { get; set; }
        }

        public sealed class AqPick
        {
            public string? ts { get; set; }
            public double? value { get; set; }
            public string? value_text { get; set; }
            public string? updated_at { get; set; }
        }

        // ===================== CL CHEM report response =====================
        public sealed class RemarkSummaryRow
        {
            public string? type { get; set; }
            public int station_15 { get; set; }
            public int station_62 { get; set; }
        }

        private static async Task<List<RemarkSummaryRow>> ReadRemarkSummaryAsync(
            SqliteConnection conn,
            string workDateYmd8,
            CancellationToken ct)
        {
            var rows = new List<RemarkSummaryRow>();

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
                                SELECT
                                    type,
                                    SUM(CASE WHEN station_id = 15 THEN 1 ELSE 0 END) AS station_15,
                                    SUM(CASE WHEN station_id = 62 THEN 1 ELSE 0 END) AS station_62
                                FROM AQ_transaction_remark_detail
                                WHERE work_date_ymd8 = $work_date_ymd8
                                GROUP BY type
                                ORDER BY type;";
            cmd.Parameters.AddWithValue("$work_date_ymd8", workDateYmd8);

            await using var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                rows.Add(new RemarkSummaryRow
                {
                    type = r.IsDBNull(0) ? null : r.GetString(0),
                    station_15 = r.IsDBNull(1) ? 0 : r.GetInt32(1),
                    station_62 = r.IsDBNull(2) ? 0 : r.GetInt32(2)
                });
            }

            return rows;
        }

        // ===================== helpers =====================

        private static string ResolveChemDbPath()
        {
            return MdbReaderSafe.ResolveActiveChemDbPath(AppContext.BaseDirectory);
        }

        private static string ResolveDataDbPath()
        {
            var baseDir = AppContext.BaseDirectory;
            return System.IO.Path.Combine(baseDir, "data.db");
        }

        private static async Task<List<ChemDailyRow>> ReadChemDailySummaryAsync(
            SqliteConnection conn,
            string dayin,
            CancellationToken ct)
        {
            var rows = new List<ChemDailyRow>();

            await using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
SELECT
  w.DAYIN AS d,
  w.PRODUCT AS ProductCode,
  p.NAME AS ProductName,
  COUNT(*) AS qty,
  SUM(w.W1 - w.W2) AS total_net
FROM WDATA AS w
LEFT JOIN PRODUCT AS p ON w.PRODUCT = p.CODE
WHERE w.STAT = '2'
  AND w.DAYIN = $dayin
GROUP BY w.DAYIN, w.PRODUCT, p.NAME
ORDER BY w.DAYIN ASC, w.PRODUCT ASC;";
            cmd.Parameters.AddWithValue("$dayin", dayin);

            await using var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                rows.Add(new ChemDailyRow
                {
                    d = r.IsDBNull(0) ? null : r.GetString(0),
                    ProductCode = r.IsDBNull(1) ? null : r.GetString(1),
                    ProductName = r.IsDBNull(2) ? null : r.GetString(2),
                    qty = r.IsDBNull(3) ? 0 : r.GetInt32(3),
                    total_net = r.IsDBNull(4) ? null : r.GetDouble(4)
                });
            }

            return rows;
        }

        // รอบ PickHour ของ "วันปัจจุบัน"
        private static async Task<Dictionary<string, AqPick>> ReadAqByHourAsync(
            SqliteConnection conn,
            int[] cfgIds,
            string targetDayYmd,
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
            cmd.Parameters.AddWithValue("@day", targetDayYmd);

            cmd.CommandText = $@"
WITH candidates AS (
  SELECT configID, ts, value, value_text, updated_at
  FROM AQ_readings_narrow_v2
  WHERE configID IN ({string.Join(",", inParams)})
    AND date(ts) = @day
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
    ON m.configID = c.configID
   AND m.max_ts = c.ts
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

        // ค่า 00:00:00 ของวันตั้งต้นแบบ exact
        private static async Task<Dictionary<string, AqPick>> ReadAqAtExactTsAsync(
            SqliteConnection conn,
            int[] cfgIds,
            string exactTs,
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

            cmd.Parameters.AddWithValue("@ts", exactTs);

            cmd.CommandText = $@"
SELECT configID, ts, value, value_text, updated_at
FROM AQ_readings_narrow_v2
WHERE configID IN ({string.Join(",", inParams)})
  AND ts = @ts;
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