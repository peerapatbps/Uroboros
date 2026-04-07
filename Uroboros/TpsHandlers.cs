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
    public static class TpsHandlers
    {
        // ---- AQ configIDs (pump status) ----
        private static readonly int[] PumpCfgIds = new[] { 761, 777, 793, 7984 };

        // mapping สำหรับ frontend data-pump
        private static readonly Dictionary<int, string> PumpCfgToName = new()
        {
            { 761, "Pump1" },
            { 777, "Pump2" },
            { 793, "Pump3" },
            { 7984, "Pump4" },
        };

        // ---- OneValueSeries sources ----
        private static readonly string[] TankSources = new[] { "PK", "TP", "RB" };
        private static readonly string[] TankKeys = new[] { "Qin", "F", "Inlet", "P", "Level" };

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

        public static async Task HandleTpsSummaryAsync(HttpListenerContext hc, EngineContext ctx, CancellationToken ct)
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
                var targetHour = nowLocal.Hour; // AQ ปัดลงชั่วโมง
                var nowSec = nowLocal.Hour * 3600 + nowLocal.Minute * 60 + nowLocal.Second; // nearest time-of-day

                await using var conn = new SqliteConnection($"Data Source={dbPath};Mode=ReadOnly;Cache=Shared;Pooling=True;");
                await conn.OpenAsync(ct).ConfigureAwait(false);

                // 1) metrics_current: TPS
                var tpsMetrics = await ReadMetricsAsync(conn, "TPS",
                    new[] { "TR_flow", "TR_pressure", "TR_cwt", "TR_sumflow" },
                    ct).ConfigureAwait(false);

                // 2) metrics_current: DPS (service water)
                var dpsMetrics = await ReadMetricsAsync(conn, "DPS",
                    new[] { "SVwater_flow", "SVwater_sumflow" },
                    ct).ConfigureAwait(false);

                // 3) Pump statuses from AQ_readings_narrow_v2 (by hour, ignore date)
                var aqByCfg = await ReadAqByHourAsync(conn, PumpCfgIds, targetHour, ct).ConfigureAwait(false);

                // 4) PK/TP/RB snapshot nearest by time-of-day (ignore date)
                var tankSnap = await ReadNearestTankSnapshotAsync(conn, TankSources, TankKeys, nowSec, ct).ConfigureAwait(false);

                // 5) RCV38 series: 1 จุดต่อ 10 นาที (bucket) ย้อนหลัง 6 ชั่วโมง by time-of-day (ignore date)
                var rcv38 = await ReadRcv38SeriesAsync(
                    conn,
                    source: "rcv38",
                    key: "p",
                    nowSec: nowSec,
                    pastSeconds: 6 * 3600,
                    ct: ct
                ).ConfigureAwait(false);

                // TsUtc: เลือก max(ts_utc) จาก metrics_current ที่อ่านมา (ถ้ามี)
                var tsUtc = tpsMetrics.TsUtc ?? dpsMetrics.TsUtc;

                var payload = new
                {
                    // TPS KPIs
                    TR_flow = tpsMetrics.GetDouble("TR_flow"),
                    TR_pressure = tpsMetrics.GetDouble("TR_pressure")/10,
                    TR_cwt = tpsMetrics.GetDouble("TR_cwt"),
                    TR_sumflow = tpsMetrics.GetDouble("TR_sumflow"),

                    // DPS service water
                    SVwater_flow = dpsMetrics.GetDouble("SVwater_flow"),
                    SVwater_sumflow = dpsMetrics.GetDouble("SVwater_sumflow"),

                    // Pumps
                    Pumps = BuildPumpStatusMap(aqByCfg),

                    // Tanks
                    Tanks = tankSnap,

                    // RCV38 series
                    RCV38 = rcv38,

                    // Meta
                    PickHour = $"{targetHour:00}:00:00",
                    PickNearestSec = nowSec,
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
                try { ctx.Log.Warn($"[TPS] /api/tps/summary error: {ex.Message}"); } catch { }
                WriteJsonError(resp, 500, "tps_summary_error", ex.Message);
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
            return System.IO.Path.Combine(baseDir, "data.db"); // ปรับไปใช้ ctx path จริงของคุณได้
        }

        private sealed class MetricsRead
        {
            public Dictionary<string, double?> Map { get; } = new(StringComparer.OrdinalIgnoreCase);
            public string? TsUtc { get; set; }
            public double? GetDouble(string key) => Map.TryGetValue(key, out var v) ? v : null;
        }

        private static async Task<MetricsRead> ReadMetricsAsync(
            SqliteConnection conn,
            string stream,
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
            cmd.Parameters.AddWithValue("@stream", stream);

            cmd.CommandText = $@"
            SELECT param, value_real, value_text, ts_utc
            FROM metrics_current
            WHERE stream = @stream
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

        // OneValueSeries: nearest-by-time-of-day (ignore date) ต่อ (source,key)
        // ในไฟล์ TpsHandlers.cs ปรับปรุงเมธอด ReadNearestTankSnapshotAsync ดังนี้:

        private static async Task<Dictionary<string, object>> ReadNearestTankSnapshotAsync(
            SqliteConnection conn,
            string[] sources,
            string[] keys,
            int nowSec,
            CancellationToken ct)
        {
            // กำหนดค่า Max Level ตามรูปภาพ Dashboard
            var tankMax = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase)
            {
                ["PK"] = 6.70,
                ["TP"] = 5.63,
                ["RB"] = 5.60
            };

            var result = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

            await using var cmd = conn.CreateCommand();

            // สร้าง SQL Parameter สำหรับ Source (PK, TP, RB)
            var srcParams = new List<string>();
            for (int i = 0; i < sources.Length; i++)
            {
                var pn = $"@s{i}";
                srcParams.Add(pn);
                cmd.Parameters.AddWithValue(pn, sources[i]);
            }

            // Query ดึงข้อมูลจาก branch_current ตามโครงสร้าง: source | key | value | ts_th
            cmd.CommandText = $@"
        SELECT source, key, value 
        FROM branch_current 
        WHERE source IN ({string.Join(",", srcParams)})";

            var rawData = new Dictionary<string, Dictionary<string, double>>(StringComparer.OrdinalIgnoreCase);

            await using var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                var src = r.GetString(0);
                var key = r.GetString(1);
                var val = r.IsDBNull(2) ? 0 : r.GetDouble(2);

                if (!rawData.TryGetValue(src, out var dict))
                {
                    dict = new Dictionary<string, double>(StringComparer.OrdinalIgnoreCase);
                    rawData[src] = dict;
                }
                dict[key] = val;
            }

            // ประมวลผลและ Map ค่าเข้ากับหน้า Dashboard
            foreach (var src in sources)
            {
                rawData.TryGetValue(src, out var d);

                // ดึงค่า Raw จาก Dictionary
                double qInRaw = (d != null && d.TryGetValue("Qin", out var v1)) ? v1 : 0;
                double f = (d != null && d.TryGetValue("F", out var v2)) ? v2 : 0;
                double inlet = (d != null && d.TryGetValue("Inlet", out var v3)) ? v3 : 0;
                double p = (d != null && d.TryGetValue("P", out var v4)) ? v4 : 0;
                double level = (d != null && d.TryGetValue("Level", out var v5)) ? v5 : 0;

                // คำนวณตามสูตรที่ระบุ
                // Delta Q = Qin - F
                // Pin = Inlet
                // Pout = P
                double deltaQ = qInRaw - f;
                var maxLimit = tankMax.TryGetValue(src, out var mx) ? mx : 6.70;
                double? pct = (maxLimit > 0) ? Math.Clamp((level / maxLimit) * 100.0, 0, 100) : 0;

                result[src] = new
                {
                    Qin = deltaQ,    // ส่งค่าที่ลบกันแล้วในชื่อ Qin เพื่อให้ Frontend แสดงเป็น Delta Q
                    Pin = inlet,     // Inlet
                    Pout = p,        // P
                    Level = level,   // Level gauge
                    Max = maxLimit,
                    Percent = pct
                };
            }

            return result;
        }

        // RCV38: series 1 จุดต่อ 10 นาที (bucket=sec/600) ในช่วงย้อนหลัง pastSeconds (by time-of-day, ignore date)
        private static async Task<List<RcvPoint>> ReadRcv38SeriesAsync(
            SqliteConnection conn,
            string source,
            string key,
            int nowSec,
            int pastSeconds,
            CancellationToken ct)
        {
            var list = new List<RcvPoint>();

            await using var cmd = conn.CreateCommand();
            cmd.Parameters.AddWithValue("@src", source);
            cmd.Parameters.AddWithValue("@key", key);
            cmd.Parameters.AddWithValue("@nowSec", nowSec);
            cmd.Parameters.AddWithValue("@past", pastSeconds);

            // deltaPast = (nowSec - sec + 86400) % 86400  (0=ตอนนี้, มากขึ้น=ย้อนหลัง)
            // bucket10 = sec/600 (0..143)
            // เลือก 1 จุดต่อ bucket โดยเอา "ts ล่าสุด" ใน bucket (ts DESC) เพื่อกันเคสลง :59 / :52
            // เรียงผลลัพธ์เก่า -> ใหม่: deltaPast DESC (เก่าสุดค่ามาก) ไปหาใกล้ปัจจุบัน
            cmd.CommandText = @"
            WITH x AS (
              SELECT
                value,
                -- วินาทีตั้งแต่เที่ยงคืน (secOfDay)
                (CAST(strftime('%H', ts) AS INTEGER) * 3600) +
                (CAST(strftime('%M', ts) AS INTEGER) * 60) +
                 CAST(strftime('%S', ts) AS INTEGER) AS sod
              FROM OneValueSeries
              WHERE source='rcv38' AND key='p'
            ),
            b AS (
              SELECT
                (sod / 360) * 360 AS bucket_sod,   -- 360s = 6 นาที
                AVG(value) AS v_avg
              FROM x
              GROUP BY bucket_sod
            )
            SELECT
              -- แปะ bucket_sod เข้ากับ ""วันนี้"" เพื่อให้ frontend plot ได้
              datetime(date('now') || ' 00:00:00', printf('+%d seconds', bucket_sod)) AS ts_plot,
              v_avg AS value
            FROM b
            ORDER BY bucket_sod;
            ";

            await using var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                var ts = r.GetString(0);
                var v = r.GetDouble(1);

                var hhmm = ts.Length >= 16 ? ts.Substring(11, 5) : ts;
                list.Add(new RcvPoint { t = hhmm, v = v });
            }

            return list;
        }

        private sealed class RcvPoint
        {
            public string t { get; set; } = "";
            public double v { get; set; }
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