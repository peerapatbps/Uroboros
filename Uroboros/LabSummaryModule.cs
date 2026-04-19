using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

public static class LabSummaryModule
{
    public static async Task HandlePostAsync(HttpListenerContext hc, JsonSerializerOptions jsonOpt, CancellationToken ct)
    {
        var req = hc.Request;

        string body;
        using (var sr = new StreamReader(req.InputStream, req.ContentEncoding ?? System.Text.Encoding.UTF8))
        {
            body = await sr.ReadToEndAsync().ConfigureAwait(false);
        }

        var dto = JsonSerializer.Deserialize<LabSummaryRequestDto>(body, jsonOpt);
        if (dto is null)
        {
            await WriteJsonAsync(hc, 400, new { error = "Invalid JSON body" }).ConfigureAwait(false);
            return;
        }

        if (!TryNormalizeRwOption(dto.RwZone?.Option, out var rwOption))
        {
            await WriteJsonAsync(hc, 400, new { error = "Invalid rwZone.option" }).ConfigureAwait(false);
            return;
        }

        if (!TryNormalizeRwFix(dto.RwZone?.Fix, out var rwFix))
        {
            await WriteJsonAsync(hc, 400, new { error = "Invalid rwZone.fix" }).ConfigureAwait(false);
            return;
        }

        if (!TryNormalizeZone4Filter(dto.Zone4?.Filter, out var zone4Filter))
        {
            await WriteJsonAsync(hc, 400, new { error = "Invalid zone4.filter" }).ConfigureAwait(false);
            return;
        }

        var result = BuildLabSummaryPayload(rwOption!, rwFix!, zone4Filter!);

        await WriteJsonAsync(hc, 200, new
        {
            data = result
        }).ConfigureAwait(false);
    }

    private static object BuildLabSummaryPayload(string rwOption, string rwFix, string zone4Filter)
    {
        var dbPath = Path.Combine(AppContext.BaseDirectory, "data.db");

        using var conn = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadOnly,
            Cache = SqliteCacheMode.Shared
        }.ToString());

        conn.Open();

        var nowLocal = DateTime.Now;

        // zone1 ใช้เวลาเลขคี่ล่าสุด เช่น 16:57 => 15:00
        var rwAnchor = GetLatestStepAnchor(nowLocal, stepHours: 2, firstHour: 1);
        var rwFrom = rwAnchor.AddHours(-12);

        var rwOptionRows = QueryLabRowsByEquipmentAndParamInWindow(conn, "RW", rwOption, rwFrom, rwAnchor);
        var rwFixRows = QueryLabRowsByEquipmentAndParamInWindow(conn, "RW", rwFix, rwFrom, rwAnchor);

        return new
        {
            zone1 = new Dictionary<string, object>
            {
                [rwOption] = ToSimplePairs(rwOptionRows),
                [rwFix] = ToSimplePairs(rwFixRows)
            },
            zone2 = QueryRecommendDoseLatest(conn),
            zone3 = QueryStatusLatest(conn, nowLocal),
            zone4 = QueryZone4Series(conn, zone4Filter, nowLocal)
        };
    }

    private static bool TryNormalizeRwOption(string? value, out string? normalized)
    {
        normalized = null;
        if (string.IsNullOrWhiteSpace(value)) return false;

        var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "TURBIDITY",
            "pH",
            "ALKALINITY",
            "CONDUCTIVITY",
            "DISSOLVED OXYGEN",
            "HARDNESS",
            "OXYGEN CONSUMED"
        };

        var v = value.Trim();
        if (!allowed.Contains(v)) return false;

        normalized = allowed.First(x => x.Equals(v, StringComparison.OrdinalIgnoreCase));
        return true;
    }

    private static bool TryNormalizeRwFix(string? value, out string? normalized)
    {
        normalized = null;
        if (string.IsNullOrWhiteSpace(value)) return false;

        if (!value.Trim().Equals("TEMPERATURE", StringComparison.OrdinalIgnoreCase))
            return false;

        normalized = "TEMPERATURE";
        return true;
    }

    private static bool TryNormalizeZone4Filter(string? value, out string? normalized)
    {
        normalized = null;
        if (string.IsNullOrWhiteSpace(value)) return false;

        var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
        {
            "CW Turbid",
            "CW Pre CL",
            "FW Turbid",
            "TW Turbid",
            "TW Pos CL"
        };

        var v = value.Trim();
        if (!allowed.Contains(v)) return false;

        normalized = allowed.First(x => x.Equals(v, StringComparison.OrdinalIgnoreCase));
        return true;
    }

    private static DateTime GetLatestStepAnchor(DateTime nowLocal, int stepHours, int firstHour)
    {
        int? best = null;

        for (var h = firstHour; h < 24; h += stepHours)
        {
            if (h <= nowLocal.Hour)
                best = h;
        }

        if (best.HasValue)
            return nowLocal.Date.AddHours(best.Value);

        return nowLocal.Date.AddDays(-1).AddHours(firstHour);
    }

    private static int? GetLatestFourHourAnchorHour(DateTime nowLocal)
    {
        var candidates = new[] { 1, 5, 9, 13, 17, 21 };
        int? best = null;

        foreach (var h in candidates)
        {
            if (h <= nowLocal.Hour)
                best = h;
        }

        return best;
    }

    private static List<LabPointDto> QueryLabRowsByEquipmentAndParamInWindow(
        SqliteConnection conn,
        string equipmentName,
        string paramName,
        DateTime fromLocal,
        DateTime toLocal)
    {
        var rows = new List<LabPointDto>();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
SELECT
    sample_date,
    sample_time,
    configparam_id,
    equipment_name,
    param_name,
    measure_th,
    value_real,
    value_text
FROM lab_import_daily
WHERE equipment_name = $equipment_name
  AND param_name = $param_name
  AND (sample_date || ' ' || sample_time) >= $from_local
  AND (sample_date || ' ' || sample_time) <= $to_local
  AND (value_real IS NOT NULL OR value_text IS NOT NULL)
ORDER BY sample_date ASC, sample_time ASC, configparam_id ASC;
";
        cmd.Parameters.AddWithValue("$equipment_name", equipmentName);
        cmd.Parameters.AddWithValue("$param_name", paramName);
        cmd.Parameters.AddWithValue("$from_local", fromLocal.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture));
        cmd.Parameters.AddWithValue("$to_local", toLocal.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture));

        using var rd = cmd.ExecuteReader();
        while (rd.Read())
        {
            var sampleDate = rd["sample_date"]?.ToString() ?? "";
            var sampleTime = rd["sample_time"]?.ToString() ?? "";

            rows.Add(new LabPointDto
            {
                sample_date = sampleDate,
                sample_time = sampleTime,
                ts_local = $"{sampleDate} {sampleTime}",
                configparam_id = Convert.ToInt32(rd["configparam_id"], CultureInfo.InvariantCulture),
                equipment_name = rd["equipment_name"]?.ToString(),
                param_name = rd["param_name"]?.ToString(),
                measure_th = rd["measure_th"]?.ToString(),
                value_real = rd["value_real"] == DBNull.Value ? null : Convert.ToDouble(rd["value_real"], CultureInfo.InvariantCulture),
                value_text = rd["value_text"] == DBNull.Value ? null : rd["value_text"]?.ToString()
            });
        }

        return rows;
    }

    private static List<object> ToSimplePairs(List<LabPointDto> rows)
    {
        var result = new List<object>();

        foreach (var row in rows)
        {
            object? value = row.value_real.HasValue
                ? row.value_real.Value
                : (string?)row.value_text;

            if (value is null)
                continue;

            result.Add(new object[]
            {
                row.ts_local,
                value
            });
        }

        return result;
    }

    private static List<LabPointDto> QueryRecommendDoseRows(SqliteConnection conn)
    {
        var rows = new List<LabPointDto>();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
WITH latest AS (
    SELECT MAX(sample_date || ' ' || sample_time) AS mx
    FROM lab_import_daily
    WHERE param_name IN ('ALUM RECOMMENDED', 'PACl RECOMMENDED')
      AND (value_real IS NOT NULL OR value_text IS NOT NULL)
)
SELECT
    sample_date,
    sample_time,
    configparam_id,
    equipment_name,
    param_name,
    measure_th,
    value_real,
    value_text
FROM lab_import_daily
WHERE (sample_date || ' ' || sample_time) = (SELECT mx FROM latest)
  AND param_name IN ('ALUM RECOMMENDED', 'PACl RECOMMENDED')
ORDER BY sample_date DESC, sample_time DESC, param_name ASC, equipment_name ASC;
";

        using var rd = cmd.ExecuteReader();
        while (rd.Read())
        {
            var sampleDate = rd["sample_date"]?.ToString() ?? "";
            var sampleTime = rd["sample_time"]?.ToString() ?? "";

            rows.Add(new LabPointDto
            {
                sample_date = sampleDate,
                sample_time = sampleTime,
                ts_local = $"{sampleDate} {sampleTime}",
                configparam_id = Convert.ToInt32(rd["configparam_id"], CultureInfo.InvariantCulture),
                equipment_name = rd["equipment_name"]?.ToString(),
                param_name = rd["param_name"]?.ToString(),
                measure_th = rd["measure_th"]?.ToString(),
                value_real = rd["value_real"] == DBNull.Value ? null : Convert.ToDouble(rd["value_real"], CultureInfo.InvariantCulture),
                value_text = rd["value_text"] == DBNull.Value ? null : rd["value_text"]?.ToString()
            });
        }

        return rows;
    }

    private static object QueryRecommendDoseLatest(SqliteConnection conn)
    {
        var rows = QueryRecommendDoseRows(conn);

        string? Pick(string paramName, string equipmentName)
        {
            var row = rows.FirstOrDefault(x =>
                string.Equals(x.param_name, paramName, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(x.equipment_name, equipmentName, StringComparison.OrdinalIgnoreCase));

            if (row is null) return null;

            return !string.IsNullOrWhiteSpace(row.value_text)
                ? row.value_text
                : row.value_real?.ToString(CultureInfo.InvariantCulture);
        }

        return new
        {
            alum = new
            {
                p12 = Pick("ALUM RECOMMENDED", "PHASE 1 & 2"),
                p34 = Pick("ALUM RECOMMENDED", "PHASE 3 & 4")
            },
            pacl = new
            {
                p12 = Pick("PACl RECOMMENDED", "PHASE 1 & 2"),
                p34 = Pick("PACl RECOMMENDED", "PHASE 3 & 4")
            }
        };
    }

    private static object QueryStatusLatest(SqliteConnection conn, DateTime nowLocal)
    {
        var statusMap = new Dictionary<int, string>
        {
            [1002] = "CW1 T",
            [1004] = "CW1 CL",
            [1014] = "CW2 T",
            [1016] = "CW2 CL",
            [1026] = "CW3 T",
            [1028] = "CW3 CL",
            [1038] = "CW4 T",
            [1040] = "CW4 CL",
            [1005] = "FW1 T",
            [1017] = "FW2 T",
            [1029] = "FW3 T",
            [1041] = "FW4 T",
            [1046] = "TW1 CL",
            [1055] = "TW2 CL",
            [1064] = "TW3 CL",
            [1067] = "TW4 CL"
        };

        var anchorHour = GetLatestFourHourAnchorHour(nowLocal);
        var items = new List<LabStatusItemDto>();

        if (!anchorHour.HasValue)
            return new { items };

        var anchorDate = nowLocal.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var anchorTime = $"{anchorHour.Value:00}:00";

        foreach (var kv in statusMap)
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
SELECT
    configparam_id,
    value_real,
    value_text
FROM lab_import_daily
WHERE configparam_id = $cfg
  AND sample_date = $sample_date
  AND sample_time = $sample_time
  AND (value_real IS NOT NULL OR value_text IS NOT NULL)
ORDER BY sample_date DESC, sample_time DESC
LIMIT 1;
";
            cmd.Parameters.AddWithValue("$cfg", kv.Key);
            cmd.Parameters.AddWithValue("$sample_date", anchorDate);
            cmd.Parameters.AddWithValue("$sample_time", anchorTime);

            using var rd = cmd.ExecuteReader();
            if (rd.Read())
            {
                items.Add(new LabStatusItemDto
                {
                    key = kv.Value,
                    configparam_id = kv.Key,
                    active = true,
                    value_real = rd["value_real"] == DBNull.Value ? null : Convert.ToDouble(rd["value_real"], CultureInfo.InvariantCulture),
                    value_text = rd["value_text"] == DBNull.Value ? null : rd["value_text"]?.ToString()
                });
            }
            else
            {
                items.Add(new LabStatusItemDto
                {
                    key = kv.Value,
                    configparam_id = kv.Key,
                    active = false
                });
            }
        }

        return new { items };
    }

    private static object QueryZone4Series(SqliteConnection conn, string filter, DateTime nowLocal)
    {
        var map = new Dictionary<string, int[]>(StringComparer.OrdinalIgnoreCase)
        {
            ["CW Turbid"] = new[] { 1002, 1014, 1026, 1038 },
            ["CW Pre CL"] = new[] { 1004, 1016, 1028, 1040 },
            ["FW Turbid"] = new[] { 1005, 1017, 1029, 1041 },
            ["TW Turbid"] = new[] { 1044, 1053, 1062, 1065 },
            ["TW Pos CL"] = new[] { 1046, 1055, 1064, 1067 }
        };

        var cfgs = map[filter];
        var series = new List<object>();

        // zone4 ใช้เวลาเลขคี่ล่าสุด เช่น 16:57 => 15:00
        var anchor = GetLatestStepAnchor(nowLocal, stepHours: 2, firstHour: 1);

        for (var i = 0; i < cfgs.Length; i++)
        {
            var cfg = cfgs[i];
            var points = QueryLastNByConfigParamIdUpToAnchor(conn, cfg, anchor, 6);

            series.Add(new
            {
                configparam_id = cfg,
                points = ToSimplePairs(points)
            });
        }

        return new
        {
            series
        };
    }

    private static List<LabPointDto> QueryLastNByConfigParamIdUpToAnchor(
        SqliteConnection conn,
        int configparamId,
        DateTime anchorLocal,
        int take)
    {
        var temp = new List<LabPointDto>();

        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
SELECT
    sample_date,
    sample_time,
    configparam_id,
    equipment_name,
    param_name,
    measure_th,
    value_real,
    value_text
FROM lab_import_daily
WHERE configparam_id = $cfg
  AND (value_real IS NOT NULL OR value_text IS NOT NULL)
  AND (sample_date || ' ' || sample_time) <= $anchor_local
ORDER BY sample_date DESC, sample_time DESC
LIMIT $take;
";
        cmd.Parameters.AddWithValue("$cfg", configparamId);
        cmd.Parameters.AddWithValue("$anchor_local", anchorLocal.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture));
        cmd.Parameters.AddWithValue("$take", take);

        using var rd = cmd.ExecuteReader();
        while (rd.Read())
        {
            var sampleDate = rd["sample_date"]?.ToString() ?? "";
            var sampleTime = rd["sample_time"]?.ToString() ?? "";

            temp.Add(new LabPointDto
            {
                sample_date = sampleDate,
                sample_time = sampleTime,
                ts_local = $"{sampleDate} {sampleTime}",
                configparam_id = Convert.ToInt32(rd["configparam_id"], CultureInfo.InvariantCulture),
                equipment_name = rd["equipment_name"]?.ToString(),
                param_name = rd["param_name"]?.ToString(),
                measure_th = rd["measure_th"]?.ToString(),
                value_real = rd["value_real"] == DBNull.Value ? null : Convert.ToDouble(rd["value_real"], CultureInfo.InvariantCulture),
                value_text = rd["value_text"] == DBNull.Value ? null : rd["value_text"]?.ToString()
            });
        }

        temp.Reverse();
        return temp;
    }

    private static async Task WriteJsonAsync(HttpListenerContext hc, int statusCode, object payload)
    {
        hc.Response.StatusCode = statusCode;
        hc.Response.ContentType = "application/json; charset=utf-8";

        var json = JsonSerializer.Serialize(payload, new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            WriteIndented = false
        });

        var bytes = System.Text.Encoding.UTF8.GetBytes(json);
        hc.Response.ContentLength64 = bytes.Length;
        await hc.Response.OutputStream.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false);
        hc.Response.OutputStream.Close();
    }

    public sealed class LabSummaryRequestDto
    {
        public LabRwZoneDto? RwZone { get; set; }
        public LabZone4Dto? Zone4 { get; set; }
    }

    public sealed class LabRwZoneDto
    {
        public string? Option { get; set; }
        public string? Fix { get; set; }
    }

    public sealed class LabZone4Dto
    {
        public string? Filter { get; set; }
    }

    private sealed class LabPointDto
    {
        public string sample_date { get; set; } = "";
        public string sample_time { get; set; } = "";
        public string ts_local { get; set; } = "";
        public int configparam_id { get; set; }
        public string? equipment_name { get; set; }
        public string? param_name { get; set; }
        public string? measure_th { get; set; }
        public double? value_real { get; set; }
        public string? value_text { get; set; }
    }

    private sealed class LabStatusItemDto
    {
        public string key { get; set; } = "";
        public int configparam_id { get; set; }
        public bool active { get; set; }
        public double? value_real { get; set; }
        public string? value_text { get; set; }
    }
}