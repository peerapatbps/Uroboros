using System.Globalization;
using System.Text.Json;
using ClosedXML.Excel;
using Microsoft.Data.Sqlite;

namespace LabImportCli;

public static class DailyLabImporter
{
    public sealed class ImportResult
    {
        public string ReportDateText { get; init; } = "";
        public string ExcelPath { get; init; } = "";
        public string SheetName { get; init; } = "";
        public int RowsWritten { get; set; }
        public int DistinctCfgCount { get; set; }
    }

    private sealed class LabStructureItem
    {
        public int configparam_id { get; set; }
        public string? plant_en { get; set; }
        public string? station_name { get; set; }
        public string? Param_name { get; set; }
        public string? equipment_name { get; set; }
        public string? measure_th { get; set; }
    }

    private sealed class LabMappingRoot
    {
        public int version { get; set; }
        public List<LabMappingBlock>? blocks { get; set; }
    }

    private sealed class LabMappingBlock
    {
        public string? name { get; set; }
        public LabAnchor? anchor { get; set; }
        public LabTimeSeries? time { get; set; }
        public List<LabMappingItem>? mappings { get; set; }
    }

    private sealed class LabAnchor
    {
        public string? find_text { get; set; }
    }

    private sealed class LabTimeSeries
    {
        public string? type { get; set; }
        public string? start { get; set; }
        public int interval_minutes { get; set; }
        public int count { get; set; }
    }

    private sealed class LabMappingItem
    {
        public int cfg { get; set; }
        public string? label { get; set; }
        public LabCellSeries? cell_series { get; set; }
        public List<LabPoint>? points { get; set; }
    }

    private sealed class LabCellSeries
    {
        public string? col { get; set; }
        public int start_row { get; set; }
        public int step_rows { get; set; }
        public int count { get; set; }
    }

    private sealed class LabPoint
    {
        public string? time { get; set; }
        public string? cell { get; set; }
    }

    private sealed record LabValueRow(
        string SampleDate,
        string SampleTime,
        int ConfigParamId,
        string EquipmentName,
        string ParamName,
        string MeasureTh,
        double? ValueReal,
        string? ValueText);

    public static ImportResult ImportDaily(string reportDateText, string? startupPath = null)
    {
        startupPath ??= AppContext.BaseDirectory;
        var startup = Path.GetFullPath(startupPath);

        if (!DateTime.TryParseExact(
                reportDateText.Trim(),
                "yyyy-MM-dd",
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var reportDate))
        {
            throw new ArgumentException("ReportDate must be yyyy-MM-dd", nameof(reportDateText));
        }

        var configDir = Path.Combine(startup, "config_");
        var structurePath = Path.Combine(configDir, "LAB_structure.json");
        var mappingPath = Path.Combine(configDir, "lab_mapping.json");
        var iniPath = Path.Combine(configDir, "config.ini");
        var dbPath = Path.Combine(startup, "data.db");

        if (!File.Exists(structurePath))
            throw new FileNotFoundException("LAB_structure.json not found", structurePath);
        if (!File.Exists(mappingPath))
            throw new FileNotFoundException("lab_mapping.json not found", mappingPath);
        if (!File.Exists(iniPath))
            throw new FileNotFoundException("config.ini not found", iniPath);

        var structure = ReadStructure(structurePath);
        var structureMap = structure
            .Where(x => x.configparam_id > 0)
            .GroupBy(x => x.configparam_id)
            .ToDictionary(g => g.Key, g => g.First());

        var mapping = ReadMapping(mappingPath);
        var basePath = ReadIniValue(iniPath, "Lab_file_location");
        if (string.IsNullOrWhiteSpace(basePath))
            throw new InvalidOperationException("Missing Lab_file_location in config_/config.ini");

        // ใช้ 2 วันเสมอ: เมื่อวาน + วันนี้ (อิงจาก reportDate ที่ส่งเข้า)
        var targetDates = new[]
        {
            reportDate.Date.AddDays(-1),
            reportDate.Date
        };

        var rows = new List<LabValueRow>();
        var excelPathsUsed = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var sheetNamesUsed = new List<string>();

        foreach (var targetDate in targetDates)
        {
            var oneDayRows = ReadRowsFromSingleDate(
                basePath,
                targetDate,
                mapping,
                structureMap,
                out var excelPath,
                out var sheetName);

            rows.AddRange(oneDayRows);
            excelPathsUsed.Add(excelPath);
            sheetNamesUsed.Add(sheetName);
        }

        UpsertIntoSqlite(dbPath, rows, targetDates);

        return new ImportResult
        {
            ReportDateText = reportDateText,
            ExcelPath = string.Join(" | ", excelPathsUsed),
            SheetName = string.Join(" | ", sheetNamesUsed.Distinct()),
            RowsWritten = rows.Count,
            DistinctCfgCount = rows.Select(x => x.ConfigParamId).Distinct().Count()
        };
    }

    private static List<LabValueRow> ReadRowsFromSingleDate(
        string basePath,
        DateTime targetDate,
        LabMappingRoot mapping,
        Dictionary<int, LabStructureItem> structureMap,
        out string excelPath,
        out string sheetName)
    {
        excelPath = ResolveDailyLabExcelPath(basePath, targetDate);
        sheetName = targetDate.Day.ToString(CultureInfo.InvariantCulture);

        var sheetNameLocal = sheetName;

        var rows = new List<LabValueRow>();

        using var workbook = new XLWorkbook(excelPath);

        var worksheet = workbook.Worksheets
            .FirstOrDefault(x => string.Equals(x.Name.Trim(), sheetNameLocal, StringComparison.OrdinalIgnoreCase));

        if (worksheet is null)
            throw new InvalidOperationException($"Sheet not found for day: {sheetNameLocal}, file: {excelPath}");

        foreach (var block in mapping.blocks ?? Enumerable.Empty<LabMappingBlock>())
        {
            var times = BuildFixedTimes(block.time);

            foreach (var mapItem in block.mappings ?? Enumerable.Empty<LabMappingItem>())
            {
                if (mapItem.cfg <= 0)
                    continue;

                structureMap.TryGetValue(mapItem.cfg, out var meta);
                var equipmentName = SafeText(meta?.equipment_name);
                var paramName = SafeText(meta?.Param_name);
                var measureTh = SafeText(meta?.measure_th);

                if (mapItem.points is { Count: > 0 })
                {
                    foreach (var pt in mapItem.points)
                    {
                        if (!TryParseTimeSpan(pt.time, out var ts) || string.IsNullOrWhiteSpace(pt.cell))
                            continue;

                        var sampleDate = targetDate.Date;
                        if (ts >= TimeSpan.FromHours(24))
                        {
                            sampleDate = sampleDate.AddDays(ts.Days);
                            ts = new TimeSpan(ts.Hours, ts.Minutes, 0);
                        }

                        var cellRef = pt.cell.Trim();
                        var cellValue = GetCellValueSafe(worksheet, cellRef);

                        rows.Add(CreateRow(
                            sampleDate,
                            ts,
                            mapItem.cfg,
                            equipmentName,
                            paramName,
                            measureTh,
                            cellValue));
                    }
                }
                else if (mapItem.cell_series is not null && times.Count > 0)
                {
                    var cells = BuildCellSeries(mapItem.cell_series);
                    var count = Math.Min(times.Count, cells.Count);

                    for (var i = 0; i < count; i++)
                    {
                        var ts = times[i];
                        var sampleDate = targetDate.Date;
                        if (ts >= TimeSpan.FromHours(24))
                        {
                            sampleDate = sampleDate.AddDays(ts.Days);
                            ts = new TimeSpan(ts.Hours, ts.Minutes, 0);
                        }

                        var cellRef = cells[i];
                        var cellValue = GetCellValueSafe(worksheet, cellRef);

                        rows.Add(CreateRow(
                            sampleDate,
                            ts,
                            mapItem.cfg,
                            equipmentName,
                            paramName,
                            measureTh,
                            cellValue));
                    }
                }
            }
        }

        return rows;
    }

    private static LabValueRow CreateRow(
        DateTime sampleDate,
        TimeSpan sampleTime,
        int cfg,
        string equipmentName,
        string paramName,
        string measureTh,
        object? value)
    {
        double? valueReal = null;
        string? valueText = null;

        if (value is not null)
        {
            switch (value)
            {
                case double d:
                    valueReal = d;
                    valueText = d.ToString(CultureInfo.InvariantCulture);
                    break;

                case decimal m:
                    valueReal = (double)m;
                    valueText = m.ToString(CultureInfo.InvariantCulture);
                    break;

                case int i:
                    valueReal = i;
                    valueText = i.ToString(CultureInfo.InvariantCulture);
                    break;

                case long l:
                    valueReal = l;
                    valueText = l.ToString(CultureInfo.InvariantCulture);
                    break;

                default:
                    {
                        var rawText = Convert.ToString(value, CultureInfo.InvariantCulture)?.Trim();

                        if (!string.IsNullOrWhiteSpace(rawText))
                        {
                            valueText = rawText;

                            if (double.TryParse(rawText, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedInvariant))
                                valueReal = parsedInvariant;
                            else if (double.TryParse(rawText, NumberStyles.Any, CultureInfo.CurrentCulture, out var parsedCurrent))
                                valueReal = parsedCurrent;
                        }

                        break;
                    }
            }
        }

        return new LabValueRow(
            sampleDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            sampleTime.ToString(@"hh\:mm", CultureInfo.InvariantCulture),
            cfg,
            equipmentName,
            paramName,
            measureTh,
            valueReal,
            valueText);
    }

    private static void UpsertIntoSqlite(string dbPath, IReadOnlyList<LabValueRow> rows, IReadOnlyList<DateTime> targetDates)
    {
        var cs = new SqliteConnectionStringBuilder
        {
            DataSource = dbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared
        }.ToString();

        using var conn = new SqliteConnection(cs);
        conn.Open();

        using (var pragma = conn.CreateCommand())
        {
            pragma.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;";
            pragma.ExecuteNonQuery();
        }

        using (var create = conn.CreateCommand())
        {
            create.CommandText = @"
CREATE TABLE IF NOT EXISTS lab_import_daily (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_date    TEXT    NOT NULL,
    sample_time    TEXT    NOT NULL,
    configparam_id INTEGER NOT NULL,
    equipment_name TEXT    NULL,
    param_name     TEXT    NULL,
    measure_th     TEXT    NULL,
    value_real     REAL    NULL,
    value_text     TEXT    NULL
);

CREATE INDEX IF NOT EXISTS ix_lab_import_daily_cfg_dt
    ON lab_import_daily(configparam_id, sample_date, sample_time);
";
            create.ExecuteNonQuery();
        }

        using (var ensureColumn = conn.CreateCommand())
        {
            ensureColumn.CommandText = @"
ALTER TABLE lab_import_daily ADD COLUMN value_text TEXT NULL;
";
            try
            {
                ensureColumn.ExecuteNonQuery();
            }
            catch (SqliteException ex) when (ex.Message.Contains("duplicate column name", StringComparison.OrdinalIgnoreCase))
            {
                // มีคอลัมน์นี้แล้ว
            }
        }

        var dateTexts = targetDates
            .Select(x => x.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture))
            .Distinct()
            .ToArray();

        if (dateTexts.Length != 2)
            throw new InvalidOperationException("Expected exactly 2 target dates.");

        using var tx = conn.BeginTransaction();

        // 1) ลบทุกอย่างที่ไม่ใช่ 2 วันที่ต้องเก็บ
        using (var deleteOthers = conn.CreateCommand())
        {
            deleteOthers.Transaction = tx;
            deleteOthers.CommandText = @"
DELETE FROM lab_import_daily
WHERE sample_date NOT IN ($d1, $d2);
";
            deleteOthers.Parameters.AddWithValue("$d1", dateTexts[0]);
            deleteOthers.Parameters.AddWithValue("$d2", dateTexts[1]);
            deleteOthers.ExecuteNonQuery();
        }

        // 2) ลบข้อมูลของ 2 วันนี้ก่อน เพื่อ insert ใหม่แบบ clean
        using (var deleteTargets = conn.CreateCommand())
        {
            deleteTargets.Transaction = tx;
            deleteTargets.CommandText = @"
DELETE FROM lab_import_daily
WHERE sample_date IN ($d1, $d2);
";
            deleteTargets.Parameters.AddWithValue("$d1", dateTexts[0]);
            deleteTargets.Parameters.AddWithValue("$d2", dateTexts[1]);
            deleteTargets.ExecuteNonQuery();
        }

        // 3) insert ใหม่
        using var insert = conn.CreateCommand();
        insert.Transaction = tx;
        insert.CommandText = @"
INSERT INTO lab_import_daily (
    sample_date,
    sample_time,
    configparam_id,
    equipment_name,
    param_name,
    measure_th,
    value_real,
    value_text
)
VALUES (
    $sample_date,
    $sample_time,
    $configparam_id,
    $equipment_name,
    $param_name,
    $measure_th,
    $value_real,
    $value_text
);
";

        var pSampleDate = insert.Parameters.Add("$sample_date", SqliteType.Text);
        var pSampleTime = insert.Parameters.Add("$sample_time", SqliteType.Text);
        var pCfg = insert.Parameters.Add("$configparam_id", SqliteType.Integer);
        var pEquip = insert.Parameters.Add("$equipment_name", SqliteType.Text);
        var pParam = insert.Parameters.Add("$param_name", SqliteType.Text);
        var pMeasure = insert.Parameters.Add("$measure_th", SqliteType.Text);
        var pReal = insert.Parameters.Add("$value_real", SqliteType.Real);
        var pText = insert.Parameters.Add("$value_text", SqliteType.Text);

        foreach (var row in rows)
        {
            pSampleDate.Value = row.SampleDate;
            pSampleTime.Value = row.SampleTime;
            pCfg.Value = row.ConfigParamId;
            pEquip.Value = DbValue(row.EquipmentName);
            pParam.Value = DbValue(row.ParamName);
            pMeasure.Value = DbValue(row.MeasureTh);
            pReal.Value = row.ValueReal.HasValue ? row.ValueReal.Value : DBNull.Value;
            pText.Value = DbValue(row.ValueText);
            insert.ExecuteNonQuery();
        }

        tx.Commit();
    }

    private static object DbValue(string? value)
        => string.IsNullOrWhiteSpace(value) ? DBNull.Value : value;

    private static List<LabStructureItem> ReadStructure(string path)
    {
        var jsonText = File.ReadAllText(path);
        return JsonSerializer.Deserialize<List<LabStructureItem>>(
                   jsonText,
                   new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
               ?? new List<LabStructureItem>();
    }

    private static LabMappingRoot ReadMapping(string path)
    {
        var jsonText = File.ReadAllText(path);
        return JsonSerializer.Deserialize<LabMappingRoot>(
                   jsonText,
                   new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
               ?? new LabMappingRoot();
    }

    private static List<TimeSpan> BuildFixedTimes(LabTimeSeries? ts)
    {
        var result = new List<TimeSpan>();
        if (ts is null)
            return result;
        if (!string.Equals(ts.type, "fixed_series", StringComparison.OrdinalIgnoreCase))
            return result;
        if (!TryParseTimeSpan(ts.start, out var startTs))
            return result;

        var stepMin = Math.Max(1, ts.interval_minutes);
        var count = Math.Max(0, ts.count);

        for (var i = 0; i < count; i++)
            result.Add(startTs.Add(TimeSpan.FromMinutes(stepMin * i)));

        return result;
    }

    private static List<string> BuildCellSeries(LabCellSeries? cs)
    {
        var result = new List<string>();
        if (cs is null)
            return result;

        var col = (cs.col ?? "").Trim().ToUpperInvariant();
        var startRow = cs.start_row;
        var stepRows = cs.step_rows <= 0 ? 1 : cs.step_rows;
        var count = Math.Max(0, cs.count);

        for (var i = 0; i < count; i++)
        {
            var row = startRow + (i * stepRows);
            result.Add($"{col}{row.ToString(CultureInfo.InvariantCulture)}");
        }

        return result;
    }

    private static bool TryParseTimeSpan(string? text, out TimeSpan timeSpan)
    {
        timeSpan = TimeSpan.Zero;
        if (string.IsNullOrWhiteSpace(text))
            return false;

        var value = text.Trim().Replace('.', ':');

        if (DateTime.TryParseExact(
                value,
                new[] { "H:mm", "HH:mm" },
                CultureInfo.InvariantCulture,
                DateTimeStyles.None,
                out var dt))
        {
            timeSpan = new TimeSpan(dt.Hour, dt.Minute, 0);
            return true;
        }

        return TimeSpan.TryParse(value, CultureInfo.InvariantCulture, out timeSpan);
    }

    private static object? GetCellValueSafe(IXLWorksheet worksheet, string a1)
    {
        if (string.IsNullOrWhiteSpace(a1))
            return null;

        var cell = worksheet.Cell(a1.Trim());
        if (cell.IsEmpty())
            return null;

        if (cell.DataType == XLDataType.Number)
            return cell.GetDouble();

        var text = cell.GetString().Trim();
        if (text.Length == 0)
            return null;

        if (double.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var invariant))
            return invariant;
        if (double.TryParse(text, NumberStyles.Any, CultureInfo.CurrentCulture, out var current))
            return current;

        return text;
    }

    private static string? ReadIniValue(string iniPath, string key)
    {
        foreach (var line in File.ReadAllLines(iniPath))
        {
            var s = line.Trim();
            if (s.Length == 0 || s.StartsWith('#') || s.StartsWith(';'))
                continue;

            var idx = s.IndexOf('=');
            if (idx <= 0)
                continue;

            var currentKey = s[..idx].Trim();
            if (!string.Equals(currentKey, key, StringComparison.OrdinalIgnoreCase))
                continue;

            return s[(idx + 1)..].Trim().Trim('"');
        }

        return null;
    }

    private static string ResolveDailyLabExcelPath(string basePath, DateTime date)
    {
        var monthAbbr = date.ToString("MMM", CultureInfo.InvariantCulture);
        var buddhistYear2 = ((date.Year + 543) % 100).ToString("00", CultureInfo.InvariantCulture);

        var candidates = new[]
        {
            Path.Combine(basePath, $"{date.Month:00}DailyLab_{monthAbbr}{buddhistYear2}.xlsx"),
            Path.Combine(basePath, $"{date.Month}DailyLab_{monthAbbr}{buddhistYear2}.xlsx")
        };

        foreach (var path in candidates)
        {
            if (File.Exists(path))
                return path;
        }

        throw new FileNotFoundException(
            $"LAB excel file not found. Tried: {string.Join(" | ", candidates)}");
    }

    private static string SafeText(string? value)
        => string.IsNullOrWhiteSpace(value) ? string.Empty : value.Trim();
}