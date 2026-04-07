#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using Microsoft.Data.Sqlite;

public sealed record PtcPoint(string hhmm, double upper, double lower);

public interface IPtcSeriesProvider
{
    IReadOnlyList<PtcPoint> GetSeries(string key);
    IReadOnlyList<string> ListKeys();
}

public sealed class SqliteUpperLowerProvider : IPtcSeriesProvider
{
    private readonly string _dbPath;
    private readonly string _cs;

    public SqliteUpperLowerProvider(string dbPath)
    {
        _dbPath = dbPath ?? throw new ArgumentNullException(nameof(dbPath));
        _cs = new SqliteConnectionStringBuilder
        {
            DataSource = _dbPath,
            Mode = SqliteOpenMode.ReadOnly,
            Cache = SqliteCacheMode.Shared,
            Pooling = true
        }.ToString();
    }

    public IReadOnlyList<string> ListKeys()
    {
        if (!File.Exists(_dbPath)) return Array.Empty<string>();

        using var con = new SqliteConnection(_cs);
        con.Open();

        using var cmd = con.CreateCommand();
        cmd.CommandText = @"SELECT DISTINCT code FROM upper_lower ORDER BY code;";

        var keys = new List<string>();
        using var rd = cmd.ExecuteReader();
        while (rd.Read())
        {
            var k = rd.IsDBNull(0) ? null : rd.GetString(0);
            if (!string.IsNullOrWhiteSpace(k)) keys.Add(k.Trim());
        }
        return keys;
    }

    public IReadOnlyList<PtcPoint> GetSeries(string key)
    {
        key = (key ?? "").Trim();
        if (key.Length == 0) return Array.Empty<PtcPoint>();
        if (!File.Exists(_dbPath)) return Array.Empty<PtcPoint>();

        using var con = new SqliteConnection(_cs);
        con.Open();

        using var cmd = con.CreateCommand();
        cmd.CommandText = @"
SELECT hhmm, upper, lower
FROM upper_lower
WHERE code = $code
ORDER BY hhmm;";
        cmd.Parameters.AddWithValue("$code", key);

        var list = new List<PtcPoint>();
        double? v23Upper = null;
        double? v23Lower = null;

        using (var rd = cmd.ExecuteReader())
        {
            while (rd.Read())
            {
                var hhmm = rd.GetString(0);        // TEXT NOT NULL
                var upper = rd.GetDouble(1);       // REAL NOT NULL
                var lower = rd.GetDouble(2);       // REAL NOT NULL

                // เก็บค่าเวลา 23:00 ไว้ใช้ทำ 23:59
                if (string.Equals(hhmm, "23:00", StringComparison.Ordinal))
                {
                    v23Upper = upper;
                    v23Lower = lower;
                }

                list.Add(new PtcPoint(hhmm, upper, lower));
            }
        }

        if (list.Count == 0) return list;

        // ✅ สร้างจุด 23:59 โดยให้ upper/lower เท่ากับของ 23:00
        // ถ้าไม่มี 23:00 ให้ fallback เป็น “แถวสุดท้าย” ที่มีอยู่
        var useUpper = v23Upper ?? list[^1].upper;
        var useLower = v23Lower ?? list[^1].lower;

        // ถ้ามี 23:59 อยู่แล้ว ให้แทนค่าให้เป็นค่าจาก 23:00 (ตาม requirement)
        var idx2359 = list.FindIndex(x => string.Equals(x.hhmm, "23:59", StringComparison.Ordinal));
        if (idx2359 >= 0)
        {
            list[idx2359] = new PtcPoint("23:59", useUpper, useLower);
        }
        else
        {
            list.Add(new PtcPoint("23:59", useUpper, useLower));
        }

        return list;
    }
}