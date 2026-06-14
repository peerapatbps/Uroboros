using Microsoft.Data.Sqlite;

namespace Uroboros.SelfTest;

/// <summary>Self-tests for <see cref="SqliteUpperLowerProvider"/>.</summary>
internal static class ST_SqliteProvider
{
    // ── Helpers ───────────────────────────────────────────────────────────────

    private static string MakeTempDb()
    {
        var path = Path.Combine(Path.GetTempPath(),
            "uro_st_" + Guid.NewGuid().ToString("N") + ".db");
        return path;
    }

    private static void CleanupDb(string path)
    {
        SqliteConnection.ClearAllPools();
        try { File.Delete(path); } catch { /* best-effort */ }
    }

    private static void CreateTable(string dbPath)
    {
        using var conn = new SqliteConnection($"Data Source={dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS upper_lower (
  code       TEXT NOT NULL,
  hhmm       TEXT NOT NULL,
  upper      REAL NOT NULL,
  lower      REAL NOT NULL,
  scraped_at TEXT NOT NULL DEFAULT (datetime('now','localtime')),
  PRIMARY KEY (code, hhmm)
);";
        cmd.ExecuteNonQuery();
    }

    private static void InsertRow(string dbPath, string code, string hhmm,
                                   double upper, double lower)
    {
        using var conn = new SqliteConnection($"Data Source={dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO upper_lower(code,hhmm,upper,lower) VALUES($c,$h,$u,$l)";
        cmd.Parameters.AddWithValue("$c", code);
        cmd.Parameters.AddWithValue("$h", hhmm);
        cmd.Parameters.AddWithValue("$u", upper);
        cmd.Parameters.AddWithValue("$l", lower);
        cmd.ExecuteNonQuery();
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    public static IEnumerable<SelfTestCase> Run()
    {
        yield return SelfTestRunner.Run("SqliteProvider | GetSeries empty key → empty", () =>
        {
            var p = new SqliteUpperLowerProvider("/nonexistent/db.sqlite");
            SelfTestRunner.Assert(p.GetSeries("").Count == 0);
            SelfTestRunner.Assert(p.GetSeries("   ").Count == 0);
        });

        yield return SelfTestRunner.Run("SqliteProvider | GetSeries missing file → empty", () =>
        {
            var p = new SqliteUpperLowerProvider("/nonexistent/path/db.sqlite");
            SelfTestRunner.Assert(p.GetSeries("any").Count == 0);
        });

        yield return SelfTestRunner.Run("SqliteProvider | ListKeys missing file → empty", () =>
        {
            var p = new SqliteUpperLowerProvider("/nonexistent/path/db.sqlite");
            SelfTestRunner.Assert(p.ListKeys().Count == 0);
        });

        yield return SelfTestRunner.Run("SqliteProvider | DB without table → empty series", () =>
        {
            var db = MakeTempDb();
            try
            {
                // Create DB but no table
                using (var conn = new SqliteConnection($"Data Source={db}"))
                    conn.Open();
                var p = new SqliteUpperLowerProvider(db);
                SelfTestRunner.Assert(p.GetSeries("any").Count == 0);
            }
            finally { CleanupDb(db); }
        });

        yield return SelfTestRunner.Run("SqliteProvider | ListKeys returns inserted codes", () =>
        {
            var db = MakeTempDb();
            try
            {
                CreateTable(db);
                InsertRow(db, "CODE_A", "00:00", 1, 0);
                InsertRow(db, "CODE_B", "00:00", 2, 1);

                var p    = new SqliteUpperLowerProvider(db);
                var keys = p.ListKeys();
                SelfTestRunner.Assert(keys.Contains("CODE_A"), "missing CODE_A");
                SelfTestRunner.Assert(keys.Contains("CODE_B"), "missing CODE_B");
            }
            finally { CleanupDb(db); }
        });

        yield return SelfTestRunner.Run("SqliteProvider | GetSeries returns correct data points", () =>
        {
            var db = MakeTempDb();
            try
            {
                CreateTable(db);
                InsertRow(db, "S1", "00:00", 1.5, 0.5);
                InsertRow(db, "S1", "06:00", 2.5, 1.5);
                InsertRow(db, "S1", "12:00", 3.0, 2.0);

                var p      = new SqliteUpperLowerProvider(db);
                var series = p.GetSeries("S1");
                SelfTestRunner.Assert(series.Count >= 3, $"Expected ≥3, got {series.Count}");

                var pt = series.First(x => x.hhmm == "00:00");
                SelfTestRunner.AssertEqual(1.5, pt.upper);
                SelfTestRunner.AssertEqual(0.5, pt.lower);
            }
            finally { CleanupDb(db); }
        });

        yield return SelfTestRunner.Run("SqliteProvider | 23:00 row → 23:59 appended with same values", () =>
        {
            var db = MakeTempDb();
            try
            {
                CreateTable(db);
                InsertRow(db, "PT", "00:00", 1.0, 0.0);
                InsertRow(db, "PT", "23:00", 5.5, 4.5);

                var p      = new SqliteUpperLowerProvider(db);
                var series = p.GetSeries("PT");

                var pt2359 = series.FirstOrDefault(x => x.hhmm == "23:59");
                SelfTestRunner.AssertNotNull(pt2359);
                SelfTestRunner.AssertEqual(5.5, pt2359!.upper);
                SelfTestRunner.AssertEqual(4.5, pt2359.lower);
            }
            finally { CleanupDb(db); }
        });

        yield return SelfTestRunner.Run("SqliteProvider | stale 23:59 overwritten by 23:00 values", () =>
        {
            var db = MakeTempDb();
            try
            {
                CreateTable(db);
                InsertRow(db, "PT3", "00:00",  1.0, 0.0);
                InsertRow(db, "PT3", "23:00",  9.0, 8.0);
                InsertRow(db, "PT3", "23:59",  0.1, 0.1); // stale

                var p      = new SqliteUpperLowerProvider(db);
                var series = p.GetSeries("PT3");

                var pt = series.First(x => x.hhmm == "23:59");
                SelfTestRunner.AssertEqual(9.0, pt.upper, "23:59 upper should be 23:00 value");
                SelfTestRunner.AssertEqual(8.0, pt.lower, "23:59 lower should be 23:00 value");
            }
            finally { CleanupDb(db); }
        });

        yield return SelfTestRunner.Run("SqliteProvider | missing code returns empty series", () =>
        {
            var db = MakeTempDb();
            try
            {
                CreateTable(db);
                InsertRow(db, "OTHER", "00:00", 1.0, 0.5);

                var p = new SqliteUpperLowerProvider(db);
                SelfTestRunner.Assert(p.GetSeries("MISSING").Count == 0);
            }
            finally { CleanupDb(db); }
        });
    }
}
