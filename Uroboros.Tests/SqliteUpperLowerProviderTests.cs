using System.IO;
using Microsoft.Data.Sqlite;
using Xunit;

public sealed class SqliteUpperLowerProviderTests : IDisposable
{
    private readonly string _dbPath;

    public SqliteUpperLowerProviderTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), "ul_test_" + Guid.NewGuid().ToString("N") + ".db");
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        try { File.Delete(_dbPath); } catch { }
    }

    // ── Helper ────────────────────────────────────────────────────

    private void CreateTable()
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
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

    private void InsertRow(string code, string hhmm, double upper, double lower)
    {
        using var conn = new SqliteConnection($"Data Source={_dbPath}");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "INSERT INTO upper_lower(code,hhmm,upper,lower) VALUES($c,$h,$u,$l)";
        cmd.Parameters.AddWithValue("$c", code);
        cmd.Parameters.AddWithValue("$h", hhmm);
        cmd.Parameters.AddWithValue("$u", upper);
        cmd.Parameters.AddWithValue("$l", lower);
        cmd.ExecuteNonQuery();
    }

    // ── GetSeries edge cases ──────────────────────────────────────

    [Fact]
    public void GetSeries_EmptyKey_ReturnsEmpty()
    {
        var p = new SqliteUpperLowerProvider(_dbPath);
        Assert.Empty(p.GetSeries(""));
        Assert.Empty(p.GetSeries("   "));
    }

    [Fact]
    public void GetSeries_MissingFile_ReturnsEmpty()
    {
        var p = new SqliteUpperLowerProvider("/nonexistent/path/db.sqlite");
        Assert.Empty(p.GetSeries("any"));
    }

    [Fact]
    public void GetSeries_DbWithoutTable_ReturnsEmpty()
    {
        // Create a DB but don't create the upper_lower table
        using (var conn = new SqliteConnection($"Data Source={_dbPath}"))
            conn.Open();

        var p = new SqliteUpperLowerProvider(_dbPath);
        Assert.Empty(p.GetSeries("any"));
    }

    // ── ListKeys edge cases ───────────────────────────────────────

    [Fact]
    public void ListKeys_MissingFile_ReturnsEmpty()
    {
        var p = new SqliteUpperLowerProvider("/nonexistent/path/db.sqlite");
        Assert.Empty(p.ListKeys());
    }

    [Fact]
    public void ListKeys_DbWithoutTable_ReturnsEmpty()
    {
        using (var conn = new SqliteConnection($"Data Source={_dbPath}"))
            conn.Open();

        var p = new SqliteUpperLowerProvider(_dbPath);
        Assert.Empty(p.ListKeys());
    }

    // ── Normal data retrieval ─────────────────────────────────────

    [Fact]
    public void ListKeys_ReturnsInsertedCodes()
    {
        CreateTable();
        InsertRow("CODE_A", "00:00", 1, 0);
        InsertRow("CODE_B", "00:00", 2, 1);

        var p = new SqliteUpperLowerProvider(_dbPath);
        var keys = p.ListKeys();
        Assert.Contains("CODE_A", keys);
        Assert.Contains("CODE_B", keys);
    }

    [Fact]
    public void GetSeries_ReturnsCorrectPoints()
    {
        CreateTable();
        InsertRow("SERIES1", "00:00", 1.5, 0.5);
        InsertRow("SERIES1", "06:00", 2.5, 1.5);
        InsertRow("SERIES1", "12:00", 3.0, 2.0);

        var p = new SqliteUpperLowerProvider(_dbPath);
        var series = p.GetSeries("SERIES1");

        // At minimum 3 source rows plus appended 23:59
        Assert.True(series.Count >= 3);
        var pt = series.First(x => x.hhmm == "00:00");
        Assert.Equal(1.5, pt.upper);
        Assert.Equal(0.5, pt.lower);
    }

    // ── 23:59 generation ─────────────────────────────────────────

    [Fact]
    public void GetSeries_With23_00_Row_Appends23_59_WithSameValues()
    {
        CreateTable();
        InsertRow("PT", "00:00", 1.0, 0.0);
        InsertRow("PT", "23:00", 5.5, 4.5);

        var p = new SqliteUpperLowerProvider(_dbPath);
        var series = p.GetSeries("PT");

        var pt2359 = series.FirstOrDefault(x => x.hhmm == "23:59");
        Assert.NotNull(pt2359);
        Assert.Equal(5.5, pt2359!.upper);
        Assert.Equal(4.5, pt2359.lower);
    }

    [Fact]
    public void GetSeries_Without23_00_Row_Appends23_59_FromLastRow()
    {
        CreateTable();
        InsertRow("PT2", "00:00", 1.0, 0.0);
        InsertRow("PT2", "12:00", 3.0, 2.0);

        var p = new SqliteUpperLowerProvider(_dbPath);
        var series = p.GetSeries("PT2");

        // 23:59 should be appended
        Assert.Contains(series, x => x.hhmm == "23:59");
    }

    [Fact]
    public void GetSeries_Existing_23_59_Row_Replaced_By_23_00_Values()
    {
        CreateTable();
        InsertRow("PT3", "00:00", 1.0, 0.0);
        InsertRow("PT3", "23:00", 9.0, 8.0);
        InsertRow("PT3", "23:59", 0.1, 0.1); // old stale value

        var p = new SqliteUpperLowerProvider(_dbPath);
        var series = p.GetSeries("PT3");

        var pt2359 = series.First(x => x.hhmm == "23:59");
        // Should be replaced by 23:00 values, not the original 0.1
        Assert.Equal(9.0, pt2359.upper);
        Assert.Equal(8.0, pt2359.lower);
    }

    [Fact]
    public void GetSeries_NoData_ForCode_ReturnsEmpty()
    {
        CreateTable();
        InsertRow("OTHER", "00:00", 1.0, 0.5);

        var p = new SqliteUpperLowerProvider(_dbPath);
        Assert.Empty(p.GetSeries("MISSING"));
    }
}
