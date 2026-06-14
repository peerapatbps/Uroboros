using System.IO;
using Microsoft.Data.Sqlite;
using Uroboros;
using Xunit;

// PTC is internal; InternalsVisibleTo is declared in Uroboros.csproj.
public sealed class PtcRtuKeysTests
{
    [Fact]
    public void GetNewGraphRtuKeys_Returns18Keys()
    {
        var keys = PTC.GetNewGraphRtuKeys();
        Assert.Equal(18, keys.Count);
    }

    [Fact]
    public void GetNewGraphRtuKeys_ContainsExpectedSamples()
    {
        var keys = PTC.GetNewGraphRtuKeys();
        Assert.Contains("UZ5411", keys);
        Assert.Contains("P046", keys);
        Assert.Contains("U029", keys);
    }

    [Fact]
    public void FetchNewGraphUrlList_Returns18Urls()
    {
        var list = PTC.FetchNewGraphUrlList();
        Assert.Equal(18, list.Count);
    }

    [Fact]
    public void FetchNewGraphUrlList_AllUrlsContainRtuParam()
    {
        foreach (var url in PTC.FetchNewGraphUrlList())
            Assert.Contains("rtu=", url, StringComparison.OrdinalIgnoreCase);
    }
}

public sealed class PtcTryGetTypenameTests
{
    [Fact]
    public void RtuKey_ReturnsCodeWithPSuffix()
    {
        // "UZ5411" is a known RTU key → should return "UZ5411P"
        var code = PTC.TryGetTypename("UZ5411");
        Assert.Equal("UZ5411P", code);
    }

    [Fact]
    public void RtuKeyAlreadyHasP_NoDuplication()
    {
        // Caller passes pressure code "UZ5411P" back in → still "UZ5411P"
        var code = PTC.TryGetTypename("UZ5411P");
        Assert.Equal("UZ5411P", code);
    }

    [Fact]
    public void RtuKey_U029_ReturnsPCode()
    {
        Assert.Equal("U029P", PTC.TryGetTypename("U029"));
    }

    [Fact]
    public void LegacyGensvgUrl_WithTypename_ReturnsTypename()
    {
        // legacy relative URL with typename param
        var code = PTC.TryGetTypename("svgtest/gensvg.php?typename=MH_P1&sdate=2001-1-1&edate=2001-1-1&refdate=2001-1-1");
        Assert.Equal("MH_P1", code);
    }

    [Fact]
    public void QueryStringWithRtu_ReturnsCode()
    {
        var code = PTC.TryGetTypename("?rtu=UZ5411");
        Assert.Equal("UZ5411P", code);
    }

    [Fact]
    public void EmptyString_ReturnsNull()
    {
        Assert.Null(PTC.TryGetTypename(""));
    }

    [Theory]
    [InlineData("random_no_typename")]
    [InlineData("gensvg.php")]
    public void UrlWithNoTypenameAndNotRtu_ReturnsNull(string input)
    {
        Assert.Null(PTC.TryGetTypename(input));
    }
}

public sealed class PtcFetchEmbedSrcListTests
{
    [Fact]
    public async Task NewGraphSeed_String_Returns18Items()
    {
        var list = await PTC.FetchEmbedSrcListAsync("newgraph", CancellationToken.None);
        Assert.Equal(18, list.Count);
    }

    [Fact]
    public async Task NewGraphAuto_String_Returns18Items()
    {
        var list = await PTC.FetchEmbedSrcListAsync("newgraph:auto", CancellationToken.None);
        Assert.Equal(18, list.Count);
    }

    [Fact]
    public async Task EmptyCommonUrl_Returns18Items()
    {
        var list = await PTC.FetchEmbedSrcListAsync("", CancellationToken.None);
        Assert.Equal(18, list.Count);
    }

    [Fact]
    public async Task WhitespaceCommonUrl_Returns18Items()
    {
        var list = await PTC.FetchEmbedSrcListAsync("   ", CancellationToken.None);
        Assert.Equal(18, list.Count);
    }
}

public sealed class PtcDbTests : IDisposable
{
    private readonly string _dbPath;

    public PtcDbTests()
    {
        _dbPath = Path.Combine(Path.GetTempPath(), "ptc_test_" + Guid.NewGuid().ToString("N") + ".db");
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        try { File.Delete(_dbPath); } catch { }
    }

    [Fact]
    public void EnsureDbUpperLower_CreatesUpperLowerTable()
    {
        PTC.EnsureDbUpperLower(_dbPath);

        using var conn = new SqliteConnection($"Data Source={_dbPath};");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name='upper_lower';";
        var result = cmd.ExecuteScalar();
        Assert.NotNull(result);
    }

    [Fact]
    public void SaveUpperLowerToDb_WritesRows_ReadableFromDb()
    {
        PTC.EnsureDbUpperLower(_dbPath);

        var data = new Dictionary<string, (double Ucl, double Lcl)>
        {
            ["00:00"] = (1.5, 0.5),
            ["06:00"] = (2.0, 1.0),
            ["12:00"] = (2.5, 1.5),
        };

        PTC.SaveUpperLowerToDb(_dbPath, "TEST_CODE", data);

        using var conn = new SqliteConnection($"Data Source={_dbPath};");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM upper_lower WHERE code='TEST_CODE';";
        var count = (long)cmd.ExecuteScalar()!;
        Assert.Equal(3, count);
    }

    [Fact]
    public void SaveUpperLowerToDb_CorrectValues_StoredAndReadBack()
    {
        PTC.EnsureDbUpperLower(_dbPath);

        var data = new Dictionary<string, (double Ucl, double Lcl)>
        {
            ["08:00"] = (3.14, 1.59),
        };

        PTC.SaveUpperLowerToDb(_dbPath, "CODE1", data);

        using var conn = new SqliteConnection($"Data Source={_dbPath};");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT hhmm, upper, lower FROM upper_lower WHERE code='CODE1';";
        using var rd = cmd.ExecuteReader();
        Assert.True(rd.Read());
        Assert.Equal("08:00", rd.GetString(0));
        Assert.Equal(3.14, rd.GetDouble(1), precision: 5);
        Assert.Equal(1.59, rd.GetDouble(2), precision: 5);
    }

    [Fact]
    public void SaveUpperLowerToDb_Replaces_ExistingRows_For_SameCode()
    {
        PTC.EnsureDbUpperLower(_dbPath);

        var data1 = new Dictionary<string, (double Ucl, double Lcl)>
        {
            ["00:00"] = (1.0, 0.5),
            ["06:00"] = (2.0, 1.0),
        };
        PTC.SaveUpperLowerToDb(_dbPath, "REPLACE_CODE", data1);

        // Save again with only 1 row — old rows should be gone
        var data2 = new Dictionary<string, (double Ucl, double Lcl)>
        {
            ["12:00"] = (9.9, 8.8),
        };
        PTC.SaveUpperLowerToDb(_dbPath, "REPLACE_CODE", data2);

        using var conn = new SqliteConnection($"Data Source={_dbPath};");
        conn.Open();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM upper_lower WHERE code='REPLACE_CODE';";
        var count = (long)cmd.ExecuteScalar()!;
        Assert.Equal(1, count);
    }

    [Fact]
    public void SaveUpperLowerToDb_NullOrEmptyData_WritesNothing()
    {
        PTC.EnsureDbUpperLower(_dbPath);
        // Should not throw
        PTC.SaveUpperLowerToDb(_dbPath, "X", new Dictionary<string, (double, double)>());
    }

    [Fact]
    public void SaveUpperLowerToDb_EmptyCode_WritesNothing()
    {
        PTC.EnsureDbUpperLower(_dbPath);
        var data = new Dictionary<string, (double Ucl, double Lcl)> { ["00:00"] = (1, 0) };
        // Should not throw
        PTC.SaveUpperLowerToDb(_dbPath, "", data);
        PTC.SaveUpperLowerToDb(_dbPath, "  ", data);
    }
}
