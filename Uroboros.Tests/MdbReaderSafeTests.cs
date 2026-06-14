using System.IO;
using System.Text;
using Uroboros;
using Xunit;

public sealed class MdbReaderSafeUnquoteTests
{
    [Theory]
    [InlineData(null, "")]
    [InlineData("", "")]
    [InlineData("  ", "")]
    [InlineData("hello", "hello")]
    [InlineData("  hello  ", "hello")]
    [InlineData("\"hello\"", "hello")]
    [InlineData("\"hello world\"", "hello world")]
    [InlineData("\"\"", "")]
    // Single quote char — not stripped (only outer double-quotes are removed)
    [InlineData("\"", "\"")]
    // Leading quote only, no trailing — not stripped
    [InlineData("\"hello", "\"hello")]
    public void Unquote_ReturnsExpected(string? input, string expected)
    {
        Assert.Equal(expected, MdbReaderSafe.Unquote(input));
    }
}

public sealed class MdbReaderSafeResolveActiveChemDbTests : IDisposable
{
    private readonly string _root;

    public MdbReaderSafeResolveActiveChemDbTests()
    {
        _root = Path.Combine(Path.GetTempPath(), "UroborosTests_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch { }
    }

    private string Fallback => Path.Combine(_root, "media", "chem.db");

    [Fact]
    public void NoPointerFile_ReturnsFallback()
    {
        var result = MdbReaderSafe.ResolveActiveChemDbPath(_root);
        Assert.Equal(Fallback, result);
    }

    [Fact]
    public void EmptyPointerFile_ReturnsFallback()
    {
        var mediaDir = Path.Combine(_root, "media");
        Directory.CreateDirectory(mediaDir);
        File.WriteAllText(Path.Combine(mediaDir, "chem.current.txt"), "   ");
        var result = MdbReaderSafe.ResolveActiveChemDbPath(_root);
        Assert.Equal(Fallback, result);
    }

    [Fact]
    public void PointerPointsToExistingFile_ReturnsThatFile()
    {
        var mediaDir = Path.Combine(_root, "media");
        Directory.CreateDirectory(mediaDir);
        var snapshotFile = Path.Combine(mediaDir, "chem_20250101_120000.db");
        File.WriteAllBytes(snapshotFile, Array.Empty<byte>());
        File.WriteAllText(Path.Combine(mediaDir, "chem.current.txt"), "chem_20250101_120000.db", Encoding.UTF8);
        var result = MdbReaderSafe.ResolveActiveChemDbPath(_root);
        Assert.Equal(snapshotFile, result);
    }

    [Fact]
    public void PointerPointsToMissingFile_ReturnsFallback()
    {
        var mediaDir = Path.Combine(_root, "media");
        Directory.CreateDirectory(mediaDir);
        File.WriteAllText(Path.Combine(mediaDir, "chem.current.txt"), "chem_doesnotexist.db", Encoding.UTF8);
        var result = MdbReaderSafe.ResolveActiveChemDbPath(_root);
        Assert.Equal(Fallback, result);
    }
}

public sealed class MdbReaderSafeSyncTests : IDisposable
{
    private readonly string _root;

    public MdbReaderSafeSyncTests()
    {
        _root = Path.Combine(Path.GetTempPath(), "UroborosSync_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_root);
    }

    public void Dispose()
    {
        try { Directory.Delete(_root, recursive: true); } catch { }
    }

    [Fact]
    public void SyncMdb_ConfigNotFound_ReturnsConfigNotFound()
    {
        var status = MdbReaderSafe.SyncMdbToMediaAtStartup(
            _root, "config_\\config.ini", out var msg);
        Assert.Equal(MdbReaderSafe.MdbSyncStatus.ConfigNotFound, status);
        Assert.NotNull(msg);
    }

    [Fact]
    public void SyncMdb_SourceMdbMissing_ReturnsSourceNotFound()
    {
        var cfgDir = Path.Combine(_root, "config_");
        Directory.CreateDirectory(cfgDir);
        var cfgFile = Path.Combine(cfgDir, "config.ini");
        File.WriteAllLines(cfgFile, new[]
        {
            "[Production]",
            "mdb_file_location=C:\\nonexistent\\path\\data.mdb"
        }, Encoding.UTF8);

        var status = MdbReaderSafe.SyncMdbToMediaAtStartup(
            _root, "config_\\config.ini", out var msg);
        Assert.Equal(MdbReaderSafe.MdbSyncStatus.SourceNotFound, status);
    }
}
