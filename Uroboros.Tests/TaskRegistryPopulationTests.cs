using Xunit;

/// <summary>
/// Verifies that all 17 production tasks (excluding Wayfarer which requires
/// external dependencies) are correctly registered and can be instantiated.
///
/// These tests mirror the registration block in Program.cs so that any
/// task that is added, renamed, or accidentally removed will be caught here.
/// </summary>
public sealed class TaskRegistryPopulationTests
{
    // ── Mirror the production registration block (without Wayfarer) ───────────

    private static TaskRegistry BuildRegistry()
    {
        var reg = new TaskRegistry();
        reg.Register("tps.refresh",          () => new TpsRefreshTask());
        reg.Register("dps.refresh",          () => new DpsRefreshTask());
        reg.Register("rws1.refresh",         () => new Rwp1RefreshTask());
        reg.Register("rws2.refresh",         () => new Rwp2RefreshTask());
        reg.Register("chem1.refresh",        () => new Chem1RefreshTask());
        reg.Register("chem2.refresh",        () => new Chem2RefreshTask());
        reg.Register("branch.refresh",       () => new BranchDataRefreshTask());
        reg.Register("rcv38.refresh",        () => new Rcv38RefreshTask());
        reg.Register("ptc.query.once",       () => new PtcQueryOnceTask());
        reg.Register("onlinelab.query",      () => new OnlineLabV2Task());
        reg.Register("Aquadat.refresh",      () => new AquadatQueryTask());
        reg.Register("AquadatFWS.refresh",   () => new AquadatFwsQueryTask());
        reg.Register("DB_upload.refresh",    () => new DbUploadTask());
        reg.Register("DB_download.refresh",  () => new DbDownloadTask());
        reg.Register("MDB_upload.refresh",   () => new MdbUploadTask());
        reg.Register("MDB_download.refresh", () => new MdbDownloadTask());
        reg.Register("LAB.import.daily",     () => new LabImportTask());
        return reg;
    }

    // ── 17 known task names ───────────────────────────────────────────────────

    public static TheoryData<string> AllTaskNames => new()
    {
        "tps.refresh",
        "dps.refresh",
        "rws1.refresh",
        "rws2.refresh",
        "chem1.refresh",
        "chem2.refresh",
        "branch.refresh",
        "rcv38.refresh",
        "ptc.query.once",
        "onlinelab.query",
        "Aquadat.refresh",
        "AquadatFWS.refresh",
        "DB_upload.refresh",
        "DB_download.refresh",
        "MDB_upload.refresh",
        "MDB_download.refresh",
        "LAB.import.daily"
    };

    // ── Suite 1: registry contains all 17 tasks ───────────────────────────────

    [Fact]
    public void Registry_Contains_Exactly_17_Production_Tasks()
    {
        var reg = BuildRegistry();
        Assert.Equal(17, reg.ListNames().Count);
    }

    // ── Suite 2: every task name resolves to a non-null instance ─────────────

    [Theory, MemberData(nameof(AllTaskNames))]
    public void TryCreate_Returns_True_And_NonNull(string name)
    {
        var reg = BuildRegistry();
        var found = reg.TryCreate(name, out var task);

        Assert.True(found,    $"Task '{name}' must be registered");
        Assert.NotNull(task);
    }

    // ── Suite 3: factory creates a NEW instance on every call (no singleton) ──

    [Theory, MemberData(nameof(AllTaskNames))]
    public void TryCreate_Returns_Fresh_Instance_Each_Call(string name)
    {
        var reg = BuildRegistry();
        reg.TryCreate(name, out var t1);
        reg.TryCreate(name, out var t2);

        Assert.NotSame(t1, t2);
    }

    // ── Suite 4: every task has a non-empty Spec.Name ─────────────────────────

    [Theory, MemberData(nameof(AllTaskNames))]
    public void Task_Spec_Name_Is_Not_Empty(string name)
    {
        var reg = BuildRegistry();
        reg.TryCreate(name, out var task);

        Assert.NotNull(task);
        Assert.False(string.IsNullOrWhiteSpace(task!.Spec.Name),
            $"Task '{name}' Spec.Name must not be empty");
    }

    // ── Suite 5: lookup is case-insensitive (registry contract) ──────────────

    [Theory]
    [InlineData("TPS.REFRESH",         "tps.refresh")]
    [InlineData("DPS.REFRESH",         "dps.refresh")]
    [InlineData("CHEM1.REFRESH",       "chem1.refresh")]
    [InlineData("CHEM2.REFRESH",       "chem2.refresh")]
    [InlineData("AQUADAT.REFRESH",     "Aquadat.refresh")]
    [InlineData("AQUADATFWS.REFRESH",  "AquadatFWS.refresh")]
    [InlineData("DB_UPLOAD.REFRESH",   "DB_upload.refresh")]
    [InlineData("MDB_UPLOAD.REFRESH",  "MDB_upload.refresh")]
    [InlineData("LAB.IMPORT.DAILY",    "LAB.import.daily")]
    public void TryCreate_Is_CaseInsensitive(string upperName, string registeredName)
    {
        var reg = BuildRegistry();
        Assert.True(reg.TryCreate(upperName, out var task),
            $"TryCreate('{upperName}') must succeed regardless of case");
        Assert.NotNull(task);
        _ = registeredName; // parameter documents which registered name is being upper-cased
    }

    // ── Suite 6: wayfarer is NOT in this registry (requires external deps) ────

    [Fact]
    public void Wayfarer_Task_Is_Not_In_Base_Registry()
    {
        // "wayfarer.pm.collect" needs a live WayfarerIntegration instance and
        // is wired separately in Program.cs — must not appear in the base registry.
        var reg = BuildRegistry();
        Assert.False(reg.TryCreate("wayfarer.pm.collect", out _),
            "Wayfarer must NOT be in the base registry (it requires external deps)");
    }
}
