// How it works
// 1) Trigger เข้ามา (timer / web listener / internal signal)
// 2) Scheduler ตัดสินใจจาก TaskSpec (RunPolicy/Timeout)
// 3) ถ้ารัน → สร้าง CancellationToken (global stop + per-task timeout)
// 4) เรียก task.ExecuteAsync(ctx, ct)
// 5) await จน Task จบ
// 6) log / release slot / handle coalesce
//
// NOTE: โค้ดนี้เป็น "โครง" ให้ CLI engine (port 8888) รับคำสั่งผ่าน HTTP
// ตามโมเดล: Web(5082) -> CLI(8888) และ WinForms อ่าน DB clone (read-only)

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Google.Apis.Drive.v3;
using LabImportCli;
using Microsoft.Data.Sqlite;
using Uroboros;
using static Google.Apis.Requests.BatchRequest;
using static Uroboros.AquadatFastCli;

// ==============================
// Scheduler model
// ==============================
#region Scheduler Model
public enum TaskPriority { Low = 0, Normal = 1, High = 2, Critical = 3 }
public enum RunPolicy { Queue, DropIfRunning, CoalesceIfRunning, SkipIfRunning }

public sealed record TaskSpec(
    string Name,
    string Group,
    TaskPriority Priority = TaskPriority.Normal,
    RunPolicy Policy = RunPolicy.Queue,
    TimeSpan? Timeout = null
);

public interface IEngineTask
{
    TaskSpec Spec { get; }
    Task ExecuteAsync(EngineContext ctx, CancellationToken ct);
}

public sealed class EngineContext
{
    public required ILogger Log { get; init; }
    public required IDbService Db { get; init; }
    public required IClock Clock { get; init; }
}

public sealed class RunningTaskInfo
{
    public required Guid Id { get; init; }
    public required string Name { get; init; }
    public required string Group { get; init; }
    public required DateTimeOffset StartedAt { get; init; }
    public required TaskPriority Priority { get; init; }
    public required CancellationTokenSource Cts { get; init; }
}

// ==============================
// Stage gate (ตามแนวคิด partition / clone)
// ==============================

public sealed class StageGate
{
    private int _paused; // 0/1
    public bool IsPaused => Volatile.Read(ref _paused) == 1;

    public StageGate(bool startPaused = false)
    {
        _paused = startPaused ? 1 : 0;
    }

    public void Pause() => Interlocked.Exchange(ref _paused, 1);
    public void Resume() => Interlocked.Exchange(ref _paused, 0);
}


// ==============================
// Scheduler
// ==============================

public sealed class Scheduler
{
    private readonly EngineContext _ctx;
    private readonly int _maxConcurrency;
    private readonly SemaphoreSlim _sem;
    private readonly Channel<IEngineTask> _inbox;
    private readonly StageGate _gate;

    // NEW: per-task enabled gate
    private readonly Func<string, bool> _isTaskEnabled;

    // NEW: health tracker (optional)
    private readonly TaskHealthTracker? _health;

    private readonly ConcurrentDictionary<string, int> _coalescePending = new(); // name -> 1
    private readonly ConcurrentDictionary<Guid, RunningTaskInfo> _running = new();

    public Scheduler(EngineContext ctx, int maxConcurrency, StageGate gate,
        Func<string, bool>? isTaskEnabled = null,
        TaskHealthTracker? health = null)
    {
        _ctx = ctx;
        _gate = gate;
        _maxConcurrency = Math.Max(1, maxConcurrency);
        _sem = new SemaphoreSlim(_maxConcurrency, _maxConcurrency);
        _isTaskEnabled = isTaskEnabled ?? (_ => true);
        _health = health;

        _inbox = Channel.CreateUnbounded<IEngineTask>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });
    }

    public bool TryEnqueue(IEngineTask task)
    {
        if (_gate.IsPaused) return false;

        var spec = task.Spec;

        // NEW: per-task enabled
        if (!_isTaskEnabled(spec.Name))
            return false;

        if (spec.Policy == RunPolicy.CoalesceIfRunning)
        {
            if (IsRunningByName(spec.Name))
            {
                _coalescePending[spec.Name] = 1;
                return true;
            }
        }

        if (spec.Policy == RunPolicy.DropIfRunning)
        {
            if (IsRunningByName(spec.Name)) return false;
        }

        if (spec.Policy == RunPolicy.SkipIfRunning)
        {
            if (IsRunningByName(spec.Name)) return true;
        }

        return _inbox.Writer.TryWrite(task);
    }

    public IReadOnlyCollection<RunningTaskInfo> SnapshotRunning()
        => _running.Values.OrderBy(x => x.StartedAt).ToList();

    public bool Cancel(Guid taskId)
    {
        if (_running.TryGetValue(taskId, out var info))
        {
            info.Cts.Cancel();
            return true;
        }
        return false;
    }

    public bool CancelAll()
    {
        var any = false;
        foreach (var kv in _running)
        {
            kv.Value.Cts.Cancel();
            any = true;
        }
        return any;
    }

    private bool IsRunningByName(string name)
        => _running.Values.Any(x => x.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

    public async Task RunLoopAsync(CancellationToken ct)
    {
        _ctx.Log.Info($"[SCHED] Start, maxConcurrency={_maxConcurrency}");

        while (!ct.IsCancellationRequested)
        {
            IEngineTask task;
            try
            {
                task = await _inbox.Reader.ReadAsync(ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            await _sem.WaitAsync(ct);

            _ = Task.Run(async () =>
            {
                var spec = task.Spec;
                var id = Guid.NewGuid();
                var started = DateTimeOffset.UtcNow;

                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

                if (spec.Timeout is { } to)
                    cts.CancelAfter(to);

                var info = new RunningTaskInfo
                {
                    Id = id,
                    Name = spec.Name,
                    Group = spec.Group,
                    StartedAt = started,
                    Priority = spec.Priority,
                    Cts = cts
                };

                _running[id] = info;

                long? durMs = null;

                try
                {
                    _ctx.Log.Info($"[TASK] START {spec.Name}");
                    var t0 = Environment.TickCount64;

                    await task.ExecuteAsync(_ctx, cts.Token);

                    durMs = Environment.TickCount64 - t0;
                    _health?.MarkOk(spec.Name, DateTimeOffset.UtcNow, durMs.Value);

                    _ctx.Log.Info($"[TASK] OK    {spec.Name} ({durMs} ms)");
                }
                catch (OperationCanceledException)
                {
                    durMs ??= null;
                    _ctx.Log.Warn($"[TASK] CANCELED {spec.Name}");
                }
                catch (Exception ex)
                {
                    // keep message short
                    _health?.MarkFail(spec.Name, DateTimeOffset.UtcNow, ex.Message, durMs);
                    _ctx.Log.Error(ex, $"[TASK] FAIL {spec.Name}");
                }
                finally
                {
                    _running.TryRemove(id, out _);
                    _sem.Release();

                    // coalesce: run again once after done if pending AND enabled AND not paused
                    if (_coalescePending.TryRemove(spec.Name, out _))
                    {
                        if (!_gate.IsPaused && _isTaskEnabled(spec.Name))
                            _inbox.Writer.TryWrite(task);
                    }
                }
            }, CancellationToken.None);
        }

        _ctx.Log.Info("[SCHED] Stop");
    }
}


// ==============================
// Minimal infra (compile-ready)
// ==============================

public interface ILogger
{
    void Info(string msg);
    void Warn(string msg);
    void Error(Exception ex, string msg);
}

public sealed class ConsoleLogger : ILogger
{
    public void Info(string msg) => Console.WriteLine(msg);
    public void Warn(string msg) => Console.WriteLine(msg);
    public void Error(Exception ex, string msg) => Console.WriteLine($"{msg}\n{ex}");
}

public interface IDbService
{
    Task CheckpointAsync(CancellationToken ct);
    Task CloneSnapshotAsync(CancellationToken ct);
}

public sealed class NullDbService : IDbService
{
    public Task CheckpointAsync(CancellationToken ct) => Task.CompletedTask;
    public Task CloneSnapshotAsync(CancellationToken ct) => Task.CompletedTask;
}

public interface IClock
{
    DateTimeOffset UtcNow { get; }
}

public sealed class SystemClock : IClock
{
    public DateTimeOffset UtcNow => DateTimeOffset.UtcNow;
}

// ==============================
// Task registry (map name -> factory)
// ==============================
public sealed class TaskRegistry
{
    private readonly Dictionary<string, Func<IEngineTask>> _map = new(StringComparer.OrdinalIgnoreCase);

    public void Register(string name, Func<IEngineTask> factory) => _map[name] = factory;

    public bool TryCreate(string name, out IEngineTask task)
    {
        if (_map.TryGetValue(name, out var f))
        {
            task = f();
            return true;
        }
        task = default!;
        return false;
    }

    public IReadOnlyList<string> ListNames() => _map.Keys.OrderBy(x => x).ToList();
}

public sealed class LabImportResult
{
    public int InsertedRows { get; set; }
    public string SourceFile { get; set; } = "";
    public string SheetName { get; set; } = "";
    public string ReportDate { get; set; } = "";
}
#endregion

#region Task

// ==============================
// Demo task
// ==============================
public sealed class DemoTaskA : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "TASK A",
        Group: "DEMO",
        Policy: RunPolicy.CoalesceIfRunning,   // หรือ DropIfRunning
        Timeout: TimeSpan.FromSeconds(10)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        ctx.Log.Info($"[A] tick {ctx.Clock.UtcNow:O}");
        await Task.Delay(200, ct);
    }
}

public sealed class DemoTaskB : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "TASK B",
        Group: "DEMO",
        Policy: RunPolicy.CoalesceIfRunning,
        Timeout: TimeSpan.FromSeconds(10)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        ctx.Log.Info($"[B] tick {ctx.Clock.UtcNow:O}");
        await Task.Delay(200, ct);
    }
}

public sealed class DemoTaskC : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "TASK C",
        Group: "DEMO",
        Policy: RunPolicy.CoalesceIfRunning,
        Timeout: TimeSpan.FromSeconds(10)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        ctx.Log.Info($"[C] tick {ctx.Clock.UtcNow:O}");
        await Task.Delay(200, ct);
    }
}

// ==============================
// RWS#1
// ==============================
public sealed class Rwp1RefreshTask : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "RWS1.refresh",
        Group: "RWP1",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(25)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        var url = "http://172.16.198.28/cgi-bin/ope/allch.cgi?";
        var rows = new List<int> { 0, 1, 3, 26, 27, 0 };
        var types = new List<string> { "value", "value", "value", "value", "value", "unit" };

        var r = await HtmlHookClient.FetchChannelDataAsync(url, rows, types, ct).ConfigureAwait(false);
        if (r == null || r.Count < 6)
            throw new Exception("RWP1 response insufficient.");

        // ตาม VB: index 0..4 เป็น value, index 5 เป็น unit (ของ row 0)
        var p1Text = HtmlHookClient.GetItem2(r, 0);
        var p2Text = HtmlHookClient.GetItem2(r, 1);
        var lvlText = HtmlHookClient.GetItem2(r, 2);
        var sum1Text = HtmlHookClient.GetItem2(r, 3);
        var sum2Text = HtmlHookClient.GetItem2(r, 4);
        var unitText = HtmlHookClient.GetItem2(r, 5); // unit (optional)

        var payload = new List<(string Param, string? ValueText, string? Unit)>();

        // ถ้าอยากเก็บ unit ก็ใส่ unitText ให้กับ flow/sumflow (แล้วแต่ต้องการ)
        HtmlHookClient.AddIfValid(payload, "rwp1_flowp1", p1Text);
        HtmlHookClient.AddIfValid(payload, "rwp1_flowp2", p2Text);
        HtmlHookClient.AddIfValid(payload, "rw_level", lvlText);
        HtmlHookClient.AddIfValid(payload, "rwp1_sumflowp1", sum1Text);
        HtmlHookClient.AddIfValid(payload, "rwp1_sumflowp2", sum2Text);

        if (payload.Count == 0)
        {
            ctx.Log.Warn("[RWP1] payload empty (all invalid)");
            return;
        }

        MetricsDb.UpsertCurrentBatch("RWP1", payload);

        ctx.Log.Info($"[RWP1] saved={payload.Count} " +
                     $"p1='{p1Text}', p2='{p2Text}', lvl='{lvlText}', sum1='{sum1Text}', sum2='{sum2Text}', unit='{unitText}'");
    }
}

// ==============================
// RWS#2
// ==============================
public sealed class Rwp2RefreshTask : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "RWS2.refresh",
        Group: "RWP2",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(25)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        var url = "http://172.17.198.114/cgi-bin/ope/allch.cgi";
        var rows = new List<int> { 0, 1, 16, 17, 0 };
        var types = new List<string> { "value", "value", "value", "value", "unit" };

        var r = await HtmlHookClient.FetchChannelDataAsync(url, rows, types, ct).ConfigureAwait(false);
        if (r == null || r.Count < 5)
            throw new Exception("RWP2 response insufficient.");

        var p3Text = HtmlHookClient.GetItem2(r, 0); // row 0 value
        var p4Text = HtmlHookClient.GetItem2(r, 1); // row 1 value
        var sum3Text = HtmlHookClient.GetItem2(r, 2); // row 16 value
        var sum4Text = HtmlHookClient.GetItem2(r, 3); // row 17 value
        var unitText = HtmlHookClient.GetItem2(r, 4); // row 0 unit (optional)

        var payload = new List<(string Param, string? ValueText, string? Unit)>();

        // ถ้าต้องการเก็บ unit ด้วย ให้ใส่ unitText ไปเลย
        if (!string.IsNullOrWhiteSpace(p3Text)) payload.Add(("rwp2_flowp3", p3Text, unitText));
        if (!string.IsNullOrWhiteSpace(p4Text)) payload.Add(("rwp2_flowp4", p4Text, unitText));
        if (!string.IsNullOrWhiteSpace(sum3Text)) payload.Add(("rwp2_sumflowp3", sum3Text, unitText));
        if (!string.IsNullOrWhiteSpace(sum4Text)) payload.Add(("rwp2_sumflowp4", sum4Text, unitText));

        if (payload.Count == 0)
        {
            ctx.Log.Warn("[RWP2] payload empty (all invalid/blank)");
            return;
        }

        MetricsDb.UpsertCurrentBatch("RWP2", payload);

        ctx.Log.Info($"[RWP2] saved={payload.Count} p3='{p3Text}', p4='{p4Text}', sum3='{sum3Text}', sum4='{sum4Text}', unit='{unitText}'");
    }
}

// ==============================
// CHEM#1
// ==============================

public sealed class Chem1RefreshTask : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "chem1.refresh",
        Group: "CHEM",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(15)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        var url = "http://172.17.198.20/cgi-bin/ope/allch.cgi";
        var rows = new List<int> { 10, 11 };
        var types = new List<string> { "value", "value" };

        var r = await HtmlHookClient
            .FetchChannelDataAsync(url, rows, types, ct)
            .ConfigureAwait(false);

        if (r == null || r.Count < 2)
            throw new Exception("CHEM1 response insufficient.");

        var w1Val = HtmlHookClient.GetItem2(r, 0);
        var w2Val = HtmlHookClient.GetItem2(r, 1);

        var payload = new List<(string Param, string? ValueText, string? Unit)>();

        HtmlHookClient.AddIfValid(payload, "cl_lineA", w1Val);
        HtmlHookClient.AddIfValid(payload, "cl_lineB", w2Val);

        if (payload.Count > 0)
        {
            MetricsDb.UpsertCurrentBatch("CHEM1", payload, DateTimeOffset.UtcNow);
            ctx.Log.Info($"[CHEM1] saved={payload.Count}");
        }
        else
        {
            ctx.Log.Warn("[CHEM1] payload empty");
        }
    }
}

// ==============================
// CHEM#2
// ==============================
public sealed class Chem2RefreshTask : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "chem2.refresh",
        Group: "CHEM",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(15)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        var url = "http://172.17.198.21/cgi-bin/ope/allch.cgi";
        var rows = new List<int> { 10, 11 };
        var types = new List<string> { "value", "value" };

        var r = await HtmlHookClient
            .FetchChannelDataAsync(url, rows, types, ct)
            .ConfigureAwait(false);

        if (r == null || r.Count < 2)
            throw new Exception("CHEM2 response insufficient.");

        var w3Val = HtmlHookClient.GetItem2(r, 0);
        var w4Val = HtmlHookClient.GetItem2(r, 1);

        var payload = new List<(string Param, string? ValueText, string? Unit)>();

        HtmlHookClient.AddIfValid(payload, "cl_lineC", w3Val);
        HtmlHookClient.AddIfValid(payload, "cl_lineD", w4Val);

        if (payload.Count > 0)
        {
            MetricsDb.UpsertCurrentBatch("CHEM2", payload, DateTimeOffset.UtcNow);
            ctx.Log.Info($"[CHEM2] saved={payload.Count}");
        }
        else
        {
            ctx.Log.Warn("[CHEM2] payload empty");
        }
    }
}

// ==============================
// TPS
// ==============================
public sealed class TpsRefreshTask : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "TPS.refresh",
        Group: "TPS",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(25)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        var url = "http://172.16.198.25/cgi-bin/ope/allch.cgi?";
        var rows = new List<int> { 16, 1, 10, 14 };
        var types = new List<string> { "value", "value", "value", "value" };

        var tps = await HtmlHookClient.FetchChannelDataAsync(url, rows, types, ct).ConfigureAwait(false);
        if (tps == null || tps.Count < 4)
            throw new Exception("TPS response insufficient.");

        var vVal = HtmlHookClient.GetItem2(tps, 0);
        var pVal = HtmlHookClient.GetItem2(tps, 1);
        var lvlVal = HtmlHookClient.GetItem2(tps, 2);
        var sumFlowVal = HtmlHookClient.GetItem2(tps, 3);

        var payload = new List<(string Param, string? ValueText, string? Unit)>();

        HtmlHookClient.AddIfValid(payload, "TR_flow", vVal);
        HtmlHookClient.AddIfValid(payload, "TR_pressure", pVal);
        HtmlHookClient.AddIfValid(payload, "TR_cwt", lvlVal);
        HtmlHookClient.AddIfValid(payload, "TR_sumflow", sumFlowVal);

        if (payload.Count == 0)
        {
            ctx.Log.Warn("[TPS] payload empty (all invalid)");
            return;
        }

        // current-only upsert (แทนที่ค่าเดิมเสมอ)
        MetricsDb.UpsertCurrentBatch("TPS", payload, tsUtc: DateTimeOffset.UtcNow);

        ctx.Log.Info($"[TPS] saved={payload.Count} " +
                     $"flow='{vVal}', press='{pVal}', lvl='{lvlVal}', sum='{sumFlowVal}'");
    }
}

// ==============================
// DPS
// ==============================
public sealed class DpsRefreshTask : IEngineTask
    {
        public TaskSpec Spec { get; } = new TaskSpec(
            Name: "DPS.refresh",
            Group: "DPS",
            Priority: TaskPriority.Normal,
            Policy: RunPolicy.DropIfRunning,
            Timeout: TimeSpan.FromSeconds(25)
        );

        public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
        {
            // ========== 1) DPS A/B (172.16.198.26) ==========
            var urlAB = "http://172.16.198.26/cgi-bin/ope/allch.cgi?";
            var rowsAB = new List<int> { 3, 4, 3, 11, 6, 18, 19 };
            var typesAB = new List<string> { "value", "value", "unit", "value", "value", "value", "value" };

            var ab = await HtmlHookClient.FetchChannelDataAsync(urlAB, rowsAB, typesAB, ct).ConfigureAwait(false);
            if (ab == null || ab.Count < 7)
                throw new Exception("DPS A/B response insufficient.");

            // index 3..6 ตาม VB เดิม (row 11,6,18,19)
            var airP1Text = HtmlHookClient.GetItem2(ab, 3); // AirP1
            var airP2Text = HtmlHookClient.GetItem2(ab, 4); // AirP2
            var svFlowText = HtmlHookClient.GetItem2(ab, 5); // SVwater_flow
            var svSumText = HtmlHookClient.GetItem2(ab, 6); // SVwater_sumflow

            // ========== 2) DPS SUM + FlowA/FlowB (172.16.198.24) ==========
            var urlSum = "http://172.16.198.24/cgi-bin/ope/allch.cgi?";

            // แก้ให้ types ตรงกับ rows 4 ตัว: sumFlow, v1(flowA), unit, v2(flowB)
            var rowsSum = new List<int> { 37, 11, 11, 12 };
            var typesSum = new List<string> { "value", "value", "unit", "value" };

            var sum = await HtmlHookClient.FetchChannelDataAsync(urlSum, rowsSum, typesSum, ct).ConfigureAwait(false);
            if (sum == null || sum.Count < 4)
                throw new Exception("DPS SUM response insufficient.");

            var sumFlowText = HtmlHookClient.GetItem2(sum, 0); // row 37 value
            var flowAText = HtmlHookClient.GetItem2(sum, 1); // row 11 value
            var unitText = HtmlHookClient.GetItem2(sum, 2); // row 11 unit (optional)
            var flowBText = HtmlHookClient.GetItem2(sum, 3); // row 12 value

            // ========== 3) Build payload (string -> metrics_current) ==========
            var payload = new List<(string Param, string? ValueText, string? Unit)>();

            // Flow A/B: ถ้าจะเก็บ unit ด้วย ก็ใส่ unitText (แต่ปกติ unit เดียวกันทั้งคู่)
            if (!string.IsNullOrWhiteSpace(flowAText))
                payload.Add(("DPS_flowA", flowAText, unitText));
            if (!string.IsNullOrWhiteSpace(flowBText))
                payload.Add(("DPS_flowB", flowBText, unitText));

            // Sumflow (unit ไม่จำเป็น)
            if (!string.IsNullOrWhiteSpace(sumFlowText))
                payload.Add(("DPS_sumflow", sumFlowText, null));

            // Air + service water
            if (!string.IsNullOrWhiteSpace(airP1Text))
                payload.Add(("AirP1", airP1Text, null));
            if (!string.IsNullOrWhiteSpace(airP2Text))
                payload.Add(("AirP2", airP2Text, null));
            if (!string.IsNullOrWhiteSpace(svFlowText))
                payload.Add(("SVwater_flow", svFlowText, null));
            if (!string.IsNullOrWhiteSpace(svSumText))
                payload.Add(("SVwater_sumflow", svSumText, null));

            if (payload.Count == 0)
            {
                ctx.Log.Warn("[DPS] payload empty");
                return;
            }

            // ✅ current-only upsert (แทนที่ค่าเดิมเสมอ)
            // เวลา TH: ให้ไปแก้/คุมใน MetricsDb.UpsertCurrent* ตามที่คุยกัน (ToOffset(+7))
            MetricsDb.UpsertCurrentBatch("DPS", payload);

            ctx.Log.Info($"[DPS] saved={payload.Count} " +
                         $"A='{flowAText}', B='{flowBText}', SUM='{sumFlowText}', " +
                         $"AirP1='{airP1Text}', AirP2='{airP2Text}', SV='{svFlowText}', SVsum='{svSumText}'");
        }
    }

// ==============================
// Branch
// ==============================
public sealed class BranchDataRefreshTask : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "Branch.refresh",
        Group: "Branch",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromMinutes(2)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        try
        {
            var branches = new[] { "PK", "TP", "RB" };
            var savedCount = await BranchFetchService.FetchBranchesAndSaveAsync(
                branches: branches,
                maxConcurrency: 3
            ).ConfigureAwait(false);

            ctx.Log.Info($"[BRANCH] OK ({savedCount} saved)");
        }
        catch (OperationCanceledException)
        {
            ctx.Log.Warn("[BRANCH] canceled");
            throw;
        }
        catch (Exception ex)
        {
            ctx.Log.Error(ex, "[BRANCH] Error");
        }
    }
}

// ==============================
// RCV38 Refresh Task
// ==============================

public sealed class Rcv38RefreshTask : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "rcv38.refresh",
        Group: "RCV",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(30)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        var url = "http://172.16.193.162/smartmap/newgraph/rcv_qry.php?rtu=rcv38&rtu_f=p";
        var dbPath = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "data.db");

        // ✅ สร้างตารางถ้ายังไม่มี (กัน “ยังไม่มี OneValuemetrics”)
        SingleValueDb.Initialize(dbPath);

        var affected = await SingleValueDb.RefreshRcvSeriesAsync(
            url: url,
            dbPath: dbPath,
            source: "rcv38",
            key: "p",
            unit: null,
            ct: ct
        ).ConfigureAwait(false);

        ctx.Log.Info($"[RCV38] OK affected={affected}");
    }
}
// ==============================
// Pressure Trend Curve (PTC) query task
// ==============================
public sealed class PtcQueryOnceTask : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "ptc.query.once",
        Group: "PTC",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromMinutes(5)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        ctx.Log.Info("[PTC] start");

        try
        {
            // 1) fetch gensvg rel list (in-memory)
            var commonUrl = "http://172.16.193.162/ptc/realtime/svgtest/common.php?NameSta=MH";
            var srcList = await Uroboros.PTC.FetchEmbedSrcListAsync(commonUrl, ct).ConfigureAwait(false);

            if (srcList.Count == 0)
            {
                ctx.Log.Warn("[PTC] No gensvg links found (srcList=0).");
                ctx.Log.Info("[PTC] done");
                return;
            }

            ctx.Log.Info($"[PTC] found={srcList.Count}");

            // 2) ensure DB + table
            var dbPath = Path.Combine(AppContext.BaseDirectory, "data.db");
            Uroboros.PTC.EnsureDbUpperLower(dbPath);

            // 3) loop: extract + save
            int ok = 0, empty = 0, fail = 0;

            foreach (var rel in srcList)
            {
                ct.ThrowIfCancellationRequested();

                var code = Uroboros.PTC.TryGetTypename(rel);
                if (string.IsNullOrWhiteSpace(code))
                {
                    empty++;
                    continue;
                }

                try
                {
                    var map = await Uroboros.PTC.ExtractUpperLowerFromGensvgRelAsync(rel, ct).ConfigureAwait(false);
                    if (map.Count == 0)
                    {
                        ctx.Log.Warn($"[PTC] {code} : no data");
                        empty++;
                        continue;
                    }

                    Uroboros.PTC.SaveUpperLowerToDb(dbPath, code!, map);
                    ctx.Log.Info($"[PTC] {code} : saved rows={map.Count}");
                    ok++;
                }
                catch (OperationCanceledException) { throw; }
                catch (Exception ex)
                {
                    fail++;
                    ctx.Log.Error(ex, $"[PTC] {code} : FAIL {ex.Message}");
                }
            }

            ctx.Log.Info($"[PTC] Summary ok={ok}, empty={empty}, fail={fail}");
        }
        catch (OperationCanceledException)
        {
            ctx.Log.Warn("[PTC] canceled");
            throw;
        }
        catch (Exception ex)
        {
            ctx.Log.Error(ex, $"[PTC] Error during execution: {ex.Message}");
        }
        finally
        {
            // ช่วยลด footprint (optional)
            // หมายเหตุ: HttpClient ไม่ควร Dispose ทุกครั้ง (เราใช้ static ใน PTC.cs แล้ว)
        }

        ctx.Log.Info("[PTC] done");
    }
}

// ==============================
// OnlineLab V2 task
// ==============================
public sealed class OnlineLabV2Task : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "onlinelab.query",
        Group: "OnlineLab",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(60)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        var dbPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "data.db");

        // ✅ ensure DB/tables (WAL + create tables)
        SingleValueDb.Initialize(dbPath);

        // ✅ pure HTTP ingest (no Chrome, no lock)
        await OnlineLabQuery.RefreshSourcesFromHtmlAsync2(
                htmlPath: "dummy",
                ct: ct)
            .ConfigureAwait(false);

        ctx.Log.Info("[OnlineLabV2] OK");
    }
}

// ==============================
// AQUADAT query task
// ==============================
public sealed class AquadatQueryTask : IEngineTask
{
    // ทำเป็น get; set; => ครอบคลุมทั้ง interface แบบ get-only และ get-set
    public TaskSpec Spec { get; set; } = new TaskSpec(
        Name: "Aquadat.refresh",
        Group: "AQUADAT",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(120)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        ctx.Log.Info("[AQUADAT] start");

        var dbPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "data.db");

        // ensure DB/tables (WAL + create tables)
        AQtable.Initialize(dbPath);

        // 1) clear narrow
        await AQtable.ClearAllNarrowV2Async(dbPath).ConfigureAwait(false);

        // 2) bypass mapping (ตาม VB เดิม)
        var bypassList = new List<Dictionary<string, string>>
        {
            new() { ["configID"]="761",  ["plant"]="MS", ["station"]="TPS" },
            new() { ["configID"]="777",  ["plant"]="MS", ["station"]="TPS" },
            new() { ["configID"]="793",  ["plant"]="MS", ["station"]="TPS" },
            new() { ["configID"]="7984", ["plant"]="MS", ["station"]="TPS" },

            new() { ["configID"]="816", ["plant"]="MS", ["station"]="DPS" },
            new() { ["configID"]="829", ["plant"]="MS", ["station"]="DPS" },
            new() { ["configID"]="842", ["plant"]="MS", ["station"]="DPS" },
            new() { ["configID"]="857", ["plant"]="MS", ["station"]="DPS" },

            new() { ["configID"]="10", ["plant"]="MS", ["station"]="RWS" },
            new() { ["configID"]="15", ["plant"]="MS", ["station"]="RWS" },
            new() { ["configID"]="20", ["plant"]="MS", ["station"]="RWS" },
            new() { ["configID"]="25", ["plant"]="MS", ["station"]="RWS" },
            new() { ["configID"]="29", ["plant"]="MS", ["station"]="RWS" },
            new() { ["configID"]="46", ["plant"]="MS", ["station"]="RWS" },
            new() { ["configID"]="52", ["plant"]="MS", ["station"]="RWS" },
            new() { ["configID"]="58", ["plant"]="MS", ["station"]="RWS" },
            new() { ["configID"]="63", ["plant"]="MS", ["station"]="RWS" },
            new() { ["configID"]="69", ["plant"]="MS", ["station"]="RWS" },

            // FWS group (ตามเดิม)
            new() { ["configID"]="476", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="486", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="496", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="506", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="516", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="526", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="536", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="546", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="556", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="566", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="576", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="586", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="596", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="606", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="618", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="628", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="638", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="648", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="658", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="668", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="678", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="688", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="698", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="708", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="718", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="728", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="738", ["plant"]="MS", ["station"]="FWS" },
            new() { ["configID"]="748", ["plant"]="MS", ["station"]="FWS" },

            // CDS group
            new() { ["configID"]="81", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="83", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="85", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="95", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="98", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="93", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="96", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="89", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="92", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="8395", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="8397", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="8399", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="8406", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="8408", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="8405", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="8407", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="8402", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="8404", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="109", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="111", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="113", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="115", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="117", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="118", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="119", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="120", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="121", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="4325", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="4326", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="129", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="131", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="133", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="135", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="137", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="139", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="141", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="143", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="145", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="147", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="151", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="154", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="155", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="158", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="157", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="160", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="8409", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="8411", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="8413", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="8416", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="8418", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="8420", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="8422", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="8419", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="8421", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="171", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="173", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="175", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="177", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="179", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="181", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="182", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="183", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="184", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="185", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="4327", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="4328", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="192", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="194", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="196", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="198", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="127", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="128", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="190", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="191", ["plant"]="MS", ["station"]="CDS" },

            new() { ["configID"]="190", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="191", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="6972", ["plant"]="MS", ["station"]="CDS" },
            new() { ["configID"]="6973", ["plant"]="MS", ["station"]="CDS" },

            // more RWS
            new() { ["configID"]="1",  ["plant"]="MS", ["station"]="RWS" },
            new() { ["configID"]="2",  ["plant"]="MS", ["station"]="RWS" },
            new() { ["configID"]="39",  ["plant"]="MS", ["station"]="RWS" },
            new() { ["configID"]="40",  ["plant"]="MS", ["station"]="RWS" },
            // QWS (lab)
            new() { ["configID"]="887", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="888", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="889", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="890", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="891", ["plant"]="MS", ["station"]="QWS" },

            // P1..P4 + chlorine
            new() { ["configID"]="996",  ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="999",  ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1002", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1044", ["plant"]="MS", ["station"]="QWS" },

            new() { ["configID"]="1008", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1011", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1014", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1017", ["plant"]="MS", ["station"]="QWS" },

            new() { ["configID"]="1020", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1023", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1026", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1029", ["plant"]="MS", ["station"]="QWS" },

            new() { ["configID"]="1032", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1035", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1038", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1041", ["plant"]="MS", ["station"]="QWS" },

            new() { ["configID"]="1004", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1016", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1028", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1040", ["plant"]="MS", ["station"]="QWS" },

            new() { ["configID"]="1046", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1055", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1064", ["plant"]="MS", ["station"]="QWS" },
            new() { ["configID"]="1067", ["plant"]="MS", ["station"]="QWS" },
        };

        var bypassJsonText = JsonSerializer.Serialize(bypassList, new JsonSerializerOptions
        {
            WriteIndented = true
        });

        var todayDt = DateTime.Today;
        var today = todayDt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var yesterday = todayDt.AddDays(-1).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        ctx.Log.Info($"Start: {yesterday} END: {today}");

        var aqFast = new AquadatFastQuery();

        var req = new MultiOutputRequest
        {
            BeginDtString = yesterday,
            EndDtString = today,
            RemoveOddHour = false,
            Token = "",
            ForcePlantText = "",
            Mode = RunMode.WriteDbOnly,
            IncludeDateTimeColumn = true,
        };

        req.ExternalJsonByKey["MAIN"] = bypassJsonText;
        req.DbWriteByKey["MAIN"] = true;
        req.DbPathByKey["MAIN"] = dbPath;
        req.DbPrefixByKey["MAIN"] = ""; // narrow_v2

        await aqFast.ProcessMultiAsync(req).ConfigureAwait(false);


        // 3) refresh CDS remarks (station 15, 62) -> clear old remark tables and rewrite
        AquadatRemarkHelper.Initialize(dbPath);

        var remark = new AquadatRemarkHelper(aqFast, dbPath);
        var remarkDateYmd8 = todayDt.ToString("yyyyMMdd", CultureInfo.InvariantCulture);

        var remarkRows = await remark.RefreshCdsRemarksAsync(remarkDateYmd8, ct).ConfigureAwait(false);
        ctx.Log.Info($"[AQUADAT] remark rows written = {remarkRows}");

        // 4) vacuum (optional)
        AQtable.VacuumDb(dbPath);

        ctx.Log.Info("[AQUADAT] OK");
    }
}

// ==============================
// AQUADAT FWS query task
// ==============================
public sealed class AquadatFwsQueryTask : IEngineTask
{
    public TaskSpec Spec { get; set; } = new TaskSpec(
        Name: "AquadatFWS.refresh",
        Group: "AQUADAT",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(180)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        ctx.Log.Info("[AQUADAT FWS] start");

        var dbPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "data.db");
        AQtable.Initialize(dbPath, rebuildViews: false);

        // 1) clear FWS_v2
        await AQtable.ClearAllFwsV2Async(dbPath).ConfigureAwait(false);

        // 2) configIDs ตาม VB เดิม
        int[] fwsConfigIds =
        {
            317, 318, 319, 320, 322, 323,
            327, 328, 329, 330, 332, 333,
            337, 338, 339, 340, 342, 343,
            347, 348, 349, 350, 352, 353,
            357, 358, 359, 360, 362, 363,
            367, 368, 369, 370, 372, 373,
            377, 378, 379, 380, 382, 383,
            387, 388, 389, 390, 392, 393,
            399, 400, 401, 402, 404, 405,
            409, 410, 411, 412, 414, 415,
            419, 420, 421, 422, 424, 425,
            429, 430, 431, 432, 434, 435,
            439, 440, 441, 442, 444, 445,
            449, 450, 451, 452, 454, 455,
            459, 460, 461, 462, 464, 465,
            469, 470, 471, 472, 474, 475,
            479, 480, 482, 484, 485,
            489, 490, 492, 494, 495,
            499, 500, 502, 504, 505,
            509, 510, 512, 514, 515,
            519, 520, 522, 524, 525,
            529, 530, 532, 534, 535,
            539, 540, 542, 544, 545,
            549, 550, 552, 554, 555,
            559, 560, 562, 564, 565,
            569, 570, 572, 574, 575,
            579, 580, 582, 584, 585,
            589, 590, 592, 594, 595,
            599, 600, 602, 604, 605,
            609, 610, 612, 614, 615,
            621, 622, 624, 626, 627,
            631, 632, 634, 636, 637,
            641, 642, 644, 646, 647,
            651, 652, 654, 656, 657,
            661, 662, 664, 666, 667,
            671, 672, 674, 676, 677,
            681, 682, 684, 686, 687,
            691, 692, 694, 696, 697,
            701, 702, 704, 706, 707,
            711, 712, 714, 716, 717,
            721, 722, 724, 726, 727,
            731, 732, 734, 736, 737,
            741, 742, 744, 746, 747,
            751, 752, 754, 756, 757,
            7944, 7945, 7946, 7947, 7948, 7949, 7950, 7951,
            7952, 7953, 7954, 7955, 7956, 7957, 7958, 7959, 7960, 7961,
            7962, 7963, 7964, 7965, 7966, 7967, 7968, 7969, 7970, 7971
        };

        int[] qwsConfigIds = { 1002, 1014, 1026, 1038 };

        // 3) build bypassList
        var bypassList = new List<Dictionary<string, string>>(fwsConfigIds.Length + qwsConfigIds.Length);

        foreach (var cfg in fwsConfigIds)
        {
            bypassList.Add(new Dictionary<string, string>
            {
                ["configID"] = cfg.ToString(CultureInfo.InvariantCulture),
                ["plant"] = "MS",
                ["station"] = "FWS"
            });
        }

        foreach (var cfg in qwsConfigIds)
        {
            bypassList.Add(new Dictionary<string, string>
            {
                ["configID"] = cfg.ToString(CultureInfo.InvariantCulture),
                ["plant"] = "MS",
                ["station"] = "QWS"
            });
        }

        var bypassJsonText = JsonSerializer.Serialize(bypassList, new JsonSerializerOptions { WriteIndented = true });

        var beginStr = DateTime.Today.AddDays(-30).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var endStr = DateTime.Today.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        var aqFast = new AquadatFastQuery();

        // 4) no Chrome: write DB only (FWS_v2)
        var req = new MultiOutputRequest
        {
            BeginDtString = beginStr,
            EndDtString = endStr,
            RemoveOddHour = false,
            Token = "",
            ForcePlantText = "",
            Mode = RunMode.WriteDbOnly,
            IncludeDateTimeColumn = true,
        };

        req.ExternalJsonByKey["MAIN"] = bypassJsonText;

        req.DbWriteByKey["MAIN"] = true;
        req.DbPathByKey["MAIN"] = dbPath;
        req.DbPrefixByKey["MAIN"] = "FWS"; // << เขียน AQ_readings_FWS_v2

        await aqFast.ProcessMultiAsync(req).ConfigureAwait(false);

        AQtable.VacuumDb(dbPath);

        ctx.Log.Info("[AQUADAT FWS] OK");
    }
}

// ==============================
// DB Upload Task (to Google Drive)
// ==============================
public sealed class DbUploadTask : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "DB_upload.refresh",
        Group: "DB",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(120)
    );

    private const string DriveFolderId = "1hLIPn9qgjqm4WliNJGsEwmjq78oaXFgm";

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        var baseDir = AppContext.BaseDirectory;
        var srcDbPath = Path.Combine(baseDir, "data.db");
        var snapshotPath = Path.Combine(baseDir, "data_ghost.db");

        // 1) validate
        if (!DbUploadModule.ValidateSourceDb(srcDbPath, out var vErr))
        {
            ctx.Log.Warn($"[DB] {vErr}");
            return;
        }

        // 2) init Drive service (ทำก่อน เพื่อถ้าพังจะจบไว)
        DriveService service;
        try
        {
            service = DbUploadModule.GetDriveServiceOrThrow();
        }
        catch (Exception ex)
        {
            ctx.Log.Error(ex, $"[DB] drive init error: {ex.Message}");
            return;
        }

        // 3) snapshot + upload (retry 1 รอบทันที)
        var ok = await SnapshotThenUpload_WithOneImmediateRetryAsync(
            ctx, ct, service, srcDbPath, snapshotPath, DriveFolderId).ConfigureAwait(false);

        if (!ok)
            ctx.Log.Warn("[DB] upload failed (after retry).");
    }

    private static async Task<bool> SnapshotThenUpload_WithOneImmediateRetryAsync(
        EngineContext ctx,
        CancellationToken ct,
        DriveService service,
        string srcDbPath,
        string snapshotPath,
        string driveFolderId)
    {
        // attempt 1
        if (await TrySnapshotThenUploadOnceAsync(ctx, ct, service, srcDbPath, snapshotPath, driveFolderId).ConfigureAwait(false))
            return true;

        // immediate re-action: attempt 2 (สั้นๆ)
        ctx.Log.Warn("[DB] retry once immediately...");

        // เคลียร์ pool เผื่อมี conn ค้าง (Microsoft.Data.Sqlite)
        try { Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools(); } catch { }

        // backoff สั้นๆ ให้ writer ปล่อย lock
        await Task.Delay(250, ct).ConfigureAwait(false);

        return await TrySnapshotThenUploadOnceAsync(ctx, ct, service, srcDbPath, snapshotPath, driveFolderId).ConfigureAwait(false);
    }

    private static async Task<bool> TrySnapshotThenUploadOnceAsync(
        EngineContext ctx,
        CancellationToken ct,
        DriveService service,
        string srcDbPath,
        string snapshotPath,
        string driveFolderId)
    {
        // A) snapshot
        try
        {
            await DbUploadModule.CreateSnapshotAsync(srcDbPath, snapshotPath, ctx, ct).ConfigureAwait(false);
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            // ถ้าเป็น lock/busy ให้ log แบบ “ชนตอนเขียน” ชัดๆ
            if (IsLikelySqliteBusyOrFileLocked(ex))
                ctx.Log.Warn($"[DB] snapshot busy/locked: {ex.Message}");
            else
                ctx.Log.Error(ex, $"[DB] snapshot error | file={snapshotPath}");

            return false;
        }

        // B) upload
        try
        {
            await DbUploadModule
                .UploadSnapshotAsync(service, snapshotPath, driveFolderId, ctx, ct)
                .ConfigureAwait(false);

            var ts = File.GetLastWriteTime(snapshotPath);
            ctx.Log.Info($"[DB] upload complete | file={Path.GetFileName(snapshotPath)} | localTs={ts:yyyy-MM-dd HH:mm:ss}");
            return true;
        }
        catch (OperationCanceledException) { throw; }
        catch (Exception ex)
        {
            // upload fail: รีแอคชันทันทีจะไปเข้ารอบ retry ของ outer
            ctx.Log.Warn($"[DB] upload error: {ex.Message}");
            return false;
        }
    }

    private static bool IsLikelySqliteBusyOrFileLocked(Exception ex)
    {
        // 1) IOException sharing violation
        if (ex is IOException ioex)
        {
            // HResult 0x20 (sharing violation), 0x21 (lock violation) บ่อยใน Windows
            var hr = ioex.HResult;
            if (hr == unchecked((int)0x80070020) || hr == unchecked((int)0x80070021))
                return true;

            // message heuristic
            var m = ioex.Message ?? "";
            if (m.Contains("being used by another process", StringComparison.OrdinalIgnoreCase) ||
                m.Contains("sharing violation", StringComparison.OrdinalIgnoreCase) ||
                m.Contains("lock violation", StringComparison.OrdinalIgnoreCase))
                return true;
        }

        // 2) SqliteException busy/locked (Microsoft.Data.Sqlite)
        if (ex is Microsoft.Data.Sqlite.SqliteException sx)
        {
            // SQLite primary codes: 5=BUSY, 6=LOCKED
            if (sx.SqliteErrorCode == 5 || sx.SqliteErrorCode == 6)
                return true;
        }

        // 3) inner
        if (ex.InnerException != null)
            return IsLikelySqliteBusyOrFileLocked(ex.InnerException);

        return false;
    }
}


// ==============================
// DB Download Task (from Google Drive)
// ==============================
public sealed class DbDownloadTask : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "DB_download.refresh",
        Group: "DB",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(120)
    );

    private const string DriveFolderId = "1hLIPn9qgjqm4WliNJGsEwmjq78oaXFgm";
    private const string RemoteFileName = "data_ghost.db";

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        var baseDir = AppContext.BaseDirectory;
        var targetDb = Path.Combine(baseDir, "data.db");
        var backupDb = Path.Combine(baseDir, "data.db.bak");
        var tempDownload = Path.Combine(baseDir, $"{RemoteFileName}.download.tmp");

        // 1) init Drive service
        DriveService service;
        try
        {
            service = DbUploadModule.GetDriveServiceOrThrow();
        }
        catch (Exception ex)
        {
            ctx.Log.Error(ex, "[DB] drive init error");
            return;
        }

        // 2) download remote file -> temp
        try
        {
            if (File.Exists(tempDownload))
                File.Delete(tempDownload);

            await DriveSyncCli.DownloadFileFromGoogleDriveAsync(
                service,
                DriveFolderId,
                RemoteFileName,
                tempDownload,
                ct
            ).ConfigureAwait(false);

            ctx.Log.Info($"[DB] download temp complete | file={RemoteFileName}");
        }
        catch (Exception ex)
        {
            ctx.Log.Error(ex, $"[DB] download error: {ex.Message}");
            return;
        }

        // 3) clear SQLite pools (Microsoft.Data.Sqlite)
        try
        {
            Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        }
        catch { }

        // 4) backup + atomic replace
        try
        {
            if (File.Exists(targetDb))
            {
                try
                {
                    File.Copy(targetDb, backupDb, true);
                }
                catch { }

                File.Replace(tempDownload, targetDb, backupDb, ignoreMetadataErrors: true);
            }
            else
            {
                File.Move(tempDownload, targetDb);
            }

            var ts = File.GetLastWriteTime(targetDb);
            ctx.Log.Info($"[DB] download complete | file=data.db | localTs={ts:yyyy-MM-dd HH:mm:ss}");
        }
        catch (Exception ex)
        {
            ctx.Log.Error(ex, $"[DB] swap error: {ex.Message}");
        }
        finally
        {
            try
            {
                if (File.Exists(tempDownload))
                    File.Delete(tempDownload);
            }
            catch { }
        }
    }
}

// ==============================
// MDB Upload Task (to Google Drive) — NO chromelock (FULL REWRITE)
// ==============================
public sealed class MdbUploadTask : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "MDB_upload.refresh",
        Group: "MDB",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(120)
    );

    private const string DriveFolderId = "1hLIPn9qgjqm4WliNJGsEwmjq78oaXFgm";

    private const string ConfigFolderName = "config_";
    private const string ConfigFileName = "config.ini";

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        var startupPath = AppContext.BaseDirectory;

        var configRel = Path.Combine(ConfigFolderName, ConfigFileName);
        var configPath = Path.Combine(startupPath, configRel);

        var mediaDir = Path.Combine(startupPath, MdbReaderSafe.MediaFolderName);
        var mdbPath = Path.Combine(mediaDir, MdbReaderSafe.MediaMdbName);
        var chemDbPath = MdbReaderSafe.ResolveActiveChemDbPath(startupPath);

        // =========================
        // STEP 1) Sync src -> media\data.mdb
        // =========================
        string? syncMsg;
        MdbReaderSafe.MdbSyncStatus st;

        try
        {
            st = MdbReaderSafe.SyncMdbToMediaAtStartup(startupPath, configRel, out syncMsg);
        }
        catch (Exception ex)
        {
            ctx.Log.Error(ex, "[MDB] Sync exception");
            return;
        }

        if (st == MdbReaderSafe.MdbSyncStatus.UpToDate)
        {
            ctx.Log.Info($"✅ Up-to-date: skip upload ({syncMsg})");
            return;
        }

        if (st != MdbReaderSafe.MdbSyncStatus.Synced)
        {
            ctx.Log.Warn($"❌ Sync error: {syncMsg ?? "(no message)"} | cfg={configPath}");
            return;
        }

        // =========================
        // STEP 1.5) Validate media\data.mdb exists
        // =========================
        if (!Directory.Exists(mediaDir))
        {
            ctx.Log.Warn($"❌ media folder missing ({mediaDir})");
            return;
        }
        if (!File.Exists(mdbPath))
        {
            ctx.Log.Warn($"❌ file missing after sync ({mdbPath})");
            return;
        }

        // =========================
        // STEP 2) Convert media\data.mdb -> media\chem.db  (atomic replace + retry)
        // =========================
        try
        {
            ctx.Log.Info($"[MDB] Convert start: {mdbPath} -> {chemDbPath}");
            MdbReaderSafe.ConvertMediaMdbToChemDb(startupPath, ctx, ct);
            ctx.Log.Info($"[MDB] Convert done: {chemDbPath}");
        }
        catch (Exception ex)
        {
            ctx.Log.Error(ex, $"❌ Convert MDB->chem.db failed: {ex.Message}");
            return;
        }

        chemDbPath = MdbReaderSafe.ResolveActiveChemDbPath(startupPath);

        if (!File.Exists(chemDbPath))
        {
            ctx.Log.Warn($"❌ Convert done but active chem db not found ({chemDbPath})");
            return;
        }

        // =========================
        // STEP 3) init Drive service
        // =========================
        DriveService service;
        try
        {
            service = GoogleDriveHelper.GetDriveServiceOrThrow();
        }
        catch (Exception ex)
        {
            ctx.Log.Error(ex, "[MDB] drive init error");
            return;
        }

        // =========================
        // STEP 4) upload chem.db
        // =========================
        try
        {
            var fiChem = new FileInfo(chemDbPath);
            var sizeMbChem = fiChem.Length / 1024.0 / 1024.0;

            await DriveSyncCli
                .UploadFileToSpecificFolderAsync(service, chemDbPath, DriveFolderId, ct)
                .ConfigureAwait(false);

            ctx.Log.Info($"⬆️ Upload done: {fiChem.Name} ({sizeMbChem:F2} MB) @ {DateTime.Now:yyyy-MM-dd HH:mm:ss}");
        }
        catch (Google.GoogleApiException ex)
        {
            ctx.Log.Error(ex, $"🚨 Google API Error: {ex.Message}");
        }
        catch (Exception ex)
        {
            ctx.Log.Error(ex, $"❌ Upload chem.db error: {ex.Message}");
        }
    }
}

// ==============================
// MDB download Task (from Google Drive) — NO chromelock (FULL REWRITE)
// ==============================
public sealed class MdbDownloadTask : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "MDB_download.refresh",
        Group: "MDB",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(180)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        var r = await ChemDownloadModule.RunAsync(ctx, ct).ConfigureAwait(false);

        if (r.Ok)
        {
            if (r.Skipped) ctx.Log.Info(r.Message);
            else ctx.Log.Info(r.Message);
        }
        else
        {
            ctx.Log.Warn(r.Message);
        }
    }
}

public sealed class LabImportTask : IEngineTask
{
    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "LAB.import.daily",
        Group: "LAB",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromMinutes(5)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        var baseDir = AppContext.BaseDirectory;

        var reportDate = DateTime.Today.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        ctx.Log.Info($"[LAB] start import reportDate={reportDate}");

        var result = await Task.Run(
            () => LabImportCli.DailyLabImporter.ImportDaily(reportDate, baseDir),
            ct
        ).ConfigureAwait(false);

        ctx.Log.Info($"[LAB] OK inserted={result.RowsWritten}, file='{result.ExcelPath}'");
    }
}
#endregion

#region WEB Listener

// ==============================
// Web listener (HttpListener) : CLI :8888
// ==============================

public sealed class WebListener
{
    private readonly EngineContext _ctx;
    private readonly Scheduler _sched;
    private readonly StageGate _gate;
    private readonly TaskRegistry _reg;
    private readonly HttpListener _http = new();
    private readonly JsonSerializerOptions _jsonOpt = new(JsonSerializerDefaults.Web);
    private readonly AqApiModule.IAqFastApi _aq = new AqFastApiReal();
    private readonly TaskConfigService _cfg;
    private readonly TaskHealthTracker _health;
    private readonly NextRunTracker _nextRun;
    private readonly IPtcSeriesProvider _ptc;
    public WebListener(
        EngineContext ctx, Scheduler sched, StageGate gate, TaskRegistry reg,
        AqApiModule.IAqFastApi aq, TaskConfigService cfg, TaskHealthTracker health,
        NextRunTracker nextRun, IPtcSeriesProvider ptc, string prefix)
    {
        _ctx = ctx ?? throw new ArgumentNullException(nameof(ctx));
        _sched = sched ?? throw new ArgumentNullException(nameof(sched));
        _gate = gate ?? throw new ArgumentNullException(nameof(gate));
        _reg = reg ?? throw new ArgumentNullException(nameof(reg));
        _aq = aq ?? throw new ArgumentNullException(nameof(aq));
        _ptc = ptc ?? throw new ArgumentNullException(nameof(ptc));
        _cfg = cfg;
        _health = health;
        _nextRun = nextRun;

        if (string.IsNullOrWhiteSpace(prefix))
            throw new ArgumentException("prefix is required", nameof(prefix));

        _http.Prefixes.Add(prefix); // e.g. "http://+:8888/"
    }

    public WebListener(
    EngineContext ctx, Scheduler sched, StageGate gate, TaskRegistry reg,
    AqApiModule.IAqFastApi aq, TaskConfigService cfg, TaskHealthTracker health,
    NextRunTracker nextRun, string prefix
)
    : this(ctx, sched, gate, reg, aq, cfg, health, nextRun,
           CreateDefaultPtcProvider(), prefix)
    {
    }

    public async Task RunAsync(CancellationToken ct)
    {
        // ✅ ให้ Ctrl+C ทำให้ listener หลุด GetContextAsync ได้แน่นอน
        using var _ = ct.Register(() =>
        {
            try { _http.Stop(); } catch { }
        });

        try
        {
            _ctx.Log.Info("[HTTP] Starting...");
            _http.Start();
            _ctx.Log.Info($"[HTTP] Listening at {string.Join(", ", _http.Prefixes)}");
        }
        catch (Exception ex)
        {
            _ctx.Log.Error(ex, "[HTTP] Start failed");
            throw;
        }

        try
        {
            while (!ct.IsCancellationRequested)
            {
                HttpListenerContext getCtx;
                try
                {
                    // ✅ ตัด WaitAsync(ct) ออก (ตัวนี้ทำหลายเคสพัง/ค้าง)
                    getCtx = await _http.GetContextAsync();
                }
                catch (HttpListenerException)
                {
                    break; // Stop() จะทำให้หลุดมาทางนี้
                }
                catch (ObjectDisposedException)
                {
                    break;
                }

                HandleAsync(getCtx, ct); // fire-and-forget per request
            }
        }
        finally
        {
            try { _http.Close(); } catch { }
            _ctx.Log.Info("[HTTP] Stopped");
        }
    }

    private async Task HandleAsync(HttpListenerContext hc, CancellationToken ct)
    {
        try
        {
            var req = hc.Request;
            var resp = hc.Response;
            var path = (req.Url?.AbsolutePath ?? "/").TrimEnd('/');
            if (path.Length == 0) path = "/";

            // ✅ CORS preflight (Chrome fetch)
            if (req.HttpMethod == "OPTIONS")
            {
                ApplyCors(hc.Response);
                hc.Response.StatusCode = 204;
                hc.Response.OutputStream.Close();
                return;
            }

            // --- routing ---
            if (req.HttpMethod == "GET" && path == "/health")
            {
                await WriteJsonAsync(hc, 200, new
                {
                    ok = true,
                    serverTsUtc = _ctx.Clock.UtcNow,
                    paused = _gate.IsPaused
                });
                return;
            }

            if (req.HttpMethod == "GET" && path == "/tasks")
            {
                await WriteJsonAsync(hc, 200, new
                {
                    tasks = _reg.ListNames(),
                    paused = _gate.IsPaused,
                    serverTsUtc = _ctx.Clock.UtcNow
                });
                return;
            }

            if (req.HttpMethod == "GET" && path == "/tasks/running")
            {
                var running = _sched.SnapshotRunning()
                    .Select(x => new
                    {
                        id = x.Id,
                        name = x.Name,
                        group = x.Group,
                        startedAtUtc = x.StartedAt,
                        priority = x.Priority.ToString()
                    })
                    .ToList();

                await WriteJsonAsync(hc, 200, new { running, serverTsUtc = _ctx.Clock.UtcNow });
                return;
            }

            // POST /tasks/enqueue 
            if (req.HttpMethod == "POST" && path == "/tasks/enqueue")
            {
                var body = await ReadBodyAsync(req, ct);
                var dto = JsonSerializer.Deserialize<EnqueueDto>(body, _jsonOpt);

                if (dto is null || string.IsNullOrWhiteSpace(dto.Name))
                {
                    await WriteJsonAsync(hc, 400, new { ok = false, error = "Missing name" });
                    return;
                }

                var name = dto.Name.Trim();

                if (!_reg.TryCreate(name, out var task))
                {
                    await WriteJsonAsync(hc, 404, new { ok = false, error = "Unknown task", name });
                    return;
                }

                var ok = _sched.TryEnqueue(task);
                await WriteJsonAsync(hc, ok ? 200 : 409, new
                {
                    ok,
                    paused = _gate.IsPaused,
                    serverTsUtc = _ctx.Clock.UtcNow
                });
                return;
            }

            // POST /tasks/cancel/{guid}
            if (req.HttpMethod == "POST" && path.StartsWith("/tasks/cancel/", StringComparison.OrdinalIgnoreCase))
            {
                var idText = path["/tasks/cancel/".Length..];
                if (!Guid.TryParse(idText, out var id))
                {
                    await WriteJsonAsync(hc, 400, new { ok = false, error = "Bad GUID" });
                    return;
                }

                var ok = _sched.Cancel(id);
                await WriteJsonAsync(hc, ok ? 200 : 404, new { ok, serverTsUtc = _ctx.Clock.UtcNow });
                return;
            }

            if (req.HttpMethod == "POST" && path == "/stage/pause")
            {
                _gate.Pause();
                await WriteJsonAsync(hc, 200, new { ok = true, paused = true, serverTsUtc = _ctx.Clock.UtcNow });
                return;
            }

            if (req.HttpMethod == "POST" && path == "/stage/resume")
            {
                _gate.Resume();
                await WriteJsonAsync(hc, 200, new { ok = true, paused = false, serverTsUtc = _ctx.Clock.UtcNow });
                return;
            }

            if (req.HttpMethod == "POST" && path == "/stage/checkpoint-clone")
            {
                _gate.Pause();
                _sched.CancelAll();

                await _ctx.Db.CheckpointAsync(ct);
                await _ctx.Db.CloneSnapshotAsync(ct);

                _gate.Resume();

                await WriteJsonAsync(hc, 200, new { ok = true, serverTsUtc = _ctx.Clock.UtcNow });
                return;
            }

            // GET /admin/tasks/config
            if (req.HttpMethod == "GET" && path == "/admin/tasks/config")
            {
                var snap = _cfg.Snapshot();
                var rows = new List<TaskConfigRowDto>();

                foreach (var name in _reg.ListNames())
                {
                    _reg.TryCreate(name, out var task);
                    var spec = task.Spec;

                    var enabled = snap.Map.TryGetValue(spec.Name, out var c) ? c.Enabled : true;
                    var intervalMs = snap.Map.TryGetValue(spec.Name, out c) ? c.IntervalMs : 5000;
                    var toOv = snap.Map.TryGetValue(spec.Name, out c) ? c.TimeoutOverrideMs : null;
                    var upd = snap.Map.TryGetValue(spec.Name, out c) ? c.UpdatedAtUnixMs : 0;

                    rows.Add(new TaskConfigRowDto(
                        Name: spec.Name,
                        Group: spec.Group,
                        Priority: spec.Priority.ToString(),
                        Policy: spec.Policy.ToString(),
                        SpecTimeoutMs: spec.Timeout is null ? null : (int)spec.Timeout.Value.TotalMilliseconds,
                        Enabled: enabled,
                        IntervalMs: intervalMs,
                        TimeoutOverrideMs: toOv,
                        UpdatedAtUnixMs: upd
                    ));
                }

                await WriteJsonAsync(hc, 200, new
                {
                    ok = true,
                    loadedAtUtc = snap.LoadedAtUtc,
                    paused = _gate.IsPaused,
                    tasks = rows
                });
                return;
            }

            // POST /admin/tasks/config body: { "updates":[{"name":"TPS_refresh","enabled":true,"intervalMs":5000}] }
            if (req.HttpMethod == "POST" && path == "/admin/tasks/config")
            {
                var body = await ReadBodyAsync(req, ct);
                var dto = JsonSerializer.Deserialize<ConfigUpdateDto>(body, _jsonOpt);

                if (dto?.Updates is null || dto.Updates.Count == 0)
                {
                    await WriteJsonAsync(hc, 400, new { ok = false, error = "Missing updates" });
                    return;
                }

                var updates = dto.Updates.Select(x => new TaskConfigService.UpdateItem(
                    Name: x.Name ?? "",
                    Enabled: x.Enabled,
                    IntervalMs: x.IntervalMs,
                    TimeoutOverrideMs: x.TimeoutOverrideMs
                ));

                var r = await _cfg.ApplyUpdatesAsync(updates, ct).ConfigureAwait(false);
                if (!r.Ok)
                {
                    await WriteJsonAsync(hc, 409, new { ok = false, error = r.Error });
                    return;
                }

                await WriteJsonAsync(hc, 200, new { ok = true, serverTsUtc = _ctx.Clock.UtcNow });
                return;
            }

            // GET /admin/tasks/status
            if (req.HttpMethod == "GET" && path == "/admin/tasks/status")
            {
                var snap = _cfg.Snapshot();
                var running = _sched.SnapshotRunning();

                var runByName = running
                    .GroupBy(x => x.Name, StringComparer.OrdinalIgnoreCase)
                    .ToDictionary(g => g.Key, g => g.First(), StringComparer.OrdinalIgnoreCase);

                var rows = new List<TaskStatusRowDto>();

                foreach (var name in _reg.ListNames())
                {
                    _reg.TryCreate(name, out var task);
                    var spec = task.Spec;

                    var specEnabled = snap.IsEnabled(spec.Name);
                    var effective = specEnabled && !_gate.IsPaused;

                    var isRunning = runByName.TryGetValue(spec.Name, out var rinfo);
                    var started = isRunning ? rinfo!.StartedAt : (DateTimeOffset?)null;
                    var runMs = isRunning ? (long?)(DateTimeOffset.UtcNow - rinfo!.StartedAt).TotalMilliseconds : null;
                    DateTimeOffset? next = _nextRun.GetNext(spec.Name);

                    // ✅ ForceRun hint: use task_settings.updated_at_unixms as an earlier "next" when it's near-future.
                    // UI expects NextRunAtUtc to change immediately after /admin/tasks/forcerun.
                    if (snap.Map.TryGetValue(spec.Name, out var rcfg) && rcfg.Enabled)
                    {
                        try
                        {
                            var dueUtc = DateTimeOffset.FromUnixTimeMilliseconds(rcfg.UpdatedAtUnixMs);
                            var nowUtc = _ctx.Clock.UtcNow;

                            // accept only "near now" to avoid permanently overriding scheduler next
                            if (dueUtc >= nowUtc.AddSeconds(-1) && dueUtc <= nowUtc.AddMinutes(30))
                            {
                                if (!next.HasValue || dueUtc < next.Value)
                                    next = dueUtc;
                            }
                        }
                        catch
                        {
                            // ignore bad unixms
                        }
                    }

                    var h = _health.Get(spec.Name);

                    rows.Add(new TaskStatusRowDto(
                        Name: spec.Name,
                        Group: spec.Group,
                        SpecEnabled: specEnabled,
                        EffectiveEnabled: effective,
                        Running: isRunning,
                        RunningId: isRunning ? rinfo!.Id : null,
                        StartedAtUtc: started,
                        RunningMs: runMs,
                        NextRunAtUtc: next,
                        LastOkAtUtc: h.LastOkAtUtc,
                        LastFailAtUtc: h.LastFailAtUtc,
                        LastError: h.LastError,
                        LastDurationMs: h.LastDurationMs
                    ));
                }

                await WriteJsonAsync(hc, 200, new
                {
                    ok = true,
                    paused = _gate.IsPaused,
                    loadedAtUtc = snap.LoadedAtUtc,
                    serverTsUtc = _ctx.Clock.UtcNow,
                    tasks = rows
                });
                return;
            }

            // POST /admin/pause  (pause only)
            if (req.HttpMethod == "POST" && path == "/admin/pause")
            {
                _gate.Pause();
                await WriteJsonAsync(hc, 200, new { ok = true, paused = true, serverTsUtc = _ctx.Clock.UtcNow });
                return;
            }

            // POST /admin/resume
            if (req.HttpMethod == "POST" && path == "/admin/resume")
            {
                _gate.Resume();
                await WriteJsonAsync(hc, 200, new { ok = true, paused = false, serverTsUtc = _ctx.Clock.UtcNow });
                return;
            }

            // POST /admin/cancelall
            if (req.HttpMethod == "POST" && path == "/admin/cancelall")
            {
                var any = _sched.CancelAll();
                await WriteJsonAsync(hc, 200, new { ok = true, anyCanceled = any, serverTsUtc = _ctx.Clock.UtcNow });
                return;
            }
            // POST /admin/tasks/forcerun
            if (req.HttpMethod == "POST" && path == "/admin/tasks/forcerun")
            {
                try
                {
                    long? dueUnixMs = null;
                    bool enabledOnly = true;

                    if (req.HasEntityBody)
                    {
                        using var sr = new StreamReader(req.InputStream, req.ContentEncoding ?? Encoding.UTF8, true, 4096, leaveOpen: true);
                        var body = await sr.ReadToEndAsync().ConfigureAwait(false);

                        if (!string.IsNullOrWhiteSpace(body))
                        {
                            using var doc = JsonDocument.Parse(body);
                            var root = doc.RootElement;

                            if (root.TryGetProperty("dueUnixMs", out var pDue))
                            {
                                if (pDue.ValueKind == JsonValueKind.Number && pDue.TryGetInt64(out var v))
                                    dueUnixMs = v;
                            }

                            if (root.TryGetProperty("enabledOnly", out var pEn))
                            {
                                if (pEn.ValueKind == JsonValueKind.False)
                                    enabledOnly = false;
                            }
                        }
                    }

                    var nowUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
                    var due = dueUnixMs ?? (nowUnixMs + 3000);

                    // ✅ เรียก service ตรง ๆ
                    var affected = await _cfg.ForceRunAsync(due, enabledOnly, ct).ConfigureAwait(false);

                    await WriteJsonAsync(hc, 200, new
                    {
                        ok = true,
                        updated = affected,
                        enabledOnly,
                        dueUnixMs = due,
                        serverTsUtc = _ctx.Clock.UtcNow
                    });

                    return;
                }
                catch (Exception ex)
                {
                    await WriteJsonAsync(hc, 500, new
                    {
                        ok = false,
                        error = "forcerun_error",
                        message = ex.Message,
                        serverTsUtc = _ctx.Clock.UtcNow
                    });
                    return;
                }
            }

            if (req.HttpMethod == "GET" && path == "/favicon.ico")
            {
                hc.Response.StatusCode = 204; // No Content
                hc.Response.OutputStream.Close();
                return;
            }

            if (req.HttpMethod == "POST" && path == "/api/verify")
            {
                await AqApiModule.HandleVerifyAsync(hc, _ctx, _aq, ct);
                return;
            }

            if (req.HttpMethod == "POST" && path == "/api/process")
            {
                await AqApiModule.HandleProcessAsync(hc, _ctx, _aq, ct);
                return;
            }

            if (req.HttpMethod == "GET" && path == "/api/lookup/products")
            {
                await LookupHandlers.HandleLookupProductsAsync(hc, ct);
                return;
            }

            if (req.HttpMethod == "GET" && path == "/api/lookup/companies")
            {
                await LookupHandlers.HandleLookupCompaniesAsync(hc, ct);
                return;
            }

            if (req.HttpMethod == "POST" && path == "/api/chem_report/export")
            {
                await ChemReportHandlers.HandleChemReportExportAsync(hc, ct);
                return;
            }

            if (req.HttpMethod == "POST" && path == "/api/chem_report")
            {
                await ChemReportQueryHandlers.HandleChemReportQueryAsync(hc, ct);
                return;
            }

            // GET /api/ptc/keys
            if (req.HttpMethod == "GET" && path == "/api/ptc/keys")
            {
                var keys = _ptc.ListKeys();
                await WriteJsonAsync(hc, 200, new
                {
                    ok = true,
                    keys,
                    serverTsUtc = _ctx.Clock.UtcNow
                });
                return;
            }

            // GET /api/ptc/series?key=UZ5411P
            if (req.HttpMethod == "GET" && path == "/api/ptc/series")
            {
                var key = (req.QueryString["key"] ?? "").Trim();
                if (string.IsNullOrWhiteSpace(key))
                {
                    await WriteJsonAsync(hc, 400, new { ok = false, error = "Missing key", serverTsUtc = _ctx.Clock.UtcNow });
                    return;
                }

                var points = _ptc.GetSeries(key);
                await WriteJsonAsync(hc, 200, new
                {
                    ok = true,
                    key,
                    points,
                    count = points.Count,
                    serverTsUtc = _ctx.Clock.UtcNow
                });
                return;
            }

            // OPTIONS/POST /api/online_lab
            if (path == "/api/online_lab")
            {
                if (req.HttpMethod == "OPTIONS")
                {
                    OnlineLabHandlers.WritePreflight(hc);
                    return;
                }

                if (req.HttpMethod == "POST")
                {
                    await OnlineLabHandlers.HandleOnlineLabAsync(hc, _ctx, ct);
                    return;
                }
            }

            if (path == "/api/dps/summary")
            {
                if (req.HttpMethod == "OPTIONS")
                {
                    DpsHandlers.WritePreflight(hc);
                    return;
                }

                if (req.HttpMethod == "GET")
                {
                    await DpsHandlers.HandleDpsSummaryAsync(hc, _ctx, ct);
                    return;
                }
            }

            if (path == "/api/tps/summary")
            {
                if (req.HttpMethod == "OPTIONS")
                {
                    TpsHandlers.WritePreflight(hc);
                    return;
                }

                if (req.HttpMethod == "GET")
                {
                    await TpsHandlers.HandleTpsSummaryAsync(hc, _ctx, ct);
                    return;
                }
            }

            if (path == "/api/rws/summary")
            {
                if (req.HttpMethod == "OPTIONS")
                {
                    TpsHandlers.WritePreflight(hc);
                    return;
                }

                if (req.HttpMethod == "GET")
                {
                    await RwsHandlers.HandleRwsSummaryAsync(hc, _ctx, ct);
                    return;
                }
            }

            if (path == "/api/chem/summary")
            {
                if (req.HttpMethod == "OPTIONS")
                {
                    TpsHandlers.WritePreflight(hc); // หรือ ChemHandlers.WritePreflight ก็ได้
                    return;
                }

                if (req.HttpMethod == "GET")
                {
                    await ChemHandlers.HandleChemSummaryAsync(hc, _ctx, ct);
                    return;
                }
            }

            if (path == "/api/event/summary")
            {
                if (req.HttpMethod == "OPTIONS")
                {
                    EventHandlers.WritePreflight(hc);
                    return;
                }

                if (req.HttpMethod == "GET")
                {
                    await EventHandlers.HandleEventSummaryAsync(hc, _ctx, ct);
                    return;
                }
            }
            await WriteJsonAsync(hc, 404, new { ok = false, error = "Not found", path });
        }
        catch (Exception ex)
        {
            _ctx.Log.Error(ex, "[HTTP] handler error");
            try { await WriteJsonAsync(hc, 500, new { ok = false, error = "Internal error" }); } catch { }
        }
    }

    private sealed record EnqueueDto(string Name);
    // inside WebListener
    private sealed class ConfigUpdateDto
    {
        public List<ConfigUpdateItem>? Updates { get; set; }
    }

    private sealed class ConfigUpdateItem
    {
        public string? Name { get; set; }
        public bool? Enabled { get; set; }
        public int? IntervalMs { get; set; }
        public int? TimeoutOverrideMs { get; set; }
    }

    private static async Task<string> ReadBodyAsync(HttpListenerRequest req, CancellationToken ct)
    {
        // ✅ กัน body ว่าง (fetch บางทียิงมา "")
        if (!req.HasEntityBody) return "{}";
        using var sr = new System.IO.StreamReader(req.InputStream, req.ContentEncoding ?? Encoding.UTF8);
        var s = await sr.ReadToEndAsync(ct);
        return string.IsNullOrWhiteSpace(s) ? "{}" : s;
    }

    private static void ApplyCors(HttpListenerResponse res)
    {
        // ✅ ให้ Chrome DevTools/fetch เรียกได้
        res.Headers["Access-Control-Allow-Origin"] = "*";
        res.Headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS";
        res.Headers["Access-Control-Allow-Headers"] = "Content-Type";
    }

    private async Task WriteJsonAsync(HttpListenerContext hc, int statusCode, object payload)
    {
        var res = hc.Response;

        // ✅ ใส่ CORS ทุก response
        ApplyCors(res);

        res.StatusCode = statusCode;
        res.ContentType = "application/json; charset=utf-8";

        var json = JsonSerializer.Serialize(payload, _jsonOpt);
        var bytes = Encoding.UTF8.GetBytes(json);

        res.ContentLength64 = bytes.Length;
        await res.OutputStream.WriteAsync(bytes, 0, bytes.Length);
        res.OutputStream.Close();
    }

    private static IPtcSeriesProvider CreateDefaultPtcProvider()
    {
        var baseDir = AppContext.BaseDirectory;
        var dbPath = Path.Combine(baseDir, "data.db"); // ✅ ตามที่คุณกำหนด
        return new SqliteUpperLowerProvider(dbPath);
    }
}
#endregion

#region MAIN_PROGRAM
// ==============================
// Program
// ==============================

public static class Program
{
    public static async Task Main()
    {
        var ctx = new EngineContext
        {
            Log = new ConsoleLogger(),
            Db = new NullDbService(),
            Clock = new SystemClock()
        };

        var gate = new StageGate(startPaused: true);

        // NEW: health + nextRun
        var health = new TaskHealthTracker();
        var nextRun = new NextRunTracker();

        // registry
        var reg = new TaskRegistry();
        reg.Register("tps.refresh", () => new TpsRefreshTask());
        reg.Register("dps.refresh", () => new DpsRefreshTask());
        reg.Register("rws1.refresh", () => new Rwp1RefreshTask());
        reg.Register("rws2.refresh", () => new Rwp2RefreshTask());
        reg.Register("chem1.refresh", () => new Chem1RefreshTask());
        reg.Register("chem2.refresh", () => new Chem2RefreshTask());
        reg.Register("branch.refresh", () => new BranchDataRefreshTask());
        reg.Register("rcv38.refresh", () => new Rcv38RefreshTask());
        reg.Register("ptc.query.once", () => new PtcQueryOnceTask());
        reg.Register("onlinelab.query", () => new OnlineLabV2Task());
        reg.Register("Aquadat.refresh", () => new AquadatQueryTask());
        reg.Register("AquadatFWS.refresh", () => new AquadatFwsQueryTask());
        reg.Register("DB_upload.refresh", () => new DbUploadTask());
        reg.Register("DB_download.refresh", () => new DbDownloadTask());
        reg.Register("MDB_upload.refresh", () => new MdbUploadTask());
        reg.Register("MDB_download.refresh", () => new MdbDownloadTask());
        reg.Register("LAB.import.daily", () => new LabImportTask());

        // NEW: config store/service
        var adminDb = Path.Combine(AppContext.BaseDirectory, "engine_admin.db");
        var store = new SqliteTaskSettingsStore(adminDb);
        var cfgSvc = new TaskConfigService(store);

        // Catalog for trigger loop + defaults (ms)
        var catalog = new List<(string Name, Func<IEngineTask> Factory, int DefaultIntervalMs)>
        {
            ("ptc.query.once", () => new PtcQueryOnceTask(), 30000),
            ("tps.refresh", () => new TpsRefreshTask(), 5000),
            ("dps.refresh", () => new DpsRefreshTask(), 5000),
            ("rws1.refresh", () => new Rwp1RefreshTask(), 5000),
            ("rws2.refresh", () => new Rwp2RefreshTask(), 5000),
            ("chem1.refresh", () => new Chem1RefreshTask(), 5000),
            ("chem2.refresh", () => new Chem2RefreshTask(), 5000),
            ("branch.refresh", () => new BranchDataRefreshTask(), 5000),
            ("rcv38.refresh", () => new Rcv38RefreshTask(), 5000),
            ("onlinelab.query", () => new OnlineLabV2Task(), 5000),
            ("Aquadat.refresh", () => new AquadatQueryTask(), 30000),      // default ปลอดภัยขึ้น
            ("AquadatFWS.refresh", () => new AquadatFwsQueryTask(), 30000),// 5 นาที
            ("DB_upload.refresh", () => new DbUploadTask(), 30000),
            ("MDB_upload.refresh", () => new MdbUploadTask(), 30000),
            ("DB_download.refresh", () => new DbUploadTask(), 30000),
            ("MDB_download.refresh", () => new MdbDownloadTask(), 30000),
            ("LAB.import.daily", () => new LabImportTask(), 30000) // 24 ชม.
        };

        using var stopCts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; stopCts.Cancel(); };

        // init config (seed defaults if missing) + publish snapshot
        await cfgSvc.InitializeAsync(
            defaults: catalog.Select(x => (x.Name, x.DefaultIntervalMs)),
            ct: stopCts.Token);

        // Scheduler with per-task gate (enabled)
        var sched = new Scheduler(
            ctx,
            maxConcurrency: 20,
            gate: gate,
            isTaskEnabled: name => cfgSvc.Snapshot().IsEnabled(name),
            health: health
        );

        var schedTask = sched.RunLoopAsync(stopCts.Token);

        // Web listener
        var _aq = new AqFastApiReal();
        var http = new WebListener(ctx, sched, gate, reg, _aq, cfgSvc, health, nextRun, "http://+:8888/");
        var httpTask = http.RunAsync(stopCts.Token);

        // Trigger loop (Keep phase + hot config)
        var triggerTask = TriggerLoop.RunAsync(ctx, sched, gate, cfgSvc, nextRun, catalog, stopCts.Token);

        ctx.Log.Info("[MAIN] Started. Ctrl+C to stop.");

        try
        {
            await Task.WhenAll(schedTask, httpTask, triggerTask);
        }
        catch (OperationCanceledException) { }

        ctx.Log.Info("[MAIN] Stopped.");
    }
}


#endregion