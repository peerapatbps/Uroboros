using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ClosedXML.Excel;
using Microsoft.Data.Sqlite;
using Uroboros;

public static class WayfarerConstants
{
    public const string TaskName = "wayfarer.pm.collect";
    public const string GroupName = "WAYFARER";
    public const int DefaultIntervalMs = 6 * 60 * 60 * 1000;
}

public sealed class WayfarerOptions
{
    public string ProjectPath { get; init; } = "";
    public string ExecutablePath { get; init; } = "";
    public string WorkingDirectory { get; init; } = "";
    public int TimeoutMinutes { get; init; } = 45;
    public string DatabasePath { get; init; } = "";
    public string MetaDbPath { get; init; } = "App_Data/wayfarer_meta.db";
}

public sealed record WayfarerRunSnapshot(
    DateTimeOffset? StartedAtUtc,
    DateTimeOffset? FinishedAtUtc,
    bool Running,
    bool? Success,
    int? ExitCode,
    bool TimedOut,
    string? Error,
    string? CommandLine,
    string? WorkingDirectory,
    string? StdoutTail,
    string? StderrTail
);

public sealed class WayfarerRunTracker
{
    private readonly object _sync = new();
    private WayfarerRunSnapshot _snapshot = new(
        StartedAtUtc: null,
        FinishedAtUtc: null,
        Running: false,
        Success: null,
        ExitCode: null,
        TimedOut: false,
        Error: null,
        CommandLine: null,
        WorkingDirectory: null,
        StdoutTail: null,
        StderrTail: null
    );

    public WayfarerRunSnapshot Snapshot()
    {
        lock (_sync) return _snapshot;
    }

    public void MarkStarted(DateTimeOffset startedAtUtc, string commandLine, string workingDirectory)
    {
        lock (_sync)
        {
            _snapshot = new WayfarerRunSnapshot(
                StartedAtUtc: startedAtUtc,
                FinishedAtUtc: null,
                Running: true,
                Success: null,
                ExitCode: null,
                TimedOut: false,
                Error: null,
                CommandLine: commandLine,
                WorkingDirectory: workingDirectory,
                StdoutTail: null,
                StderrTail: null
            );
        }
    }

    public void MarkCompleted(DateTimeOffset finishedAtUtc, bool success, int? exitCode, bool timedOut, string? error, string? stdoutTail, string? stderrTail)
    {
        lock (_sync)
        {
            _snapshot = _snapshot with
            {
                FinishedAtUtc = finishedAtUtc,
                Running = false,
                Success = success,
                ExitCode = exitCode,
                TimedOut = timedOut,
                Error = error,
                StdoutTail = stdoutTail,
                StderrTail = stderrTail
            };
        }
    }

    public void UpdateOutput(string? stdoutTail, string? stderrTail)
    {
        lock (_sync)
        {
            _snapshot = _snapshot with
            {
                StdoutTail = stdoutTail ?? _snapshot.StdoutTail,
                StderrTail = stderrTail ?? _snapshot.StderrTail
            };
        }
    }
}

public sealed record WayfarerProcessResult(
    int ExitCode,
    bool TimedOut,
    string StdoutTail,
    string StderrTail,
    DateTimeOffset StartedAtUtc,
    DateTimeOffset FinishedAtUtc,
    string CommandLine,
    string WorkingDirectory
);

public sealed class WayfarerIntegration
{
    private readonly WayfarerOptions _options;
    public WayfarerRunTracker RunTracker { get; } = new();

    public WayfarerIntegration(WayfarerOptions options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public WayfarerOptions Options => _options;

    public static WayfarerIntegration LoadFromAppSettings(string baseDirectory, ILogger log)
    {
        var options = new WayfarerOptions();
        var settingsPath = Path.Combine(baseDirectory, "appsettings.json");

        if (File.Exists(settingsPath))
        {
            using var stream = File.OpenRead(settingsPath);
            using var doc = JsonDocument.Parse(stream);
            if (doc.RootElement.TryGetProperty("Wayfarer", out var section))
            {
                options = new WayfarerOptions
                {
                    ProjectPath = GetString(section, "ProjectPath"),
                    ExecutablePath = GetString(section, "ExecutablePath"),
                    WorkingDirectory = GetString(section, "WorkingDirectory"),
                    TimeoutMinutes = GetInt(section, "TimeoutMinutes", 45),
                    DatabasePath = GetString(section, "DatabasePath"),
                    MetaDbPath = GetString(section, "MetaDbPath", "App_Data/wayfarer_meta.db")
                };
            }
        }
        else
        {
            log.Warn($"[WAYFARER] appsettings.json not found at {settingsPath}. Using defaults.");
        }

        return new WayfarerIntegration(new WayfarerOptions
        {
            ProjectPath = ResolvePath(baseDirectory, options.ProjectPath),
            ExecutablePath = ResolvePath(baseDirectory, options.ExecutablePath),
            WorkingDirectory = ResolvePath(baseDirectory, options.WorkingDirectory),
            TimeoutMinutes = options.TimeoutMinutes,
            DatabasePath = ResolvePath(baseDirectory, options.DatabasePath),
            MetaDbPath = ResolvePath(baseDirectory, options.MetaDbPath)
        });
    }

    public async Task EnsureSchedulerTaskRowAsync(string dataDbPath, CancellationToken ct)
    {
        var dir = Path.GetDirectoryName(dataDbPath);
        if (!string.IsNullOrWhiteSpace(dir))
            Directory.CreateDirectory(dir);

        await using var conn = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = dataDbPath,
            Mode = SqliteOpenMode.ReadWriteCreate,
            Cache = SqliteCacheMode.Shared,
            Pooling = true
        }.ToString());
        await conn.OpenAsync(ct).ConfigureAwait(false);

        await using var initCmd = conn.CreateCommand();
        initCmd.CommandText = """
            CREATE TABLE IF NOT EXISTS scheduler_task (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              kind TEXT NOT NULL,
              status TEXT NOT NULL,
              task TEXT NOT NULL,
              interval REAL,
              time TEXT
            );
            """;
        await initCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        await using var upsertCmd = conn.CreateCommand();
        upsertCmd.CommandText = """
            INSERT INTO scheduler_task(kind, status, task, interval, time)
            SELECT 'loop', 'Enable', @task, 6.0, NULL
            WHERE NOT EXISTS (
                SELECT 1 FROM scheduler_task WHERE lower(task) = lower(@task)
            );

            UPDATE scheduler_task
            SET kind = 'loop',
                status = 'Enable',
                interval = 6.0
            WHERE lower(task) = lower(@task);
            """;
        upsertCmd.Parameters.AddWithValue("@task", WayfarerConstants.TaskName);
        await upsertCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public SqliteConnection OpenReadOnlyConnection()
    {
        var dbPath = ResolveDatabasePath();
        EnsureExists(dbPath, "Wayfarer database");

        var conn = new SqliteConnection(BuildReadOnlyConnectionString(dbPath));
        conn.Open();
        return conn;
    }

    public SqliteConnection OpenReadOnlyMetaConnection()
    {
        var metaDbPath = _options.MetaDbPath;
        EnsureExists(metaDbPath, "Wayfarer metadata database");

        var conn = new SqliteConnection(BuildReadOnlyConnectionString(metaDbPath));
        conn.Open();
        return conn;
    }

    public async Task<WayfarerProcessResult> RunCollectorAsync(ILogger log, CancellationToken ct)
    {
        var timeoutMinutes = Math.Max(1, _options.TimeoutMinutes);
        var command = BuildProcessCommand();
        var startedAtUtc = DateTimeOffset.UtcNow;
        RunTracker.MarkStarted(startedAtUtc, command.CommandLine, command.WorkingDirectory);

        using var process = new Process
        {
            StartInfo = new ProcessStartInfo
            {
                FileName = command.FileName,
                Arguments = command.Arguments,
                WorkingDirectory = command.WorkingDirectory,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            },
            EnableRaisingEvents = true
        };

        var stdout = new StringBuilder();
        var stderr = new StringBuilder();
        var stdoutLock = new object();
        var stderrLock = new object();

        process.OutputDataReceived += (_, e) =>
        {
            if (e.Data is null) return;
            lock (stdoutLock)
            {
                AppendTail(stdout, e.Data);
                RunTracker.UpdateOutput(stdout.ToString(), null);
            }
        };
        process.ErrorDataReceived += (_, e) =>
        {
            if (e.Data is null) return;
            lock (stderrLock)
            {
                AppendTail(stderr, e.Data);
                RunTracker.UpdateOutput(null, stderr.ToString());
            }
        };

        if (!process.Start())
            throw new InvalidOperationException("Failed to start Wayfarer process.");

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();

        using var timeoutCts = new CancellationTokenSource(TimeSpan.FromMinutes(timeoutMinutes));
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

        try
        {
            await process.WaitForExitAsync(linkedCts.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeoutCts.IsCancellationRequested)
        {
            TryKillProcessTree(process, log, "timeout");
            var finishedAt = DateTimeOffset.UtcNow;
            var stdoutTail = stdout.ToString();
            var stderrTail = stderr.ToString();
            RunTracker.MarkCompleted(finishedAt, success: false, exitCode: null, timedOut: true, error: "Wayfarer timed out.", stdoutTail, stderrTail);
            throw new TimeoutException($"Wayfarer timed out after {timeoutMinutes} minute(s).");
        }
        catch
        {
            TryKillProcessTree(process, log, "canceled");
            throw;
        }
        finally
        {
            if (!process.HasExited)
                TryKillProcessTree(process, log, "cleanup");
        }

        var finishedAtUtc = DateTimeOffset.UtcNow;
        var outTail = stdout.ToString();
        var errTail = stderr.ToString();
        var success = process.ExitCode == 0;
        var error = success ? null : $"Wayfarer exited with code {process.ExitCode}.";
        RunTracker.MarkCompleted(finishedAtUtc, success, process.ExitCode, timedOut: false, error, outTail, errTail);

        return new WayfarerProcessResult(
            ExitCode: process.ExitCode,
            TimedOut: false,
            StdoutTail: outTail,
            StderrTail: errTail,
            StartedAtUtc: startedAtUtc,
            FinishedAtUtc: finishedAtUtc,
            CommandLine: command.CommandLine,
            WorkingDirectory: command.WorkingDirectory
        );
    }

    private string ResolveDatabasePath()
    {
        if (!string.IsNullOrWhiteSpace(_options.DatabasePath))
            return _options.DatabasePath;

        if (!string.IsNullOrWhiteSpace(_options.ExecutablePath))
            return Path.Combine(Path.GetDirectoryName(_options.ExecutablePath)!, "wayfarer.db");

        if (!string.IsNullOrWhiteSpace(_options.ProjectPath))
        {
            var projectDir = File.Exists(_options.ProjectPath)
                ? Path.GetDirectoryName(_options.ProjectPath)!
                : _options.ProjectPath;
            return Path.Combine(projectDir, "bin", "Debug", "net10.0", "wayfarer.db");
        }

        return Path.Combine(AppContext.BaseDirectory, "wayfarer.db");
    }

    private (string FileName, string Arguments, string WorkingDirectory, string CommandLine) BuildProcessCommand()
    {
        if (!string.IsNullOrWhiteSpace(_options.ExecutablePath) && File.Exists(_options.ExecutablePath))
        {
            var exe = _options.ExecutablePath;
            var workDir = !string.IsNullOrWhiteSpace(_options.WorkingDirectory)
                ? _options.WorkingDirectory
                : Path.GetDirectoryName(exe)!;
            return (exe, "", workDir, Quote(exe));
        }

        if (!string.IsNullOrWhiteSpace(_options.ProjectPath))
        {
            var projectPath = _options.ProjectPath;
            if (Directory.Exists(projectPath))
            {
                var candidate = Path.Combine(projectPath, "Wayfarer.Worker", "Wayfarer.Worker.csproj");
                if (File.Exists(candidate))
                    projectPath = candidate;
            }

            var projectDir = File.Exists(projectPath) ? Path.GetDirectoryName(projectPath)! : projectPath;
            var workDir = !string.IsNullOrWhiteSpace(_options.WorkingDirectory)
                ? _options.WorkingDirectory
                : projectDir;

            var assemblyName = File.Exists(projectPath)
                ? Path.GetFileNameWithoutExtension(projectPath)
                : "Wayfarer.Worker";
            var builtDll = Path.Combine(projectDir, "bin", "Debug", "net10.0", $"{assemblyName}.dll");
            if (File.Exists(builtDll))
            {
                var args = Quote(builtDll);
                return ("dotnet", args, workDir, $"dotnet {args}");
            }

            var runArgs = $"run --project {Quote(projectPath)} --no-build";
            return ("dotnet", runArgs, workDir, $"dotnet {runArgs}");
        }

        throw new InvalidOperationException("Wayfarer configuration is missing. Set Wayfarer:ExecutablePath or Wayfarer:ProjectPath in appsettings.json.");
    }

    private static string ResolvePath(string baseDirectory, string path)
    {
        if (string.IsNullOrWhiteSpace(path)) return "";
        return Path.IsPathRooted(path) ? path : Path.GetFullPath(Path.Combine(baseDirectory, path));
    }

    private static void EnsureExists(string path, string label)
    {
        if (!File.Exists(path))
            throw new FileNotFoundException($"{label} not found: {path}", path);
    }

    private static string BuildReadOnlyConnectionString(string path)
    {
        return new SqliteConnectionStringBuilder
        {
            DataSource = path,
            Mode = SqliteOpenMode.ReadOnly,
            Cache = SqliteCacheMode.Shared,
            Pooling = true
        }.ToString();
    }

    private static void AppendTail(StringBuilder sb, string line)
    {
        const int maxChars = 12000;
        sb.AppendLine(line);
        if (sb.Length > maxChars)
            sb.Remove(0, sb.Length - maxChars);
    }

    private static void TryKillProcessTree(Process process, ILogger log, string reason)
    {
        try
        {
            if (!process.HasExited)
            {
                log.Warn($"[WAYFARER] Killing process tree for {reason}.");
                process.Kill(entireProcessTree: true);
                process.WaitForExit();
            }
        }
        catch (Exception ex)
        {
            log.Warn($"[WAYFARER] Failed to kill process tree: {ex.Message}");
        }
    }

    private static string Quote(string value)
        => value.Contains(' ') ? $"\"{value}\"" : value;

    private static string GetString(JsonElement section, string key, string fallback = "")
    {
        if (section.TryGetProperty(key, out var value) && value.ValueKind == JsonValueKind.String)
            return value.GetString() ?? fallback;
        return fallback;
    }

    private static int GetInt(JsonElement section, string key, int fallback)
    {
        if (section.TryGetProperty(key, out var value))
        {
            if (value.ValueKind == JsonValueKind.Number && value.TryGetInt32(out var n))
                return n;
            if (value.ValueKind == JsonValueKind.String && int.TryParse(value.GetString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out n))
                return n;
        }
        return fallback;
    }
}

public sealed class WayfarerPmCollectTask : IEngineTask
{
    private readonly WayfarerIntegration _integration;

    public WayfarerPmCollectTask(WayfarerIntegration integration)
    {
        _integration = integration;
    }

    public TaskSpec Spec { get; } = new(
        Name: WayfarerConstants.TaskName,
        Group: WayfarerConstants.GroupName,
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromMinutes(50)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        var result = await _integration.RunCollectorAsync(ctx.Log, ct).ConfigureAwait(false);

        ctx.Log.Info($"[WAYFARER] ExitCode={result.ExitCode}");
        if (!string.IsNullOrWhiteSpace(result.StdoutTail))
            ctx.Log.Info($"[WAYFARER][STDOUT]\n{result.StdoutTail}");
        if (!string.IsNullOrWhiteSpace(result.StderrTail))
            ctx.Log.Warn($"[WAYFARER][STDERR]\n{result.StderrTail}");

        if (result.ExitCode != 0)
            throw new InvalidOperationException($"Wayfarer exited with code {result.ExitCode}.");
    }
}

public sealed record WayfarerSummary(
    int Total,
    int Waiting,
    int Scheduled,
    int InProgress,
    int Completed
);

public sealed record WayfarerWorkOrderListItem(
    long WoNo,
    string? DetailUrl,
    string? WoCode,
    string? WoDate,
    string? WoProblem,
    string? WoStatusCode,
    string? WoStatusName,
    string? WoTypeCode,
    long? EqNo,
    long? PuNo,
    string? DeptCode,
    string? TaskName,
    string? PuName,
    string? EqName,
    string? RequestPersonName,
    string? MaintenanceDeptName,
    string? ScheduledStart,
    string? ScheduledFinish,
    int? ScheduledDuration,
    string? ActualStart,
    string? ActualFinish,
    int? ActualDuration,
    int? WorkDuration,
    int? DowntimeDuration,
    string? CompleteDate,
    string? FetchedAtUtc
);

public sealed record WayfarerListResponse(
    int Page,
    int PageSize,
    int Total,
    WayfarerSummary Summary,
    IReadOnlyList<WayfarerWorkOrderListItem> Items
);

public sealed record WayfarerStatusFilter(string? Code, string? Name);
public sealed record WayfarerDeptFilter(string? Code, string? Name);

public sealed record WayfarerFilterResponse(
    IReadOnlyList<WayfarerStatusFilter> Statuses,
    IReadOnlyList<string> Types,
    IReadOnlyList<WayfarerDeptFilter> Departments,
    string? LatestFetchedAtUtc
);

public sealed record WayfarerDetailResponse(
    WayfarerWorkOrderListItem? Overview,
    IReadOnlyList<Dictionary<string, object?>> Tasks,
    IReadOnlyList<Dictionary<string, object?>> People,
    IReadOnlyList<Dictionary<string, object?>> History,
    IReadOnlyList<Dictionary<string, object?>> DamageFailure,
    IReadOnlyList<Dictionary<string, object?>> ActualManhrs,
    Dictionary<string, object?>? Flags
);

public sealed record WayfarerExportRequest(IReadOnlyList<long>? WoNos);

public static class WayfarerApiHandler
{
    private const int MaxPageSize = 200;

    public static async Task<bool> TryHandleAsync(
        HttpListenerContext hc,
        EngineContext ctx,
        Scheduler sched,
        TaskRegistry reg,
        TaskConfigService cfg,
        TaskHealthTracker health,
        NextRunTracker nextRun,
        WayfarerIntegration integration,
        JsonSerializerOptions jsonOpt,
        Func<HttpListenerContext, int, object, Task> writeJsonAsync,
        Func<HttpListenerRequest, CancellationToken, Task<string>> readBodyAsync,
        CancellationToken ct)
    {
        var req = hc.Request;
        var path = (req.Url?.AbsolutePath ?? "/").TrimEnd('/');
        if (path.Length == 0) path = "/";

        if (!path.StartsWith("/api/wayfarer", StringComparison.OrdinalIgnoreCase))
            return false;

        if (req.HttpMethod == "GET" && path.Equals("/api/wayfarer/health", StringComparison.OrdinalIgnoreCase))
        {
            using var conn = integration.OpenReadOnlyConnection();
            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT COUNT(*) FROM pm_wo_index";
            var count = Convert.ToInt32(cmd.ExecuteScalar(), CultureInfo.InvariantCulture);
            await writeJsonAsync(hc, 200, new { ok = true, workOrders = count, serverTime = DateTimeOffset.UtcNow });
            return true;
        }

        if (req.HttpMethod == "GET" && path.Equals("/api/wayfarer/task/status", StringComparison.OrdinalIgnoreCase))
        {
            var payload = BuildTaskStatusPayload(ctx, sched, reg, cfg, health, nextRun, integration);
            await writeJsonAsync(hc, 200, payload);
            return true;
        }

        if (req.HttpMethod == "GET" && path.Equals("/api/wayfarer/task/last-run", StringComparison.OrdinalIgnoreCase))
        {
            var payload = BuildTaskStatusPayload(ctx, sched, reg, cfg, health, nextRun, integration);
            await writeJsonAsync(hc, 200, payload);
            return true;
        }

        if (req.HttpMethod == "POST" && path.Equals("/api/wayfarer/task/trigger", StringComparison.OrdinalIgnoreCase))
        {
            if (!reg.TryCreate(WayfarerConstants.TaskName, out var task))
            {
                await writeJsonAsync(hc, 404, new { ok = false, error = "Wayfarer task is not registered.", serverTsUtc = ctx.Clock.UtcNow });
                return true;
            }

            var accepted = sched.TryEnqueueManual(task);
            await writeJsonAsync(hc, accepted ? 200 : 409, new
            {
                ok = accepted,
                accepted,
                task = WayfarerConstants.TaskName,
                serverTsUtc = ctx.Clock.UtcNow
            });
            return true;
        }

        if (req.HttpMethod == "GET" && path.Equals("/api/wayfarer/filters", StringComparison.OrdinalIgnoreCase))
        {
            await using var conn = integration.OpenReadOnlyConnection();
            var statuses = await ReadStatusFiltersAsync(conn, ct).ConfigureAwait(false);
            var types = await ReadScalarListAsync<string>(conn,
                "SELECT DISTINCT wo_type_code FROM pm_wo_index WHERE wo_type_code IS NOT NULL AND wo_type_code <> '' ORDER BY wo_type_code",
                ct).ConfigureAwait(false);
            var departments = await ReadDeptFiltersAsync(integration, conn, ct).ConfigureAwait(false);

            await using var latestCmd = conn.CreateCommand();
            latestCmd.CommandText = "SELECT MAX(fetched_at_utc) FROM pm_wo_index";
            var latest = await latestCmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;

            await writeJsonAsync(hc, 200, new WayfarerFilterResponse(statuses, types, departments, latest));
            return true;
        }

        if (req.HttpMethod == "GET" && path.Equals("/api/wayfarer/workorders", StringComparison.OrdinalIgnoreCase))
        {
            var query = req.QueryString;
            var page = Clamp(ParseInt(query["page"], 1), 1, int.MaxValue);
            var pageSize = Clamp(ParseInt(query["pageSize"], 25), 1, MaxPageSize);
            var offset = (page - 1) * pageSize;

            var where = BuildWhere(query, out var parameters);
            var orderBy = BuildOrderBy(query["sort"], query["dir"]);

            await using var conn = integration.OpenReadOnlyConnection();
            var total = await CountAsync(conn, where, parameters, ct).ConfigureAwait(false);
            var summary = await SummaryAsync(conn, where, parameters, ct).ConfigureAwait(false);
            var items = await ListAsync(conn, where, parameters, orderBy, pageSize, offset, ct).ConfigureAwait(false);

            await writeJsonAsync(hc, 200, new WayfarerListResponse(page, pageSize, total, summary, items));
            return true;
        }

        if (req.HttpMethod == "GET" && path.StartsWith("/api/wayfarer/workorders/", StringComparison.OrdinalIgnoreCase))
        {
            var idText = path["/api/wayfarer/workorders/".Length..];
            if (!long.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var woNo))
            {
                await writeJsonAsync(hc, 400, new { ok = false, error = "Invalid work order number." });
                return true;
            }

            await using var conn = integration.OpenReadOnlyConnection();
            var detail = await ReadDetailAsync(conn, woNo, ct).ConfigureAwait(false);
            if (detail is null)
            {
                await writeJsonAsync(hc, 404, new { ok = false, message = $"Work order {woNo} not found" });
                return true;
            }

            await writeJsonAsync(hc, 200, detail);
            return true;
        }

        if (req.HttpMethod == "POST" && path.Equals("/api/wayfarer/export", StringComparison.OrdinalIgnoreCase))
        {
            var body = await readBodyAsync(req, ct).ConfigureAwait(false);
            var request = JsonSerializer.Deserialize<WayfarerExportRequest>(body, jsonOpt) ?? new WayfarerExportRequest(Array.Empty<long>());
            var woNos = (request.WoNos ?? Array.Empty<long>())
                .Where(x => x > 0)
                .Distinct()
                .Take(25)
                .ToList();

            if (woNos.Count == 0)
            {
                await writeJsonAsync(hc, 400, "Please select at least one work order.");
                return true;
            }

            await using var conn = integration.OpenReadOnlyConnection();
            using var workbook = new XLWorkbook();

            var overviewRows = new List<WayfarerWorkOrderListItem>();
            var detailRows = new List<WayfarerDetailResponse>();

            foreach (var woNo in woNos)
            {
                var detail = await ReadDetailAsync(conn, woNo, ct).ConfigureAwait(false);
                if (detail?.Overview is null) continue;

                overviewRows.Add(detail.Overview);
                detailRows.Add(detail);
            }

            if (overviewRows.Count == 0)
            {
                await writeJsonAsync(hc, 404, "No selected work orders were found.");
                return true;
            }

            BuildOverviewSheet(workbook, overviewRows);
            foreach (var detail in detailRows)
                BuildDetailSheet(workbook, detail);

            using var stream = new MemoryStream();
            workbook.SaveAs(stream);
            stream.Position = 0;

            var bytes = stream.ToArray();
            var response = hc.Response;
            ApplyBinaryHeaders(response);
            response.StatusCode = 200;
            response.ContentType = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
            response.AddHeader("Content-Disposition", $"attachment; filename=\"wayfarer-export-{DateTime.Now:yyyyMMdd-HHmmss}.xlsx\"");
            response.ContentLength64 = bytes.LongLength;
            await response.OutputStream.WriteAsync(bytes, 0, bytes.Length, ct).ConfigureAwait(false);
            response.OutputStream.Close();
            return true;
        }

        await writeJsonAsync(hc, 404, new { ok = false, error = "Wayfarer endpoint not found", path });
        return true;
    }

    private static object BuildTaskStatusPayload(
        EngineContext ctx,
        Scheduler sched,
        TaskRegistry reg,
        TaskConfigService cfg,
        TaskHealthTracker health,
        NextRunTracker nextRun,
        WayfarerIntegration integration)
    {
        if (!reg.TryCreate(WayfarerConstants.TaskName, out var task))
        {
            return new
            {
                ok = false,
                error = "Wayfarer task is not registered.",
                serverTsUtc = ctx.Clock.UtcNow
            };
        }

        var spec = task.Spec;
        var snap = cfg.Snapshot();
        var running = sched.SnapshotRunning().FirstOrDefault(x => x.Name.Equals(spec.Name, StringComparison.OrdinalIgnoreCase));
        var isRunning = running is not null;
        var started = running?.StartedAt;
        var runningMs = started.HasValue ? (long?)(DateTimeOffset.UtcNow - started.Value).TotalMilliseconds : null;
        var next = nextRun.GetNext(spec.Name);

        if (snap.Map.TryGetValue(spec.Name, out var rcfg) && rcfg.Enabled)
        {
            try
            {
                var dueUtc = DateTimeOffset.FromUnixTimeMilliseconds(rcfg.UpdatedAtUnixMs);
                var nowUtc = ctx.Clock.UtcNow;
                if (dueUtc >= nowUtc.AddSeconds(-1) && dueUtc <= nowUtc.AddMinutes(30))
                {
                    if (!next.HasValue || dueUtc < next.Value)
                        next = dueUtc;
                }
            }
            catch
            {
            }
        }

        var h = health.Get(spec.Name);
        var lastRun = integration.RunTracker.Snapshot();

        return new
        {
            ok = true,
            serverTsUtc = ctx.Clock.UtcNow,
            task = new
            {
                name = spec.Name,
                group = spec.Group,
                specEnabled = snap.IsEnabled(spec.Name),
                effectiveEnabled = snap.IsEnabled(spec.Name),
                running = isRunning,
                runningId = running?.Id,
                startedAtUtc = started,
                runningMs,
                nextRunAtUtc = next,
                lastOkAtUtc = h.LastOkAtUtc,
                lastFailAtUtc = h.LastFailAtUtc,
                lastError = h.LastError,
                lastDurationMs = h.LastDurationMs,
                lastRun
            }
        };
    }

    private static string BaseFrom => """
        FROM pm_wo_index i
        LEFT JOIN pm_wo_schedule_status s ON s.wo_no = i.wo_no
        LEFT JOIN (
            SELECT * FROM (
                SELECT t.*, ROW_NUMBER() OVER (PARTITION BY t.wo_no ORDER BY COALESCE(t.task_order, 999999), t.wo_task_no) AS rn
                FROM pm_wo_task t
            ) tx WHERE tx.rn = 1
        ) t ON t.wo_no = i.wo_no
        LEFT JOIN (
            SELECT wo_no, MAX(person_name) AS request_person_name
            FROM pm_wo_people_departments
            WHERE role_type = 'request_person'
            GROUP BY wo_no
        ) req ON req.wo_no = i.wo_no
        LEFT JOIN (
            SELECT wo_no, MAX(dept_name) AS maintenance_dept_name
            FROM pm_wo_people_departments
            WHERE role_type = 'maintenance_dept'
            GROUP BY wo_no
        ) md ON md.wo_no = i.wo_no
        """;

    private static string SelectList => """
        SELECT i.wo_no, i.detail_url, i.wo_code, i.wo_date, i.wo_problem,
               COALESCE(s.wo_status_code, i.wo_status_code) AS wo_status_code,
               s.wo_status_name,
               i.wo_type_code, i.eq_no, i.pu_no, i.dept_code,
               t.task_name, t.pu_name, t.eq_name,
               req.request_person_name, md.maintenance_dept_name,
               s.sch_start_d AS scheduled_start, s.sch_finish_d AS scheduled_finish, s.sch_duration AS scheduled_duration,
               s.act_start_d AS actual_start, s.act_finish_d AS actual_finish, s.act_duration AS actual_duration,
               s.work_duration, s.dt_duration AS downtime_duration, s.complete_date,
               i.fetched_at_utc
        """;

    private static string BuildWhere(NameValueCollection req, out Dictionary<string, object?> parameters)
    {
        var filters = new List<string> { "1 = 1" };
        parameters = new Dictionary<string, object?>();

        var q = (req["q"] ?? "").Trim();
        if (!string.IsNullOrWhiteSpace(q))
        {
            filters.Add("""
                (
                    CAST(i.wo_no AS TEXT) LIKE @q OR
                    i.wo_code LIKE @q OR
                    i.wo_problem LIKE @q OR
                    t.task_name LIKE @q OR
                    t.eq_name LIKE @q OR
                    t.pu_name LIKE @q OR
                    i.dept_code LIKE @q OR
                    md.maintenance_dept_name LIKE @q OR
                    req.request_person_name LIKE @q
                )
                """);
            parameters["@q"] = $"%{q}%";
        }

        var from = (req["from"] ?? "").Trim();
        if (IsIsoDate(from))
        {
            filters.Add("date(i.wo_date) >= date(@from)");
            parameters["@from"] = from;
        }

        var to = (req["to"] ?? "").Trim();
        if (IsIsoDate(to))
        {
            filters.Add("date(i.wo_date) <= date(@to)");
            parameters["@to"] = to;
        }

        var status = (req["status"] ?? "").Trim();
        if (!string.IsNullOrWhiteSpace(status))
        {
            filters.Add("COALESCE(s.wo_status_code, i.wo_status_code) = @status");
            parameters["@status"] = status;
        }

        var type = (req["type"] ?? "").Trim();
        if (!string.IsNullOrWhiteSpace(type))
        {
            filters.Add("i.wo_type_code = @type");
            parameters["@type"] = type;
        }

        var dept = (req["dept"] ?? "").Trim();
        if (!string.IsNullOrWhiteSpace(dept))
        {
            filters.Add("i.dept_code = @dept");
            parameters["@dept"] = dept;
        }

        return "WHERE " + string.Join(" AND ", filters);
    }

    private static string BuildOrderBy(string? sort, string? dir)
    {
        var column = (sort ?? "wo_date").Trim().ToLowerInvariant() switch
        {
            "wo_no" => "i.wo_no",
            "status" => "COALESCE(s.wo_status_code, i.wo_status_code)",
            "type" => "i.wo_type_code",
            "dept" => "i.dept_code",
            "fetched" => "i.fetched_at_utc",
            _ => "i.wo_date"
        };

        var direction = string.Equals(dir, "asc", StringComparison.OrdinalIgnoreCase) ? "ASC" : "DESC";
        return $"ORDER BY {column} {direction}, i.wo_no DESC";
    }

    private static async Task<int> CountAsync(SqliteConnection conn, string where, Dictionary<string, object?> parameters, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) {BaseFrom} {where}";
        AddParameters(cmd, parameters);
        var value = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return Convert.ToInt32(value, CultureInfo.InvariantCulture);
    }

    private static async Task<WayfarerSummary> SummaryAsync(SqliteConnection conn, string where, Dictionary<string, object?> parameters, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            WITH summary_src AS (
                SELECT
                    COALESCE(s.wo_status_code, i.wo_status_code) AS status_code,
                    s.complete_date
                {BaseFrom}
                {where}
            )
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status_code IN ('10','15','20') THEN 1 ELSE 0 END) AS waiting,
                SUM(CASE WHEN status_code = '30' THEN 1 ELSE 0 END) AS scheduled,
                SUM(CASE WHEN status_code = '50' THEN 1 ELSE 0 END) AS in_progress,
                SUM(CASE WHEN status_code IN ('70','80','99') OR complete_date IS NOT NULL THEN 1 ELSE 0 END) AS completed
            FROM summary_src
            """;
        AddParameters(cmd, parameters);

        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return new WayfarerSummary(0, 0, 0, 0, 0);

        return new WayfarerSummary(
            GetInt(reader, "total") ?? 0,
            GetInt(reader, "waiting") ?? 0,
            GetInt(reader, "scheduled") ?? 0,
            GetInt(reader, "in_progress") ?? 0,
            GetInt(reader, "completed") ?? 0
        );
    }

    private static async Task<IReadOnlyList<WayfarerWorkOrderListItem>> ListAsync(SqliteConnection conn, string where, Dictionary<string, object?> parameters, string orderBy, int pageSize, int offset, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            {SelectList}
            {BaseFrom}
            {where}
            {orderBy}
            LIMIT @limit OFFSET @offset
            """;
        AddParameters(cmd, parameters);
        cmd.Parameters.AddWithValue("@limit", pageSize);
        cmd.Parameters.AddWithValue("@offset", offset);

        var items = new List<WayfarerWorkOrderListItem>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            items.Add(MapListItem(reader));
        return items;
    }

    private static async Task<WayfarerWorkOrderListItem?> ReadOverviewAsync(SqliteConnection conn, long woNo, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = $"""
            {SelectList}
            {BaseFrom}
            WHERE i.wo_no = @woNo
            LIMIT 1
            """;
        cmd.Parameters.AddWithValue("@woNo", woNo);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        return await reader.ReadAsync(ct).ConfigureAwait(false) ? MapListItem(reader) : null;
    }

    private static async Task<WayfarerDetailResponse?> ReadDetailAsync(SqliteConnection conn, long woNo, CancellationToken ct)
    {
        var overview = await ReadOverviewAsync(conn, woNo, ct).ConfigureAwait(false);
        if (overview is null) return null;

        var tasks = await ReadRowsAsync(conn, """
            SELECT task_order, task_name, task_procedure, task_duration, remark, wo_cause,
                   task_date, task_done, pu_code, pu_name, eq_code, eq_name,
                   failure_action_name, failure_mode_name, failure_cause_name
            FROM pm_wo_task
            WHERE wo_no = @woNo
            ORDER BY COALESCE(task_order, 999999), wo_task_no
            """, new() { ["@woNo"] = woNo }, ct).ConfigureAwait(false);

        var people = await ReadRowsAsync(conn, """
            SELECT role_type, person_code, person_name, dept_code, dept_name,
                   costcenter_code, costcenter_name, site_code, site_name
            FROM pm_wo_people_departments
            WHERE wo_no = @woNo
            ORDER BY id
            """, new() { ["@woNo"] = woNo }, ct).ConfigureAwait(false);

        var history = await ReadRowsAsync(conn, """
            SELECT seq_no, type, detail, timestamps, action_person_code, action_person_name
            FROM pm_wo_history
            WHERE wo_no = @woNo
            ORDER BY COALESCE(seq_no, 999999), id
            """, new() { ["@woNo"] = woNo }, ct).ConfigureAwait(false);

        var damageFailure = await ReadRowsAsync(conn, """
            SELECT damage_code, damage_name, failure_mode_code, failure_mode_name,
                   failure_cause_code, failure_cause_name, failure_action_code, failure_action_name,
                   component, effect_desc, cause_desc, action_desc,
                   other_problem, other_cause, other_action, other_action_result
            FROM pm_wo_damage_failure
            WHERE wo_no = @woNo
            """, new() { ["@woNo"] = woNo }, ct).ConfigureAwait(false);

        var actualManhrs = await ReadRowsAsync(conn, """
            SELECT person_code, person_name, dept_code, dept_name, hours, qty, qty_hours,
                   rate_person, unit_cost, amount, flag_act, tr_date
            FROM pm_wo_actual_manhrs
            WHERE wo_no = @woNo
            ORDER BY wo_resc_no
            """, new() { ["@woNo"] = woNo }, ct).ConfigureAwait(false);

        var flags = (await ReadRowsAsync(conn, """
            SELECT hot_work, confine_space, work_at_height, lock_out_tag_out,
                   wait_for_shutdown, wait_for_material, wait_for_other,
                   flag_cancel, flag_his, flag_del, flag_approve_m, flag_approve_resc,
                   flag_approve, flag_not_approved, flag_wait_status, flag_pu,
                   print_flag, authorize_csv
            FROM pm_wo_meta_flags
            WHERE wo_no = @woNo
            LIMIT 1
            """, new() { ["@woNo"] = woNo }, ct).ConfigureAwait(false)).FirstOrDefault();

        return new WayfarerDetailResponse(overview, tasks, people, history, damageFailure, actualManhrs, flags);
    }

    private static WayfarerWorkOrderListItem MapListItem(SqliteDataReader r) => new(
        WoNo: GetLong(r, "wo_no") ?? 0,
        DetailUrl: GetString(r, "detail_url"),
        WoCode: GetString(r, "wo_code"),
        WoDate: GetString(r, "wo_date"),
        WoProblem: GetString(r, "wo_problem"),
        WoStatusCode: GetString(r, "wo_status_code"),
        WoStatusName: GetString(r, "wo_status_name"),
        WoTypeCode: GetString(r, "wo_type_code"),
        EqNo: GetLong(r, "eq_no"),
        PuNo: GetLong(r, "pu_no"),
        DeptCode: GetString(r, "dept_code"),
        TaskName: GetString(r, "task_name"),
        PuName: GetString(r, "pu_name"),
        EqName: GetString(r, "eq_name"),
        RequestPersonName: GetString(r, "request_person_name"),
        MaintenanceDeptName: GetString(r, "maintenance_dept_name"),
        ScheduledStart: GetString(r, "scheduled_start"),
        ScheduledFinish: GetString(r, "scheduled_finish"),
        ScheduledDuration: GetInt(r, "scheduled_duration"),
        ActualStart: GetString(r, "actual_start"),
        ActualFinish: GetString(r, "actual_finish"),
        ActualDuration: GetInt(r, "actual_duration"),
        WorkDuration: GetInt(r, "work_duration"),
        DowntimeDuration: GetInt(r, "downtime_duration"),
        CompleteDate: GetString(r, "complete_date"),
        FetchedAtUtc: GetString(r, "fetched_at_utc")
    );

    private static async Task<IReadOnlyList<WayfarerStatusFilter>> ReadStatusFiltersAsync(SqliteConnection conn, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = """
            SELECT COALESCE(s.wo_status_code, i.wo_status_code) AS code,
                   MAX(s.wo_status_name) AS name
            FROM pm_wo_index i
            LEFT JOIN pm_wo_schedule_status s ON s.wo_no = i.wo_no
            WHERE COALESCE(s.wo_status_code, i.wo_status_code) IS NOT NULL
            GROUP BY COALESCE(s.wo_status_code, i.wo_status_code)
            ORDER BY CAST(COALESCE(s.wo_status_code, i.wo_status_code) AS INTEGER)
            """;
        var list = new List<WayfarerStatusFilter>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            list.Add(new WayfarerStatusFilter(GetString(reader, "code"), GetString(reader, "name")));
        return list;
    }

    private static async Task<IReadOnlyList<WayfarerDeptFilter>> ReadDeptFiltersAsync(WayfarerIntegration integration, SqliteConnection mainConn, CancellationToken ct)
    {
        try
        {
            await using var metaConn = integration.OpenReadOnlyMetaConnection();
            await using var cmd = metaConn.CreateCommand();
            cmd.CommandText = """
                SELECT deptCode AS code, deptName AS name
                FROM meta_departments
                WHERE deptCode IS NOT NULL
                  AND deptCode <> ''
                ORDER BY deptCode
                """;
            var metaList = new List<WayfarerDeptFilter>();
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
                metaList.Add(new WayfarerDeptFilter(GetString(reader, "code"), GetString(reader, "name")));

            if (metaList.Count > 0)
                return metaList;
        }
        catch (FileNotFoundException)
        {
        }
        catch (SqliteException)
        {
        }

        await using var fallbackCmd = mainConn.CreateCommand();
        fallbackCmd.CommandText = """
            SELECT i.dept_code AS code, MAX(p.dept_name) AS name
            FROM pm_wo_index i
            LEFT JOIN pm_wo_people_departments p ON p.wo_no = i.wo_no AND p.dept_code = i.dept_code
            WHERE i.dept_code IS NOT NULL AND i.dept_code <> ''
            GROUP BY i.dept_code
            ORDER BY i.dept_code
            """;
        var fallbackList = new List<WayfarerDeptFilter>();
        await using var fallbackReader = await fallbackCmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await fallbackReader.ReadAsync(ct).ConfigureAwait(false))
            fallbackList.Add(new WayfarerDeptFilter(GetString(fallbackReader, "code"), GetString(fallbackReader, "name")));
        return fallbackList;
    }

    private static async Task<IReadOnlyList<T>> ReadScalarListAsync<T>(SqliteConnection conn, string sql, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        var list = new List<T>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            if (!reader.IsDBNull(0))
                list.Add((T)Convert.ChangeType(reader.GetValue(0), typeof(T), CultureInfo.InvariantCulture));
        }
        return list;
    }

    private static async Task<IReadOnlyList<Dictionary<string, object?>>> ReadRowsAsync(SqliteConnection conn, string sql, Dictionary<string, object?> parameters, CancellationToken ct)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        AddParameters(cmd, parameters);

        var rows = new List<Dictionary<string, object?>>();
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            for (var i = 0; i < reader.FieldCount; i++)
            {
                var name = reader.GetName(i);
                row[name] = reader.IsDBNull(i) ? null : reader.GetValue(i);
            }
            rows.Add(row);
        }
        return rows;
    }

    private static void AddParameters(SqliteCommand cmd, Dictionary<string, object?> parameters)
    {
        foreach (var (key, value) in parameters)
            cmd.Parameters.AddWithValue(key, value ?? DBNull.Value);
    }

    private static string? GetString(SqliteDataReader r, string name)
    {
        var ordinal = r.GetOrdinal(name);
        return r.IsDBNull(ordinal) ? null : Convert.ToString(r.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static int? GetInt(SqliteDataReader r, string name)
    {
        var ordinal = r.GetOrdinal(name);
        return r.IsDBNull(ordinal) ? null : Convert.ToInt32(r.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static long? GetLong(SqliteDataReader r, string name)
    {
        var ordinal = r.GetOrdinal(name);
        return r.IsDBNull(ordinal) ? null : Convert.ToInt64(r.GetValue(ordinal), CultureInfo.InvariantCulture);
    }

    private static int ParseInt(string? value, int fallback)
        => int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : fallback;

    private static int Clamp(int value, int min, int max) => Math.Min(Math.Max(value, min), max);

    private static bool IsIsoDate(string value)
        => DateOnly.TryParseExact(value, "yyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out _);

    private static void BuildOverviewSheet(XLWorkbook workbook, IReadOnlyList<WayfarerWorkOrderListItem> rows)
    {
        var ws = workbook.Worksheets.Add("Overview");
        ws.Cell(1, 1).Value = "Wayfarer Export";
        ws.Cell(2, 1).Value = "Generated";
        ws.Cell(2, 2).Value = FormatThaiDateTime(DateTimeOffset.UtcNow.ToString("O", CultureInfo.InvariantCulture));
        ws.Cell(3, 1).Value = "Selected Work Orders";
        ws.Cell(3, 2).Value = rows.Count;

        var headers = new[]
        {
            "WO No", "WO Code", "WO Date", "Type", "Status Code", "Status Name",
            "Problem", "Task Name", "PU", "EQ", "Dept", "Maintenance Dept",
            "Scheduled Start", "Scheduled Finish", "Actual Start", "Actual Finish",
            "Work Duration", "Downtime", "Fetched"
        };

        for (var i = 0; i < headers.Length; i++)
            ws.Cell(5, i + 1).Value = headers[i];

        var rowIndex = 6;
        foreach (var row in rows)
        {
            ws.Cell(rowIndex, 1).Value = row.WoNo;
            ws.Cell(rowIndex, 2).Value = row.WoCode;
            ws.Cell(rowIndex, 3).Value = NormalizeExportValue("wo_date", row.WoDate);
            ws.Cell(rowIndex, 4).Value = row.WoTypeCode;
            ws.Cell(rowIndex, 5).Value = row.WoStatusCode;
            ws.Cell(rowIndex, 6).Value = row.WoStatusName;
            ws.Cell(rowIndex, 7).Value = row.WoProblem;
            ws.Cell(rowIndex, 8).Value = row.TaskName;
            ws.Cell(rowIndex, 9).Value = row.PuName ?? row.PuNo?.ToString();
            ws.Cell(rowIndex, 10).Value = row.EqName ?? row.EqNo?.ToString();
            ws.Cell(rowIndex, 11).Value = row.DeptCode;
            ws.Cell(rowIndex, 12).Value = row.MaintenanceDeptName;
            ws.Cell(rowIndex, 13).Value = NormalizeExportValue("scheduled_start", row.ScheduledStart);
            ws.Cell(rowIndex, 14).Value = NormalizeExportValue("scheduled_finish", row.ScheduledFinish);
            ws.Cell(rowIndex, 15).Value = NormalizeExportValue("actual_start", row.ActualStart);
            ws.Cell(rowIndex, 16).Value = NormalizeExportValue("actual_finish", row.ActualFinish);
            ws.Cell(rowIndex, 17).Value = row.WorkDuration;
            ws.Cell(rowIndex, 18).Value = row.DowntimeDuration;
            ws.Cell(rowIndex, 19).Value = NormalizeExportValue("fetched_at_utc", row.FetchedAtUtc);
            rowIndex++;
        }

        StyleSheet(ws, 5, headers.Length);
    }

    private static void BuildDetailSheet(XLWorkbook workbook, WayfarerDetailResponse detail)
    {
        var overview = detail.Overview!;
        var ws = workbook.Worksheets.Add(SafeSheetName($"WO-{overview.WoNo}-{overview.WoCode}"));
        var row = 1;

        ws.Cell(row, 1).Value = $"Work Order {overview.WoNo}";
        ws.Cell(row, 2).Value = overview.WoCode;
        ws.Range(row, 1, row, 4).Style.Font.Bold = true;
        row += 2;

        row = WriteKeyValueSection(ws, row, "Overview", new Dictionary<string, object?>
        {
            ["WO No"] = overview.WoNo,
            ["WO Code"] = overview.WoCode,
            ["WO Date"] = overview.WoDate,
            ["Type"] = overview.WoTypeCode,
            ["Status Code"] = overview.WoStatusCode,
            ["Status Name"] = overview.WoStatusName,
            ["Problem"] = overview.WoProblem,
            ["Task"] = overview.TaskName,
            ["PU"] = overview.PuName ?? overview.PuNo?.ToString(),
            ["EQ"] = overview.EqName ?? overview.EqNo?.ToString(),
            ["Dept"] = overview.DeptCode,
            ["Maintenance Dept"] = overview.MaintenanceDeptName,
            ["Request Person"] = overview.RequestPersonName,
            ["Scheduled Start"] = overview.ScheduledStart,
            ["Scheduled Finish"] = overview.ScheduledFinish,
            ["Actual Start"] = overview.ActualStart,
            ["Actual Finish"] = overview.ActualFinish,
            ["Complete Date"] = overview.CompleteDate,
            ["Work Duration"] = overview.WorkDuration,
            ["Downtime Duration"] = overview.DowntimeDuration,
            ["Fetched"] = overview.FetchedAtUtc,
            ["Detail URL"] = overview.DetailUrl
        });

        row = WriteTableSection(ws, row, "Tasks", detail.Tasks);
        row = WriteTableSection(ws, row, "People / Departments", detail.People);
        row = WriteTableSection(ws, row, "History", detail.History);
        row = WriteTableSection(ws, row, "Damage / Failure", detail.DamageFailure);
        row = WriteTableSection(ws, row, "Actual Manhours", detail.ActualManhrs);

        if (detail.Flags is not null)
            row = WriteTableSection(ws, row, "Meta Flags", new[] { detail.Flags });

        ws.Columns().AdjustToContents();
    }

    private static int WriteKeyValueSection(IXLWorksheet ws, int row, string title, IReadOnlyDictionary<string, object?> values)
    {
        ws.Cell(row, 1).Value = title;
        ws.Cell(row, 1).Style.Font.Bold = true;
        row++;
        foreach (var item in values)
        {
            ws.Cell(row, 1).Value = item.Key;
            ws.Cell(row, 2).Value = NormalizeExportValue(item.Key, item.Value);
            row++;
        }
        return row + 1;
    }

    private static int WriteTableSection(IXLWorksheet ws, int row, string title, IReadOnlyList<Dictionary<string, object?>> rows)
    {
        ws.Cell(row, 1).Value = title;
        ws.Cell(row, 1).Style.Font.Bold = true;
        row++;

        if (rows.Count == 0)
        {
            ws.Cell(row, 1).Value = "No data";
            return row + 2;
        }

        var columns = rows[0].Keys.ToList();
        for (var i = 0; i < columns.Count; i++)
            ws.Cell(row, i + 1).Value = columns[i];

        var headerRow = row;
        row++;

        foreach (var entry in rows)
        {
            for (var i = 0; i < columns.Count; i++)
                ws.Cell(row, i + 1).Value = entry.TryGetValue(columns[i], out var value) ? NormalizeExportValue(columns[i], value) : "";
            row++;
        }

        ws.Range(headerRow, 1, headerRow, columns.Count).Style.Font.Bold = true;
        ws.Range(headerRow, 1, row - 1, columns.Count).Style.Border.OutsideBorder = XLBorderStyleValues.Thin;
        ws.Range(headerRow, 1, row - 1, columns.Count).Style.Border.InsideBorder = XLBorderStyleValues.Thin;
        return row + 1;
    }

    private static void StyleSheet(IXLWorksheet ws, int headerRow, int columnCount)
    {
        var lastRow = ws.LastRowUsed()?.RowNumber() ?? headerRow;
        ws.Range(headerRow, 1, headerRow, columnCount).Style.Font.Bold = true;
        ws.Range(headerRow, 1, headerRow, columnCount).Style.Fill.BackgroundColor = XLColor.FromHtml("#D9EAF7");
        ws.Range(headerRow, 1, lastRow, columnCount).Style.Border.OutsideBorder = XLBorderStyleValues.Thin;
        ws.Range(headerRow, 1, lastRow, columnCount).Style.Border.InsideBorder = XLBorderStyleValues.Thin;
        ws.Columns().AdjustToContents();
    }

    private static string SafeSheetName(string? raw)
    {
        var name = string.IsNullOrWhiteSpace(raw) ? "Sheet" : raw;
        foreach (var ch in new[] { '\\', '/', '?', '*', '[', ']', ':' })
            name = name.Replace(ch, '-');
        return name.Length <= 31 ? name : name[..31];
    }

    private static string NormalizeExportValue(string? key, object? value)
    {
        if (value is null) return "";
        var text = value.ToString() ?? "";
        if (string.IsNullOrWhiteSpace(text)) return "";
        if (LooksLikeDateKey(key) || LooksLikeIsoDateTime(text))
            return FormatThaiDateTime(text);
        return text;
    }

    private static bool LooksLikeDateKey(string? key)
    {
        if (string.IsNullOrWhiteSpace(key)) return false;
        var k = key.Replace(" ", "_").ToLowerInvariant();
        return k.Contains("date") || k.Contains("time") || k.Contains("timestamp") || k.Contains("fetched");
    }

    private static bool LooksLikeIsoDateTime(string text)
        => text.Contains('T') && (text.EndsWith("Z", StringComparison.OrdinalIgnoreCase) || text.Contains('+'));

    private static string FormatThaiDateTime(string text)
    {
        if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dto))
            return TimeZoneInfo.ConvertTime(dto, GetBangkokTimeZone()).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

        if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out var dt))
            return dt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

        return text;
    }

    private static TimeZoneInfo GetBangkokTimeZone()
    {
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById("SE Asia Standard Time");
        }
        catch (TimeZoneNotFoundException)
        {
            return TimeZoneInfo.FindSystemTimeZoneById("Asia/Bangkok");
        }
    }

    private static void ApplyBinaryHeaders(HttpListenerResponse resp)
    {
        resp.Headers["Access-Control-Allow-Origin"] = "*";
        resp.Headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS";
        resp.Headers["Access-Control-Allow-Headers"] = "Content-Type";
    }
}
