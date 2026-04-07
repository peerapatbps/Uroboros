#nullable enable
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

namespace Uroboros
{
    public sealed record TaskRuntimeConfig(
        string Name,
        bool Enabled,
        int IntervalMs,
        int? TimeoutOverrideMs,
        long UpdatedAtUnixMs
    );

    public sealed record TaskConfigRowDto(
        string Name,
        string Group,
        string Priority,
        string Policy,
        int? SpecTimeoutMs,
        bool Enabled,
        int IntervalMs,
        int? TimeoutOverrideMs,
        long UpdatedAtUnixMs
    );

    public sealed record TaskStatusRowDto(
        string Name,
        string Group,
        bool SpecEnabled,
        bool EffectiveEnabled,
        bool Running,
        Guid? RunningId,
        DateTimeOffset? StartedAtUtc,
        long? RunningMs,
        DateTimeOffset? NextRunAtUtc,
        DateTimeOffset? LastOkAtUtc,
        DateTimeOffset? LastFailAtUtc,
        string? LastError,
        long? LastDurationMs
    );

    public interface ITaskSettingsStore
    {
        Task EnsureInitAsync(CancellationToken ct);
        Task<IReadOnlyDictionary<string, TaskRuntimeConfig>> LoadAllAsync(CancellationToken ct);
        Task UpsertManyAsync(IEnumerable<TaskRuntimeConfig> items, CancellationToken ct);
        Task DeleteManyAsync(IEnumerable<string> names, CancellationToken ct);

        // ✅ forcerun: bump updated_at_unixms for enabled tasks (or all)
        Task<int> ForceRunAsync(long dueUnixMs, bool enabledOnly, CancellationToken ct);
    }

    public sealed class SqliteTaskSettingsStore : ITaskSettingsStore
    {
        private readonly string _dbPath;
        public SqliteTaskSettingsStore(string dbPath) => _dbPath = dbPath;

        private SqliteConnection NewConn()
        {
            var cs = new SqliteConnectionStringBuilder
            {
                DataSource = _dbPath,
                Mode = SqliteOpenMode.ReadWriteCreate,
                Cache = SqliteCacheMode.Shared,
                Pooling = true
            }.ToString();

            var c = new SqliteConnection(cs);
            c.Open();

            using var cmd = c.CreateCommand();
            cmd.CommandText = "PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;";
            cmd.ExecuteNonQuery();

            return c;
        }

        public async Task EnsureInitAsync(CancellationToken ct)
        {
            var dir = Path.GetDirectoryName(_dbPath);
            if (!string.IsNullOrWhiteSpace(dir))
                Directory.CreateDirectory(dir);

            await using var c = NewConn();
            await using var cmd = c.CreateCommand();
            cmd.CommandText = @"
CREATE TABLE IF NOT EXISTS task_settings (
  name TEXT PRIMARY KEY,
  enabled INTEGER NOT NULL,
  interval_ms INTEGER NOT NULL,
  timeout_override_ms INTEGER NULL,
  updated_at_unixms INTEGER NOT NULL
);";
            await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        public async Task<IReadOnlyDictionary<string, TaskRuntimeConfig>> LoadAllAsync(CancellationToken ct)
        {
            var map = new Dictionary<string, TaskRuntimeConfig>(StringComparer.OrdinalIgnoreCase);

            await using var c = NewConn();
            await using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT name, enabled, interval_ms, timeout_override_ms, updated_at_unixms FROM task_settings;";
            await using var r = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);

            while (await r.ReadAsync(ct).ConfigureAwait(false))
            {
                var name = r.GetString(0);
                var enabled = r.GetInt64(1) == 1;
                var intervalMs = Convert.ToInt32(r.GetInt64(2), CultureInfo.InvariantCulture);
                int? toMs = r.IsDBNull(3) ? null : Convert.ToInt32(r.GetInt64(3), CultureInfo.InvariantCulture);
                var upd = r.GetInt64(4);

                map[name] = new TaskRuntimeConfig(name, enabled, intervalMs, toMs, upd);
            }

            return map;
        }

        public async Task UpsertManyAsync(IEnumerable<TaskRuntimeConfig> items, CancellationToken ct)
        {
            var list = items.Where(x => !string.IsNullOrWhiteSpace(x.Name))
                            .Select(x => x with { Name = x.Name.Trim() })
                            .ToList();
            if (list.Count == 0) return;

            await using var c = NewConn();
            using var tx = c.BeginTransaction();

            await using var cmd = c.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = @"
INSERT INTO task_settings(name, enabled, interval_ms, timeout_override_ms, updated_at_unixms)
VALUES ($name, $enabled, $interval, $to, $upd)
ON CONFLICT(name) DO UPDATE SET
  enabled=excluded.enabled,
  interval_ms=excluded.interval_ms,
  timeout_override_ms=excluded.timeout_override_ms,
  updated_at_unixms=excluded.updated_at_unixms;";

            var pName = cmd.CreateParameter(); pName.ParameterName = "$name"; cmd.Parameters.Add(pName);
            var pEnabled = cmd.CreateParameter(); pEnabled.ParameterName = "$enabled"; cmd.Parameters.Add(pEnabled);
            var pInterval = cmd.CreateParameter(); pInterval.ParameterName = "$interval"; cmd.Parameters.Add(pInterval);
            var pTo = cmd.CreateParameter(); pTo.ParameterName = "$to"; cmd.Parameters.Add(pTo);
            var pUpd = cmd.CreateParameter(); pUpd.ParameterName = "$upd"; cmd.Parameters.Add(pUpd);

            foreach (var it in list)
            {
                pName.Value = it.Name;
                pEnabled.Value = it.Enabled ? 1 : 0;
                pInterval.Value = it.IntervalMs;
                pTo.Value = (object?)it.TimeoutOverrideMs ?? DBNull.Value;
                pUpd.Value = it.UpdatedAtUnixMs;
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            tx.Commit();
        }

        public async Task DeleteManyAsync(IEnumerable<string> names, CancellationToken ct)
        {
            var list = names.Where(x => !string.IsNullOrWhiteSpace(x))
                            .Select(x => x.Trim())
                            .Distinct(StringComparer.OrdinalIgnoreCase)
                            .ToList();
            if (list.Count == 0) return;

            await using var c = NewConn();
            using var tx = c.BeginTransaction();

            await using var cmd = c.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = "DELETE FROM task_settings WHERE name = $name;";

            var p = cmd.CreateParameter();
            p.ParameterName = "$name";
            cmd.Parameters.Add(p);

            foreach (var n in list)
            {
                p.Value = n;
                await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            tx.Commit();
        }

        // ✅ forcerun: UPDATE updated_at_unixms
        public async Task<int> ForceRunAsync(long dueUnixMs, bool enabledOnly, CancellationToken ct)
        {
            await using var c = NewConn();
            await using var cmd = c.CreateCommand();
            cmd.Parameters.AddWithValue("$due", dueUnixMs);

            cmd.CommandText = enabledOnly
                ? "UPDATE task_settings SET updated_at_unixms = $due WHERE enabled = 1;"
                : "UPDATE task_settings SET updated_at_unixms = $due;";

            return await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    public sealed class TaskConfigSnapshot
    {
        public required DateTimeOffset LoadedAtUtc { get; init; }
        public required IReadOnlyDictionary<string, TaskRuntimeConfig> Map { get; init; }

        public bool IsEnabled(string name) => Map.TryGetValue(name, out var c) ? c.Enabled : true;
        public int GetIntervalMsOrDefault(string name, int defaultMs) => Map.TryGetValue(name, out var c) ? c.IntervalMs : defaultMs;
        public int? GetTimeoutOverrideMs(string name) => Map.TryGetValue(name, out var c) ? c.TimeoutOverrideMs : null;
    }

    public sealed class TaskConfigService
    {
        private readonly ITaskSettingsStore _store;

        // canonical => specName (registry)
        private Dictionary<string, string> _canonToSpec = new(StringComparer.OrdinalIgnoreCase);

        private TaskConfigSnapshot _snap = new TaskConfigSnapshot
        {
            LoadedAtUtc = DateTimeOffset.UtcNow,
            Map = new Dictionary<string, TaskRuntimeConfig>(StringComparer.OrdinalIgnoreCase)
        };

        public TaskConfigService(ITaskSettingsStore store) => _store = store;

        public TaskConfigSnapshot Snapshot() => Volatile.Read(ref _snap);

        // ===== canonicalization (dot/underscore/space/case = same family) =====
        private static string Canon(string? name)
        {
            if (string.IsNullOrWhiteSpace(name)) return "";
            var s = name.Trim().ToLowerInvariant();
            s = s.Replace(".", "").Replace("_", "").Replace(" ", "");
            return s;
        }

        /// <summary>
        /// Map incoming name (from UI/DB) -> actual Spec.Name in registry.
        /// Return null if unknown.
        /// </summary>
        public string? ResolveToSpecName(string? anyName)
        {
            var c = Canon(anyName);
            if (c.Length == 0) return null;
            return _canonToSpec.TryGetValue(c, out var spec) ? spec : null;
        }

        public sealed record UpdateItem(string Name, bool? Enabled, int? IntervalMs, int? TimeoutOverrideMs);

        // guard (optional tune)
        public static readonly IReadOnlyDictionary<string, int> MinIntervalMsByTask =
            new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
            {
                ["DB_upload.refresh"] = 30_000,
                ["MDB_upload.refresh"] = 30_000,
                ["AquadatFWS.refresh"] = 30_000,
                ["Aquadat.refresh"] = 30_000,
                ["ptc.query.once"] = 30_000,
            };

        public async Task InitializeAsync(IEnumerable<(string Name, int DefaultIntervalMs)> defaults, CancellationToken ct)
        {
            await _store.EnsureInitAsync(ct).ConfigureAwait(false);

            var defs = defaults
                .Where(x => !string.IsNullOrWhiteSpace(x.Name))
                .Select(x => (Name: x.Name.Trim(), x.DefaultIntervalMs))
                .ToList();

            // build canon map from registry/spec list
            _canonToSpec = defs
                .GroupBy(x => Canon(x.Name))
                .ToDictionary(g => g.Key, g => g.First().Name, StringComparer.OrdinalIgnoreCase);

            var existing = await _store.LoadAllAsync(ct).ConfigureAwait(false);

            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var toUpsert = new List<TaskRuntimeConfig>();
            var toDelete = new List<string>();

            // group existing by canonical family
            var fam = existing.Values
                .GroupBy(r => Canon(r.Name))
                .ToDictionary(g => g.Key, g => g.OrderByDescending(x => x.UpdatedAtUnixMs).ToList(), StringComparer.OrdinalIgnoreCase);

            foreach (var d in defs)
            {
                var canon = Canon(d.Name);

                fam.TryGetValue(canon, out var members);
                members ??= new List<TaskRuntimeConfig>();

                // pick best existing (newest updatedAt)
                var best = members.FirstOrDefault();

                if (best is null)
                {
                    // seed brand new
                    toUpsert.Add(new TaskRuntimeConfig(
                        Name: d.Name,
                        Enabled: true,
                        IntervalMs: d.DefaultIntervalMs,
                        TimeoutOverrideMs: null,
                        UpdatedAtUnixMs: now
                    ));
                }
                else
                {
                    // always write into canonical spec.Name = d.Name
                    toUpsert.Add(new TaskRuntimeConfig(
                        Name: d.Name,
                        Enabled: best.Enabled,
                        IntervalMs: best.IntervalMs,
                        TimeoutOverrideMs: best.TimeoutOverrideMs,
                        UpdatedAtUnixMs: Math.Max(best.UpdatedAtUnixMs, now)
                    ));

                    // delete everything in this family EXCEPT d.Name
                    foreach (var m in members)
                    {
                        if (!string.Equals(m.Name, d.Name, StringComparison.OrdinalIgnoreCase))
                            toDelete.Add(m.Name);
                    }
                }
            }

            // prune unknown rows (not in registry) to stop “งอก”
            var knownCanon = new HashSet<string>(defs.Select(x => Canon(x.Name)), StringComparer.OrdinalIgnoreCase);
            foreach (var r in existing.Values)
            {
                var c = Canon(r.Name);
                if (c.Length == 0) continue;
                if (!knownCanon.Contains(c))
                    toDelete.Add(r.Name);
            }

            if (toUpsert.Count > 0)
                await _store.UpsertManyAsync(toUpsert, ct).ConfigureAwait(false);

            if (toDelete.Count > 0)
                await _store.DeleteManyAsync(toDelete, ct).ConfigureAwait(false);

            var all = await _store.LoadAllAsync(ct).ConfigureAwait(false);

            Volatile.Write(ref _snap, new TaskConfigSnapshot
            {
                LoadedAtUtc = DateTimeOffset.UtcNow,
                Map = all
            });
        }

        public async Task<(bool Ok, string? Error)> ApplyUpdatesAsync(IEnumerable<UpdateItem> updates, CancellationToken ct)
        {
            var up = updates.ToList();
            if (up.Count == 0) return (true, null);

            var cur = Snapshot().Map;

            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            var toSave = new List<TaskRuntimeConfig>();

            foreach (var u in up)
            {
                var specName = ResolveToSpecName(u.Name);
                if (string.IsNullOrWhiteSpace(specName))
                    return (false, $"Unknown task '{u.Name}'");

                cur.TryGetValue(specName, out var old);

                var enabled = u.Enabled ?? old?.Enabled ?? true;

                var intervalMs = u.IntervalMs ?? old?.IntervalMs ?? 5000;
                if (intervalMs < 250)
                    return (false, $"interval_ms too small for '{specName}'");

                if (MinIntervalMsByTask.TryGetValue(specName, out var minMs) && intervalMs < minMs)
                    return (false, $"interval_ms below min ({minMs} ms) for '{specName}'");

                int? toMs = (u.TimeoutOverrideMs is null) ? old?.TimeoutOverrideMs : u.TimeoutOverrideMs;

                toSave.Add(new TaskRuntimeConfig(
                    Name: specName,                 // ✅ always save by Spec.Name only
                    Enabled: enabled,
                    IntervalMs: intervalMs,
                    TimeoutOverrideMs: toMs,
                    UpdatedAtUnixMs: now
                ));
            }

            await _store.UpsertManyAsync(toSave, ct).ConfigureAwait(false);

            var all = await _store.LoadAllAsync(ct).ConfigureAwait(false);

            Volatile.Write(ref _snap, new TaskConfigSnapshot
            {
                LoadedAtUtc = DateTimeOffset.UtcNow,
                Map = all
            });

            return (true, null);
        }

        // ✅ PUBLIC: forcerun ใช้ข้ามไฟล์ได้
        public async Task<int> ForceRunAsync(long dueUnixMs, bool enabledOnly, CancellationToken ct)
        {
            var affected = await _store.ForceRunAsync(dueUnixMs, enabledOnly, ct).ConfigureAwait(false);

            // refresh snapshot (ให้ /admin/tasks/config อ่านเจอ updated_at ใหม่ทันที)
            var all = await _store.LoadAllAsync(ct).ConfigureAwait(false);
            Volatile.Write(ref _snap, new TaskConfigSnapshot
            {
                LoadedAtUtc = DateTimeOffset.UtcNow,
                Map = all
            });

            return affected;
        }
    }
}