#nullable enable
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Download;
using Google.Apis.Drive.v3;
using Microsoft.Data.Sqlite;

namespace Uroboros
{
    // ==============================
    // MDB reader + sync + convert entry
    // ==============================
    public static class MdbReaderSafe
    {
        // =========================================================
        // Constants
        // =========================================================
        public const string MediaFolderName = "media";
        public const string MediaMdbName = "data.mdb";
        public const string MediaDbTempName = "data.db";
        public const string MediaChemDbName = "chem.db";

        private const string SectionName = "Production";
        private const string IniKeySrcMdb = "mdb_file_location";
        private const string IniKeySrcTs = "timestamp";

        // stamp file (for dedupe convert)
        private const string ChemStampFileName = "chem.db.stamp";
        private const string ChemPointerFileName = "chem.current.txt";

        private static string MediaDir(string startupPath) => Path.Combine(startupPath, MediaFolderName);
        private static string MediaMdbPath(string startupPath) => Path.Combine(MediaDir(startupPath), MediaMdbName);
        private static string MediaChemDbPath(string startupPath) => Path.Combine(MediaDir(startupPath), MediaChemDbName);
        private static string MediaChemStampPath(string startupPath) => Path.Combine(MediaDir(startupPath), ChemStampFileName);
        private static string MediaChemPointerPath(string startupPath) => Path.Combine(MediaDir(startupPath), ChemPointerFileName);

        // =========================================================
        // Public helper: always resolve active chem DB
        // =========================================================
        public static string ResolveActiveChemDbPath(string startupPath)
        {
            var mediaDir = MediaDir(startupPath);
            Directory.CreateDirectory(mediaDir);

            var pointerPath = MediaChemPointerPath(startupPath);
            var fallback = MediaChemDbPath(startupPath); // media\chem.db

            if (!File.Exists(pointerPath))
                return fallback;

            try
            {
                var fileName = (File.ReadAllText(pointerPath, Encoding.UTF8) ?? "").Trim();
                if (string.IsNullOrWhiteSpace(fileName))
                    return fallback;

                var full = Path.Combine(mediaDir, fileName);
                return File.Exists(full) ? full : fallback;
            }
            catch
            {
                return fallback;
            }
        }

        // =========================================================
        // Step 1: Sync MDB -> startup\media\data.mdb (dedupe by src timestamp)
        // =========================================================
        public enum MdbSyncStatus
        {
            Synced,
            UpToDate,
            ConfigNotFound,
            SourceNotFound,
            ErrorOccurred
        }

        public static MdbSyncStatus SyncMdbToMediaAtStartup(
            string startupPath,
            string configPathOrFileName,
            out string? message,
            bool configIsFileName = true)
        {
            var cfgPath = configIsFileName
                ? Path.Combine(startupPath, configPathOrFileName)
                : configPathOrFileName;

            return SyncMdbToMediaAtStartupCore(startupPath, cfgPath, out message);
        }

        private static MdbSyncStatus SyncMdbToMediaAtStartupCore(
            string startupPath,
            string configPath,
            out string? message)
        {
            message = null;

            try
            {
                if (!File.Exists(configPath))
                {
                    message = $"config.ini not found ({configPath})";
                    return MdbSyncStatus.ConfigNotFound;
                }

                var ini = new IniFile(configPath);

                var srcMdb = Unquote(ini.ReadString(SectionName, IniKeySrcMdb, ""));
                if (string.IsNullOrWhiteSpace(srcMdb) || !File.Exists(srcMdb))
                {
                    message = $"Source MDB not found ({srcMdb})";
                    return MdbSyncStatus.SourceNotFound;
                }

                var srcTsUtc = File.GetLastWriteTimeUtc(srcMdb);
                var srcStamp = ToStamp(srcTsUtc);

                var savedStamp = (ini.ReadString(SectionName, IniKeySrcTs, "") ?? "").Trim();

                if (string.Equals(srcStamp, savedStamp, StringComparison.Ordinal))
                {
                    message = $"MDB is up-to-date (stamp={srcStamp})";
                    return MdbSyncStatus.UpToDate;
                }

                var mediaDir = MediaDir(startupPath);
                Directory.CreateDirectory(mediaDir);

                var dstMdb = MediaMdbPath(startupPath);

                File.Copy(srcMdb, dstMdb, overwrite: true);

                ini.WriteString(SectionName, IniKeySrcTs, srcStamp);
                ini.Save();

                message = @"MDB synced to media\data.mdb (stamp=" + srcStamp + ")";
                return MdbSyncStatus.Synced;
            }
            catch (Exception ex)
            {
                message = ex.Message;
                return MdbSyncStatus.ErrorOccurred;
            }
        }

        public static string Unquote(string? s)
        {
            var t = (s ?? "").Trim();
            if (t.Length >= 2 && t.StartsWith("\"", StringComparison.Ordinal) && t.EndsWith("\"", StringComparison.Ordinal))
                return t.Substring(1, t.Length - 2);
            return t;
        }

        // =========================================================
        // Step 1.1/1.2: Convert media\data.mdb -> active chem snapshot
        // =========================================================
        public static bool ConvertMediaMdbToChemDb(
            string startupPath,
            EngineContext? ctx = null,
            CancellationToken ct = default)
        {
            var mediaDir = MediaDir(startupPath);
            Directory.CreateDirectory(mediaDir);

            var mdbPath = MediaMdbPath(startupPath);
            if (!File.Exists(mdbPath))
                throw new FileNotFoundException(@"media\data.mdb not found.", mdbPath);

            var stampPath = MediaChemStampPath(startupPath);
            var mdbStamp = ToStamp(File.GetLastWriteTimeUtc(mdbPath));

            if (File.Exists(stampPath))
            {
                var oldStamp = (SafeReadAllText(stampPath) ?? "").Trim();
                if (string.Equals(oldStamp, mdbStamp, StringComparison.Ordinal))
                {
                    ctx?.Log?.Info($"[MDB] Convert skip (active chem snapshot already matches mdb stamp={mdbStamp})");
                    return false;
                }
            }

            var tempChem = Path.Combine(mediaDir, $"chem.convert.{Guid.NewGuid():N}.db");

            ctx?.Log?.Info($"[MDB] Converting: {mdbPath} -> {tempChem} (stamp={mdbStamp})");

            try
            {
                ct.ThrowIfCancellationRequested();

                MdbToSqliteConverter.ConvertToChemSqlite(mdbPath, tempChem);

                SqliteConnection.ClearAllPools();

                ActivateChemSnapshotFromFile(
                    startupPath: startupPath,
                    sourceDbFile: tempChem,
                    snapshotStamp: mdbStamp,
                    ctx: ctx,
                    keep: 2);

                File.WriteAllText(stampPath, mdbStamp, Encoding.UTF8);

                ctx?.Log?.Info($"[MDB] Convert OK: active chem snapshot updated (stamp={mdbStamp})");
                return true;
            }
            finally
            {
                TryDelete(tempChem);
            }
        }

        // =========================================================
        // Snapshot activation helpers
        // =========================================================
        public static string ActivateChemSnapshotFromFile(
            string startupPath,
            string sourceDbFile,
            string snapshotStamp,
            EngineContext? ctx = null,
            int keep = 5)
        {
            if (!File.Exists(sourceDbFile))
                throw new FileNotFoundException("Source DB file missing.", sourceDbFile);

            var mediaDir = MediaDir(startupPath);
            Directory.CreateDirectory(mediaDir);

            var safeStamp = MakeSafeFileStamp(snapshotStamp);
            var snapshotFileName = $"chem_{safeStamp}.db";
            var snapshotFullPath = Path.Combine(mediaDir, snapshotFileName);

            if (!File.Exists(snapshotFullPath))
                File.Copy(sourceDbFile, snapshotFullPath, overwrite: true);

            ValidateSqlite(snapshotFullPath);

            SetActiveChemDbPath(startupPath, snapshotFileName);

            // compatibility only: seed chem.db ครั้งแรก ถ้ายังไม่มี
            var compatChem = MediaChemDbPath(startupPath);
            if (!File.Exists(compatChem))
            {
                try { File.Copy(snapshotFullPath, compatChem, overwrite: true); }
                catch { /* ignore */ }
            }

            CleanupOldChemSnapshots(startupPath, keep, ctx);

            ctx?.Log?.Info($"[CHEM] Active snapshot = {snapshotFullPath}");
            return snapshotFullPath;
        }

        private static void ValidateSqlite(string dbPath)
        {
            using var conn = new SqliteConnection(
                $"Data Source={dbPath};Mode=ReadOnly;Cache=Shared;Pooling=False;");
            conn.Open();

            using var cmd = conn.CreateCommand();
            cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' LIMIT 1;";
            _ = cmd.ExecuteScalar();
        }

        private static void SetActiveChemDbPath(string startupPath, string dbFileNameOnly)
        {
            var pointer = MediaChemPointerPath(startupPath);
            var tmp = pointer + ".tmp";

            File.WriteAllText(tmp, dbFileNameOnly, new UTF8Encoding(true));

            if (File.Exists(pointer))
                File.Replace(tmp, pointer, null, ignoreMetadataErrors: true);
            else
                File.Move(tmp, pointer);
        }

        private static string GetActiveChemDbPathInternal(string startupPath)
            => ResolveActiveChemDbPath(startupPath);

        private static void CleanupOldChemSnapshots(string startupPath, int keep, EngineContext? ctx)
        {
            try
            {
                var mediaDir = MediaDir(startupPath);
                var active = Path.GetFileName(GetActiveChemDbPathInternal(startupPath));

                var files = Directory.GetFiles(mediaDir, "chem_*.db")
                    .Select(p => new FileInfo(p))
                    .OrderByDescending(f => f.LastWriteTimeUtc)
                    .ToList();

                int kept = 0;

                foreach (var fi in files)
                {
                    if (string.Equals(fi.Name, active, StringComparison.OrdinalIgnoreCase))
                    {
                        kept++;
                        continue;
                    }

                    if (kept < keep)
                    {
                        kept++;
                        continue;
                    }

                    try { fi.Delete(); }
                    catch { /* ignore */ }
                }
            }
            catch (Exception ex)
            {
                ctx?.Log?.Warn($"[CHEM] Cleanup snapshot warning: {ex.Message}");
            }
        }

        private static string MakeSafeFileStamp(string s)
        {
            var bad = Path.GetInvalidFileNameChars();
            var arr = s.Select(ch => bad.Contains(ch) ? '_' : ch).ToArray();
            return new string(arr)
                .Replace(":", "_", StringComparison.Ordinal)
                .Replace(" ", "_", StringComparison.Ordinal);
        }

        private static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { /* ignore */ }
        }

        private static string? SafeReadAllText(string path)
        {
            try { return File.Exists(path) ? File.ReadAllText(path, Encoding.UTF8) : null; }
            catch { return null; }
        }

        private static string ToStamp(DateTime utc)
            => utc.ToUniversalTime().ToString("yyyyMMdd_HHmmss_fffffff", CultureInfo.InvariantCulture);

        // =========================================================
        // INI (simple, safe)
        // =========================================================
        private sealed class IniFile
        {
            private readonly string _path;
            private List<string> _lines;

            public IniFile(string path)
            {
                _path = path;
                _lines = File.ReadAllLines(_path, Encoding.UTF8).ToList();
            }

            public string ReadString(string section, string key, string defaultValue)
            {
                int secIdx = FindSectionIndex(section);
                if (secIdx < 0) return defaultValue;

                for (int i = secIdx + 1; i < _lines.Count; i++)
                {
                    var line = _lines[i];
                    if (IsSectionLine(line)) break;

                    var kv = ParseKeyValue(line);
                    if (kv.hasValue && string.Equals(kv.key, key, StringComparison.OrdinalIgnoreCase))
                        return kv.value;
                }

                return defaultValue;
            }

            public void WriteString(string section, string key, string value)
            {
                int secIdx = FindSectionIndex(section);

                if (secIdx < 0)
                {
                    _lines.Add("");
                    _lines.Add("[" + section + "]");
                    _lines.Add(key + "=" + value);
                    return;
                }

                for (int i = secIdx + 1; i < _lines.Count; i++)
                {
                    var line = _lines[i];
                    if (IsSectionLine(line)) break;

                    var kv = ParseKeyValue(line);
                    if (kv.hasValue && string.Equals(kv.key, key, StringComparison.OrdinalIgnoreCase))
                    {
                        _lines[i] = key + "=" + value;
                        return;
                    }
                }

                int insertAt = _lines.Count;
                for (int i = secIdx + 1; i < _lines.Count; i++)
                {
                    if (IsSectionLine(_lines[i]))
                    {
                        insertAt = i;
                        break;
                    }
                }

                _lines.Insert(insertAt, key + "=" + value);
            }

            public void Save()
            {
                File.WriteAllLines(_path, _lines, Encoding.UTF8);
            }

            private int FindSectionIndex(string section)
            {
                var target = "[" + section.Trim() + "]";
                for (int i = 0; i < _lines.Count; i++)
                {
                    if (string.Equals(_lines[i].Trim(), target, StringComparison.OrdinalIgnoreCase))
                        return i;
                }
                return -1;
            }

            private static bool IsSectionLine(string line)
            {
                var t = line.Trim();
                return t.StartsWith("[", StringComparison.Ordinal) && t.EndsWith("]", StringComparison.Ordinal);
            }

            private static (bool hasValue, string key, string value) ParseKeyValue(string line)
            {
                var t = line.Trim();
                if (t.Length == 0) return (false, "", "");
                if (t.StartsWith(";", StringComparison.Ordinal)) return (false, "", "");

                int p = t.IndexOf('=');
                if (p <= 0) return (false, "", "");

                var k = t.Substring(0, p).Trim();
                var v = t.Substring(p + 1).Trim();
                return (true, k, v);
            }
        }
    }

    // =========================================================
    // MDB -> SQLite converter (data.mdb -> temp chem.db)
    // =========================================================
    internal static class MdbToSqliteConverter
    {
        private const string CreateSql = @"
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS PRODUCT (
  CODE TEXT,
  NAME TEXT
);
CREATE INDEX IF NOT EXISTS IX_PRODUCT_CODE ON PRODUCT(CODE);
CREATE INDEX IF NOT EXISTS IX_PRODUCT_NAME ON PRODUCT(NAME);

CREATE TABLE IF NOT EXISTS COMPANY (
  CODE TEXT,
  NAME TEXT
);
CREATE INDEX IF NOT EXISTS IX_COMPANY_CODE ON COMPANY(CODE);
CREATE INDEX IF NOT EXISTS IX_COMPANY_NAME ON COMPANY(NAME);

CREATE TABLE IF NOT EXISTS CARTYPE (
  CODE TEXT,
  NAME TEXT
);
CREATE INDEX IF NOT EXISTS IX_CARTYPE_CODE ON CARTYPE(CODE);

CREATE TABLE IF NOT EXISTS WDATA (
  STAT    TEXT,
  TRUCK   TEXT,
  CARTYPE TEXT,
  COMPANY TEXT,
  PRODUCT TEXT,

  DAYIN   TEXT,
  TMIN    TEXT,
  W1      REAL,

  DAYOUT  TEXT,
  TMOUT   TEXT,
  W2      REAL
);
CREATE INDEX IF NOT EXISTS IX_WDATA_DAYIN    ON WDATA(DAYIN);
CREATE INDEX IF NOT EXISTS IX_WDATA_DAYOUT   ON WDATA(DAYOUT);
CREATE INDEX IF NOT EXISTS IX_WDATA_STAT     ON WDATA(STAT);
CREATE INDEX IF NOT EXISTS IX_WDATA_PRODUCT  ON WDATA(PRODUCT);
CREATE INDEX IF NOT EXISTS IX_WDATA_COMPANY  ON WDATA(COMPANY);
";

        public static void ConvertToChemSqlite(string mdbPath, string sqlitePath)
        {
            if (!File.Exists(mdbPath))
                throw new FileNotFoundException("MDB not found.", mdbPath);

            var dir = Path.GetDirectoryName(sqlitePath);
            if (!string.IsNullOrWhiteSpace(dir)) Directory.CreateDirectory(dir);

            using var sqlite = new SqliteConnection(
                $"Data Source={sqlitePath};Mode=ReadWriteCreate;Cache=Shared;Pooling=False;");
            sqlite.Open();

            using (var cmd = sqlite.CreateCommand())
            {
                cmd.CommandText = CreateSql;
                cmd.ExecuteNonQuery();
            }

            using var tx = sqlite.BeginTransaction();

            ExecNonQuery(sqlite, "DELETE FROM WDATA;", tx);
            ExecNonQuery(sqlite, "DELETE FROM PRODUCT;", tx);
            ExecNonQuery(sqlite, "DELETE FROM COMPANY;", tx);
            ExecNonQuery(sqlite, "DELETE FROM CARTYPE;", tx);

            CopyTable_PRODUCT(mdbPath, sqlite, tx);
            CopyTable_COMPANY(mdbPath, sqlite, tx);
            CopyTable_CARTYPE(mdbPath, sqlite, tx);
            CopyTable_WDATA(mdbPath, sqlite, tx);

            tx.Commit();

            SqliteConnection.ClearAllPools();
        }

        private static void CopyTable_PRODUCT(string mdbPath, SqliteConnection sqlite, SqliteTransaction tx)
        {
            const string sql = "SELECT CODE, NAME FROM PRODUCT";
            using var insert = sqlite.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT INTO PRODUCT(CODE, NAME) VALUES ($c, $n);";
            var pc = AddP(insert, "$c");
            var pn = AddP(insert, "$n");

            StreamMdbRows(mdbPath, sql, null, rec =>
            {
                pc.Value = SafeText(rec, 0);
                pn.Value = SafeText(rec, 1);
                insert.ExecuteNonQuery();
            });
        }

        private static void CopyTable_COMPANY(string mdbPath, SqliteConnection sqlite, SqliteTransaction tx)
        {
            const string sql = "SELECT CODE, NAME FROM COMPANY";
            using var insert = sqlite.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT INTO COMPANY(CODE, NAME) VALUES ($c, $n);";
            var pc = AddP(insert, "$c");
            var pn = AddP(insert, "$n");

            StreamMdbRows(mdbPath, sql, null, rec =>
            {
                pc.Value = SafeText(rec, 0);
                pn.Value = SafeText(rec, 1);
                insert.ExecuteNonQuery();
            });
        }

        private static void CopyTable_CARTYPE(string mdbPath, SqliteConnection sqlite, SqliteTransaction tx)
        {
            const string sql = "SELECT CODE, NAME FROM CARTYPE";
            using var insert = sqlite.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT INTO CARTYPE(CODE, NAME) VALUES ($c, $n);";
            var pc = AddP(insert, "$c");
            var pn = AddP(insert, "$n");

            StreamMdbRows(mdbPath, sql, null, rec =>
            {
                pc.Value = SafeText(rec, 0);
                pn.Value = SafeText(rec, 1);
                insert.ExecuteNonQuery();
            });
        }

        private static void CopyTable_WDATA(string mdbPath, SqliteConnection sqlite, SqliteTransaction tx)
        {
            const string sql =
                "SELECT STAT, TRUCK, CARTYPE, COMPANY, PRODUCT, DAYIN, TMIN, W1, DAYOUT, TMOUT, W2 " +
                "FROM WDATA WHERE STAT = '2'";

            using var insert = sqlite.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText =
                "INSERT INTO WDATA(STAT,TRUCK,CARTYPE,COMPANY,PRODUCT,DAYIN,TMIN,W1,DAYOUT,TMOUT,W2) " +
                "VALUES ($st,$tr,$ct,$co,$pr,$din,$tmin,$w1,$dout,$tmout,$w2);";

            var p0 = AddP(insert, "$st");
            var p1 = AddP(insert, "$tr");
            var p2 = AddP(insert, "$ct");
            var p3 = AddP(insert, "$co");
            var p4 = AddP(insert, "$pr");
            var p5 = AddP(insert, "$din");
            var p6 = AddP(insert, "$tmin");
            var p7 = AddP(insert, "$w1");
            var p8 = AddP(insert, "$dout");
            var p9 = AddP(insert, "$tmout");
            var p10 = AddP(insert, "$w2");

            StreamMdbRows(mdbPath, sql, null, rec =>
            {
                p0.Value = SafeText(rec, 0);
                p1.Value = SafeText(rec, 1);
                p2.Value = SafeText(rec, 2);
                p3.Value = SafeText(rec, 3);
                p4.Value = SafeText(rec, 4);

                p5.Value = SafeDateIso(rec, 5);
                p6.Value = SafeText(rec, 6);
                p7.Value = SafeReal(rec, 7);

                p8.Value = SafeDateIso(rec, 8);
                p9.Value = SafeText(rec, 9);
                p10.Value = SafeReal(rec, 10);

                insert.ExecuteNonQuery();
            });
        }

        private static SqliteParameter AddP(SqliteCommand cmd, string name)
        {
            var p = cmd.CreateParameter();
            p.ParameterName = name;
            cmd.Parameters.Add(p);
            return p;
        }

        private static void ExecNonQuery(SqliteConnection conn, string sql, SqliteTransaction tx)
        {
            using var cmd = conn.CreateCommand();
            cmd.Transaction = tx;
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        private static string SafeText(IDataRecord rec, int i)
        {
            if (i < 0 || i >= rec.FieldCount) return "";
            if (rec.IsDBNull(i)) return "";
            return (rec.GetValue(i)?.ToString() ?? "").Trim();
        }

        private static double SafeReal(IDataRecord rec, int i)
        {
            if (i < 0 || i >= rec.FieldCount) return 0;
            if (rec.IsDBNull(i)) return 0;

            var v = rec.GetValue(i);
            if (v is double d) return d;
            if (v is float f) return f;
            if (v is decimal m) return (double)m;

            if (double.TryParse(v?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var x)) return x;
            if (double.TryParse(v?.ToString(), NumberStyles.Any, CultureInfo.CurrentCulture, out var y)) return y;
            return 0;
        }

        private static string SafeDateIso(IDataRecord rec, int i)
        {
            if (i < 0 || i >= rec.FieldCount) return "";
            if (rec.IsDBNull(i)) return "";

            try
            {
                var dt = Convert.ToDateTime(rec.GetValue(i), CultureInfo.InvariantCulture);
                return dt.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            }
            catch
            {
                return "";
            }
        }

        private static void StreamMdbRows(
            string mdbPath,
            string sql,
            List<OleDbParameter>? paramsInOrder,
            Action<IDataRecord> rowHandler)
        {
            using var conn = OpenMdb(mdbPath);
            using var cmd = new OleDbCommand(sql, conn);

            if (paramsInOrder is { Count: > 0 })
                cmd.Parameters.AddRange(paramsInOrder.ToArray());

            using var r = cmd.ExecuteReader(CommandBehavior.SequentialAccess);
            if (r is null) return;

            while (r.Read())
                rowHandler(r);
        }

        private static OleDbConnection OpenMdb(string mdbPath)
        {
            var providers = new[]
            {
                "Microsoft.ACE.OLEDB.16.0",
                "Microsoft.ACE.OLEDB.12.0",
                "Microsoft.Jet.OLEDB.4.0"
            };

            Exception? lastEx = null;
            foreach (var prov in providers)
            {
                try
                {
                    var cs = $"Provider={prov};Data Source={mdbPath};Persist Security Info=False;";
                    var conn = new OleDbConnection(cs);
                    conn.Open();
                    return conn;
                }
                catch (Exception ex) { lastEx = ex; }
            }

            throw new InvalidOperationException(
                "Failed to open MDB for conversion: " + (lastEx?.Message ?? "unknown"),
                lastEx);
        }
    }

    // =========================================================
    // ChemExportRefCsv.cs (merged here) — CLI export
    // =========================================================
    public static class ChemExportRefCsv
    {
        public static void ExportChemReceiveCsv_RefFormat(
            string filePath,
            DataTable dtView,
            string exportType,
            string exportedBy,
            string sourceDatabase,
            string dateRangeText,
            string productText,
            string companyText)
        {
            const int TOTAL_COLS = 9;

            if (string.IsNullOrWhiteSpace(filePath))
                throw new ArgumentException("filePath is empty.", nameof(filePath));

            if (dtView is null)
                throw new ArgumentNullException(nameof(dtView), "dtView is null.");

            if (dtView.Rows.Count == 0)
                throw new InvalidOperationException("ไม่มีข้อมูลสำหรับ export");

            var dir = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrWhiteSpace(dir))
                Directory.CreateDirectory(dir);

            var nowText = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

            using var sw = new StreamWriter(
                filePath,
                append: false,
                encoding: new UTF8Encoding(encoderShouldEmitUTF8Identifier: true));

            WriteHeaderLineRef(sw, "Export Type", exportType, TOTAL_COLS);
            WriteHeaderLineRef(sw, "Exported At", nowText, TOTAL_COLS);
            WriteHeaderLineRef(sw, "Exported By", exportedBy, TOTAL_COLS);
            WriteHeaderLineRef(sw, "Source Database", sourceDatabase, TOTAL_COLS);
            WriteHeaderLineRef(sw, "Date Range", dateRangeText, TOTAL_COLS);
            WriteHeaderLineRef(sw, "Product", productText, TOTAL_COLS);
            WriteHeaderLineRef(sw, "Company", companyText, TOTAL_COLS);
            WriteHeaderLineRef(sw, "Rows", dtView.Rows.Count.ToString(CultureInfo.InvariantCulture), TOTAL_COLS);
            WriteHeaderLineRef(sw, "Encoding", "UTF-8 with BOM", TOTAL_COLS);
            WriteHeaderLineRef(sw, "------------------------------------------------------------", "", TOTAL_COLS);

            sw.WriteLine(string.Join(",", Enumerable.Repeat("", TOTAL_COLS)));

            sw.WriteLine(string.Join(",", new[]
            {
                "No.", "Date", "Product", "Company", "Truck", "Type",
                "Gross(In)", "Gross(Out)", "NetWeight"
            }));

            foreach (DataRow r in dtView.Rows)
            {
                var noText = SafeStr(r, "No");
                var dateText = SafeDate_dMy(r, "Date");
                var product = SafeStr(r, "Product");
                var company = SafeStr(r, "Company");
                var truck = SafeStr(r, "Truck");
                var typ = SafeStr(r, "Type");

                var grossIn = SafeNum0(r, "Gross Weight (In)");
                var grossOut = SafeNum0(r, "Gross Weight (Out)");
                var netW = SafeNum0(r, "Net Received Weight");

                var cells = new[]
                {
                    CsvCell(noText),
                    CsvCell(dateText),
                    CsvCell(product),
                    CsvCell(company),
                    CsvCell(truck),
                    CsvCell(typ),
                    CsvCell(grossIn),
                    CsvCell(grossOut),
                    CsvCell(netW)
                };

                sw.WriteLine(string.Join(",", cells));
            }
        }

        private static void WriteHeaderLineRef(StreamWriter sw, string key, string value, int totalCols)
        {
            var keyPad = key.Length < 18 ? key.PadRight(18) : key.Substring(0, 18);

            string firstCell = string.IsNullOrEmpty(value)
                ? "# " + keyPad
                : "# " + keyPad + " : " + value;

            var sb = new StringBuilder();
            sb.Append(CsvCell(firstCell));

            for (int i = 2; i <= totalCols; i++)
            {
                sb.Append(',');
                sb.Append("");
            }

            sw.WriteLine(sb.ToString());
        }

        private static string CsvCell(string? s)
        {
            if (s is null) return "";

            var t = s;
            bool needQuote = t.Contains(',') || t.Contains('"') || t.Contains('\r') || t.Contains('\n');

            if (t.Contains('"'))
                t = t.Replace("\"", "\"\"");

            return needQuote ? "\"" + t + "\"" : t;
        }

        private static string SafeStr(DataRow r, string col)
        {
            if (r.Table.Columns.Contains(col) && r[col] is not DBNull && r[col] is not null)
                return (r[col].ToString() ?? "").Trim();
            return "";
        }

        private static string SafeDate_dMy(DataRow r, string col)
        {
            if (r.Table.Columns.Contains(col) && r[col] is not DBNull && r[col] is not null)
            {
                var d = Convert.ToDateTime(r[col], CultureInfo.InvariantCulture);
                return d.ToString("d/M/yyyy", CultureInfo.InvariantCulture);
            }
            return "";
        }

        private static string SafeNum0(DataRow r, string col)
        {
            if (r.Table.Columns.Contains(col) && r[col] is not DBNull && r[col] is not null)
            {
                if (double.TryParse(r[col].ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var v))
                    return Math.Round(v, 0).ToString("0", CultureInfo.InvariantCulture);
            }
            return "0";
        }
    }

    // =========================================================
    // CHEM Upload Module (Google Drive)
    // =========================================================
    public static class ChemUploadModule
    {
        public sealed record Settings(
            string DriveFolderId,
            string ConfigFolderName,
            string ConfigFileName
        );

        public static Settings DefaultSettings => new Settings(
            DriveFolderId: "1hLIPn9qgjqm4WliNJGsEwmjq78oaXFgm",
            ConfigFolderName: "config_",
            ConfigFileName: "config.ini"
        );

        public sealed record Result(bool Ok, bool Skipped, string Message);

        public static async Task<Result> RunAsync(
            EngineContext ctx,
            CancellationToken ct,
            Settings? settings = null)
        {
            settings ??= DefaultSettings;

            var startupPath = AppContext.BaseDirectory;
            var cfgPath = Path.Combine(startupPath, settings.ConfigFolderName, settings.ConfigFileName);

            string? syncMsg;
            var st = MdbReaderSafe.SyncMdbToMediaAtStartup(startupPath, cfgPath, out syncMsg);

            if (st == MdbReaderSafe.MdbSyncStatus.UpToDate)
                return new Result(true, true, $"✅ Up-to-date: skip convert/upload ({syncMsg})");

            if (st != MdbReaderSafe.MdbSyncStatus.Synced)
                return new Result(false, false, $"❌ Sync failed: {syncMsg}");

            try
            {
                MdbReaderSafe.ConvertMediaMdbToChemDb(startupPath, ctx, ct);
            }
            catch (Exception ex)
            {
                ctx.Log.Error(ex, "[CHEM] convert MDB->active chem snapshot failed");
                return new Result(false, false, "❌ Convert failed: " + ex.Message);
            }

            var activeChemDbPath = MdbReaderSafe.ResolveActiveChemDbPath(startupPath);
            if (!File.Exists(activeChemDbPath))
                return new Result(false, false, $"❌ active chem db missing after convert ({activeChemDbPath})");

            DriveService service;
            try
            {
                service = GoogleDriveHelper.GetDriveServiceOrThrow();
            }
            catch (Exception ex)
            {
                ctx.Log.Error(ex, "[CHEM] drive init error");
                return new Result(false, false, "❌ Drive init failed: " + ex.Message);
            }

            try
            {
                var mediaDir = Path.Combine(startupPath, MdbReaderSafe.MediaFolderName);
                var uploadWorkDir = Path.Combine(mediaDir, "upload_work");
                Directory.CreateDirectory(uploadWorkDir);

                var uploadPath = Path.Combine(uploadWorkDir, "chem.db");
                File.Copy(activeChemDbPath, uploadPath, overwrite: true);

                await DriveSyncCli
                    .UploadFileToSpecificFolderAsync(service, uploadPath, settings.DriveFolderId, ct)
                    .ConfigureAwait(false);

                var fi = new FileInfo(uploadPath);
                var sizeMb = fi.Length / 1024.0 / 1024.0;
                return new Result(true, false, $"⬆️ Upload done: chem.db ({sizeMb:F2} MB)");
            }
            catch (Exception ex)
            {
                ctx.Log.Error(ex, "[CHEM] upload error");
                return new Result(false, false, "❌ Upload failed: " + ex.Message);
            }
        }
    }

    // ==============================
    // MDB Download Module (from Google Drive)
    // ==============================
    public static class ChemDownloadModule
    {
        public sealed record Settings(
            string DriveFolderId,
            string RemoteName,
            string TargetName,
            string LocalBakName,
            string IniSection,
            string IniKey_DownloadTs
        );

        public static Settings DefaultSettings => new Settings(
            DriveFolderId: "1hLIPn9qgjqm4WliNJGsEwmjq78oaXFgm",
            RemoteName: "chem.db",
            TargetName: "chem.db",
            LocalBakName: "chem.db.bak",
            IniSection: "Production",
            IniKey_DownloadTs: "download_mdb_timestamp"
        );

        public sealed record Result(bool Ok, bool Skipped, string Message);

        public static async Task<Result> RunAsync(
            EngineContext ctx,
            CancellationToken ct,
            Settings? settings = null)
        {
            settings ??= DefaultSettings;

            var startupPath = AppContext.BaseDirectory;
            var mediaDir = Path.Combine(startupPath, MdbReaderSafe.MediaFolderName);
            Directory.CreateDirectory(mediaDir);

            var cfgPath = Path.Combine(startupPath, "config_", "config.ini");

            var targetChem = Path.Combine(mediaDir, settings.TargetName); // compatibility only
            var localBak = Path.Combine(mediaDir, settings.LocalBakName);
            var tempDownload = localBak + ".tmp";

            DriveService service;
            try
            {
                service = GoogleDriveHelper.GetDriveServiceOrThrow();
            }
            catch (Exception ex)
            {
                ctx.Log.Error(ex, "❌ Google Drive init failed");
                return new Result(false, false, "❌ Google Drive init failed: " + ex.Message);
            }

            Google.Apis.Drive.v3.Data.File f;
            try
            {
                f = await FindFileByNameAsync(service, settings.DriveFolderId, settings.RemoteName, ct)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ctx.Log.Error(ex, "❌ Find Drive file failed");
                return new Result(false, false, "❌ Find Drive file failed: " + ex.Message);
            }

            if (!f.ModifiedTime.HasValue)
                return new Result(false, false, "❌ Drive file has no modifiedTime");

            DateTime driveLocal;
            var mt = f.ModifiedTime.Value;

            if (mt is DateTime dt)
                driveLocal = dt.ToLocalTime();
            else
                driveLocal = Convert.ToDateTime(mt).ToLocalTime();

            var driveHHmm = driveLocal.ToString("HH:mm", CultureInfo.InvariantCulture);

            var iniRaw = File.Exists(cfgPath)
                ? (IniUtf8BomHelper.ReadIniValueUtf8Bom(cfgPath, settings.IniSection, settings.IniKey_DownloadTs, "") ?? "").Trim()
                : "";

            string iniHHmm = "";
            if (!string.IsNullOrWhiteSpace(iniRaw) &&
                DateTime.TryParse(iniRaw, CultureInfo.InvariantCulture, DateTimeStyles.None, out var iniDt))
            {
                iniHHmm = iniDt.ToString("HH:mm", CultureInfo.InvariantCulture);
            }

            if (!string.IsNullOrWhiteSpace(iniHHmm) &&
                string.Equals(iniHHmm, driveHHmm, StringComparison.Ordinal))
            {
                return new Result(true, true, $"✅ Skip download (HH:mm เท่ากัน) | ini={iniHHmm} drive={driveHHmm}");
            }

            try
            {
                ctx.Log.Info($"[CHEM DL] Download start: {settings.RemoteName} | ini={iniHHmm} drive={driveHHmm}");

                TryDelete(tempDownload);
                TryDelete(localBak);

                var getReq = service.Files.Get(f.Id);

                await using (var fs = new FileStream(tempDownload, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    var prog = await getReq.DownloadAsync(fs, ct).ConfigureAwait(false);
                    if (prog.Status == DownloadStatus.Failed)
                        return new Result(false, false, "❌ Download failed: " + (prog.Exception?.Message ?? "unknown"));
                }

                File.Move(tempDownload, localBak, overwrite: true);

                var size = new FileInfo(localBak).Length;
                if (size <= 0)
                    return new Result(false, false, "❌ Downloaded file is empty (0 bytes): " + localBak);

                ctx.Log.Info($"[CHEM DL] Download ok: {localBak} ({size / 1024.0 / 1024.0:F2} MB)");
            }
            catch (Exception ex)
            {
                ctx.Log.Error(ex, "❌ Download chem.db failed");
                TryDelete(tempDownload);
                return new Result(false, false, "❌ Download failed: " + ex.Message);
            }

            string newDbFileName;
            try
            {
                var stamp = driveLocal.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
                newDbFileName = "chem_" + stamp + ".db";

                var activatedPath = MdbReaderSafe.ActivateChemSnapshotFromFile(
                    startupPath: startupPath,
                    sourceDbFile: localBak,
                    snapshotStamp: stamp,
                    ctx: ctx,
                    keep: 5);

                if (!File.Exists(targetChem))
                {
                    try { File.Copy(activatedPath, targetChem, overwrite: true); }
                    catch { /* ignore */ }
                }

                ctx.Log.Info($"[CHEM DL] Snapshot activated: {activatedPath}");
            }
            catch (Exception ex)
            {
                ctx.Log.Error(ex, "❌ Activate snapshot chem.db failed");
                return new Result(false, false, "❌ Activate snapshot failed: " + ex.Message);
            }

            try
            {
                var newIniTs = driveLocal.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture);

                if (File.Exists(cfgPath))
                {
                    IniUtf8BomHelper.WriteIniValueUtf8Bom(cfgPath, settings.IniSection, settings.IniKey_DownloadTs, newIniTs);
                    ctx.Log.Info($"[CHEM DL] Updated INI: {settings.IniKey_DownloadTs}={newIniTs}");
                }
                else
                {
                    ctx.Log.Warn($"[CHEM DL] config.ini not found, cannot write timestamp: {cfgPath}");
                }
            }
            catch (Exception ex)
            {
                ctx.Log.Warn($"[CHEM DL] Update INI timestamp warning: {ex.Message}");
            }

            return new Result(true, false,
                $"⬇️ Download done: {settings.RemoteName} -> active snapshot {newDbFileName} | ini={iniHHmm} drive={driveHHmm}");
        }

        private static async Task<Google.Apis.Drive.v3.Data.File> FindFileByNameAsync(
            DriveService service,
            string folderId,
            string fileName,
            CancellationToken ct)
        {
            var list = service.Files.List();
            list.Q = $"name = '{EscapeDriveQueryLiteral(fileName)}' and '{folderId}' in parents and trashed = false";
            list.Fields = "files(id, name, modifiedTime, size)";
            list.PageSize = 1;
            list.SupportsAllDrives = true;
            list.IncludeItemsFromAllDrives = true;

            var res = await list.ExecuteAsync(ct).ConfigureAwait(false);
            if (res.Files == null || res.Files.Count == 0)
                throw new FileNotFoundException("Drive file not found: " + fileName);

            return res.Files[0];
        }

        private static string EscapeDriveQueryLiteral(string s) => (s ?? "").Replace("'", "''");

        private static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { /* ignore */ }
        }

        private static class IniUtf8BomHelper
        {
            public static string ReadIniValueUtf8Bom(string iniPath, string section, string key, string defaultValue)
            {
                var text = ReadAllTextUtf8Bom(iniPath);
                var lines = SplitLines(text);

                var targetSection = "[" + section + "]";
                var inSection = false;

                foreach (var raw in lines)
                {
                    var line = raw.Trim();
                    if (line.Length == 0) continue;
                    if (line.StartsWith(";", StringComparison.Ordinal)) continue;

                    if (IsSection(line))
                    {
                        inSection = string.Equals(line, targetSection, StringComparison.OrdinalIgnoreCase);
                        continue;
                    }

                    if (!inSection) continue;

                    var p = line.IndexOf('=');
                    if (p <= 0) continue;

                    var k = line.Substring(0, p).Trim();
                    if (!string.Equals(k, key, StringComparison.OrdinalIgnoreCase)) continue;

                    return line.Substring(p + 1).Trim();
                }

                return defaultValue;
            }

            public static void WriteIniValueUtf8Bom(string iniPath, string section, string key, string value)
            {
                var text = ReadAllTextUtf8Bom(iniPath);
                var lines = SplitLines(text).ToList();

                var targetSection = "[" + section + "]";
                var foundSection = false;
                var wrote = false;

                for (int i = 0; i < lines.Count; i++)
                {
                    var t = lines[i].Trim();

                    if (IsSection(t))
                    {
                        if (string.Equals(t, targetSection, StringComparison.OrdinalIgnoreCase))
                        {
                            foundSection = true;
                            continue;
                        }

                        if (foundSection && !wrote)
                        {
                            lines.Insert(i, key + "=" + value);
                            wrote = true;
                            break;
                        }
                    }

                    if (!foundSection) continue;
                    if (t.Length == 0 || t.StartsWith(";", StringComparison.Ordinal)) continue;
                    if (IsSection(t)) continue;

                    var p = t.IndexOf('=');
                    if (p <= 0) continue;

                    var k = t.Substring(0, p).Trim();
                    if (!string.Equals(k, key, StringComparison.OrdinalIgnoreCase)) continue;

                    lines[i] = key + "=" + value;
                    wrote = true;
                    break;
                }

                if (!foundSection)
                {
                    if (lines.Count > 0 && lines[^1].Length != 0) lines.Add("");
                    lines.Add(targetSection);
                    lines.Add(key + "=" + value);
                }
                else if (foundSection && !wrote)
                {
                    lines.Add(key + "=" + value);
                }

                WriteAllTextUtf8Bom(iniPath, string.Join(Environment.NewLine, lines));
            }

            private static bool IsSection(string line)
                => line.StartsWith("[", StringComparison.Ordinal) && line.EndsWith("]", StringComparison.Ordinal);

            private static string[] SplitLines(string s)
                => (s ?? "").Replace("\r\n", "\n").Replace("\r", "\n").Split('\n');

            private static string ReadAllTextUtf8Bom(string path)
            {
                var bytes = File.ReadAllBytes(path);
                if (bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
                    return Encoding.UTF8.GetString(bytes, 3, bytes.Length - 3);
                return Encoding.UTF8.GetString(bytes);
            }

            private static void WriteAllTextUtf8Bom(string path, string content)
            {
                var bom = new byte[] { 0xEF, 0xBB, 0xBF };
                var body = Encoding.UTF8.GetBytes(content ?? "");
                using var fs = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None);
                fs.Write(bom, 0, bom.Length);
                fs.Write(body, 0, body.Length);
            }
        }
    }
}