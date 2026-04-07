// GoogleDrive.cs  (C# / .NET 10 CLI)  — COPY/PASTE WHOLE FILE
// NuGet: Google.Apis.Drive.v3, Google.Apis.Auth, Google.Apis, Google.Apis.Core
// NuGet: Microsoft.Data.Sqlite
//
// FLOW (ตามที่คุณสั่ง ห้ามออกนอกทาง):
//   source:   data.db
//   snapshot: data_ghost.db   (ชื่อ "snapshotPath" ยังใช้เหมือนเดิม)
// แต่เพื่อชนะ Windows file-lock 100% เราจะ "ไม่แตะ/ไม่ replace" data_ghost.db บนดิสก์
// และอัปโหลด snapshot temp ขึ้น Drive โดย "บังคับชื่อบน Drive" ให้เป็น data_ghost.db
//
// IMPORTANT:
// - ต้องมี IEngineTask/TaskSpec/EngineContext/ILogger ของคุณอยู่แล้ว
// - ILogger.Error signature เป็น Error(Exception ex, string msg)

#nullable enable
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Google;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Drive.v3;
using Google.Apis.Services;
using Google.Apis.Util.Store;
using Microsoft.Data.Sqlite;

namespace Uroboros
{
    // =========================================================
    // Drive + File helpers (CLI)
    // =========================================================
    internal static class DriveSyncCli
    {
        // 1) Upload file -> folder (Update if exists by SAME NAME, else Create)
        public static async Task UploadFileToSpecificFolderAsync(
            DriveService service,
            string filePath,
            string folderId,
            CancellationToken ct = default)
        {
            if (service is null) throw new ArgumentNullException(nameof(service));
            if (!File.Exists(filePath)) throw new FileNotFoundException("Local file not found", filePath);

            string fileName = Path.GetFileName(filePath);

            var searchRequest = service.Files.List();
            searchRequest.Q = $"name = '{EscapeDriveQueryLiteral(fileName)}' and '{folderId}' in parents and trashed = false";
            searchRequest.Fields = "files(id, name)";
            searchRequest.PageSize = 1;
            searchRequest.SupportsAllDrives = true;
            searchRequest.IncludeItemsFromAllDrives = true;

            var searchResult = await searchRequest.ExecuteAsync(ct).ConfigureAwait(false);
            string? existingFileId = (searchResult.Files != null && searchResult.Files.Count > 0)
                ? searchResult.Files[0].Id
                : null;

            await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);

            if (!string.IsNullOrWhiteSpace(existingFileId))
            {
                var updateMetadata = new Google.Apis.Drive.v3.Data.File { Name = fileName };
                var updateRequest = service.Files.Update(updateMetadata, existingFileId, fileStream, "application/octet-stream");
                updateRequest.Fields = "id";
                updateRequest.SupportsAllDrives = true;
                await updateRequest.UploadAsync(ct).ConfigureAwait(false);
            }
            else
            {
                var createMetadata = new Google.Apis.Drive.v3.Data.File
                {
                    Name = fileName,
                    Parents = new List<string> { folderId }
                };

                var createRequest = service.Files.Create(createMetadata, fileStream, "application/octet-stream");
                createRequest.Fields = "id";
                createRequest.SupportsAllDrives = true;
                await createRequest.UploadAsync(ct).ConfigureAwait(false);
            }
        }

        // 1b) Upload file -> folder BUT force remote name (สำคัญ: ชนะ lock โดยไม่แตะ data_ghost.db บนดิสก์)
        public static async Task UploadFileToSpecificFolderAsNameAsync(
            DriveService service,
            string filePath,
            string folderId,
            string remoteFileName,
            CancellationToken ct = default)
        {
            if (service is null) throw new ArgumentNullException(nameof(service));
            if (!File.Exists(filePath)) throw new FileNotFoundException("Local file not found", filePath);
            if (string.IsNullOrWhiteSpace(remoteFileName)) throw new ArgumentException("remoteFileName is required", nameof(remoteFileName));

            var searchRequest = service.Files.List();
            searchRequest.Q = $"name = '{EscapeDriveQueryLiteral(remoteFileName)}' and '{folderId}' in parents and trashed = false";
            searchRequest.Fields = "files(id, name)";
            searchRequest.PageSize = 1;
            searchRequest.SupportsAllDrives = true;
            searchRequest.IncludeItemsFromAllDrives = true;

            var searchResult = await searchRequest.ExecuteAsync(ct).ConfigureAwait(false);
            string? existingFileId = (searchResult.Files != null && searchResult.Files.Count > 0)
                ? searchResult.Files[0].Id
                : null;

            await using var fileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);

            if (!string.IsNullOrWhiteSpace(existingFileId))
            {
                var updateMetadata = new Google.Apis.Drive.v3.Data.File { Name = remoteFileName };
                var updateRequest = service.Files.Update(updateMetadata, existingFileId, fileStream, "application/octet-stream");
                updateRequest.Fields = "id";
                updateRequest.SupportsAllDrives = true;
                await updateRequest.UploadAsync(ct).ConfigureAwait(false);
            }
            else
            {
                var createMetadata = new Google.Apis.Drive.v3.Data.File
                {
                    Name = remoteFileName,
                    Parents = new List<string> { folderId }
                };

                var createRequest = service.Files.Create(createMetadata, fileStream, "application/octet-stream");
                createRequest.Fields = "id";
                createRequest.SupportsAllDrives = true;
                await createRequest.UploadAsync(ct).ConfigureAwait(false);
            }
        }

        // 2) List files (top N)
        public static async Task ListGoogleDriveFilesAsync(DriveService service, int pageSize = 10, CancellationToken ct = default)
        {
            if (service is null) throw new ArgumentNullException(nameof(service));

            var listRequest = service.Files.List();
            listRequest.PageSize = pageSize;
            listRequest.Fields = "nextPageToken, files(id, name, mimeType)";

            var resp = await listRequest.ExecuteAsync(ct).ConfigureAwait(false);
            var files = resp.Files;

            if (files != null && files.Count > 0)
            {
                Console.WriteLine("📂 Files on Drive:");
                foreach (var f in files)
                    Console.WriteLine($"- {f.Name} | id={f.Id} | {f.MimeType}");
            }
            else
            {
                Console.WriteLine("📂 No files found.");
            }
        }

        // 3) Get latest by name (optionally in folder)
        public static async Task<Google.Apis.Drive.v3.Data.File?> GetLatestDriveFileByNameAsync(
            DriveService service,
            string fileName,
            string? folderId = null,
            CancellationToken ct = default)
        {
            if (service is null) throw new ArgumentNullException(nameof(service));
            if (string.IsNullOrWhiteSpace(fileName)) throw new ArgumentException("fileName is required", nameof(fileName));

            string safeName = EscapeDriveQueryLiteral(fileName);

            string q = $"name = '{safeName}' and trashed = false";
            if (!string.IsNullOrWhiteSpace(folderId))
                q += $" and '{folderId}' in parents";

            var req = service.Files.List();
            req.Q = q;
            req.Fields = "files(id,name,modifiedTime,size)";
            req.PageSize = 10;
            req.SupportsAllDrives = true;
            req.IncludeItemsFromAllDrives = true;

            var resp = await req.ExecuteAsync(ct).ConfigureAwait(false);
            var files = resp.Files;
            if (files == null || files.Count == 0) return null;

            return files.OrderByDescending(f => f.ModifiedTime ?? DateTime.MinValue).First();
        }

        // 4) Gate: Drive up-to-date vs local
        public static async Task<bool> IsDriveUpToDateVsLocalAsync(
            DriveService service,
            string fileName,
            string localPath,
            string? folderId = null,
            CancellationToken ct = default)
        {
            if (!File.Exists(localPath))
                throw new FileNotFoundException("Local file not found", localPath);

            DateTime localUtc = File.GetLastWriteTime(localPath).ToUniversalTime();

            var driveFile = await GetLatestDriveFileByNameAsync(service, fileName, folderId, ct).ConfigureAwait(false);
            if (driveFile == null || !driveFile.ModifiedTime.HasValue)
                return false;

            DateTime driveUtc = driveFile.ModifiedTime.Value;
            return driveUtc >= localUtc;
        }

        // 5) Sync local then upload if needed (MDB)
        public static async Task<string> SyncThenUploadIfNeededAsync(
            string startupPath,
            string configFileName,
            string folderId,
            CancellationToken ct = default)
        {
            string mediaMdb = Path.Combine(startupPath, "media", "data.mdb");
            const string fileName = "data.mdb";

            SyncMdbToMediaAtStartup(startupPath, configFileName);

            if (!File.Exists(mediaMdb))
                return $"❌ Missing after sync: {mediaMdb}";

            var service = GoogleDriveHelper.GetDriveService();
            if (service is null)
                return "❌ Google Drive service is null (auth failed?)";

            bool driveOk;
            try
            {
                driveOk = await IsDriveUpToDateVsLocalAsync(service, fileName, mediaMdb, folderId, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                return $"❌ Drive check error: {ex.Message}";
            }

            if (driveOk)
            {
                var tsLocal = File.GetLastWriteTime(mediaMdb);
                return $"✅ Drive up-to-date (skip upload) | local={tsLocal:yyyy-MM-dd HH:mm:ss}";
            }

            try
            {
                await UploadFileToSpecificFolderAsync(service, mediaMdb, folderId, ct).ConfigureAwait(false);
                var tsLocal = File.GetLastWriteTime(mediaMdb);
                return $"⬆️ Upload done | local={tsLocal:yyyy-MM-dd HH:mm:ss}";
            }
            catch (GoogleApiException ex)
            {
                return $"🚨 Google API Error: {ex.Message}";
            }
            catch (Exception ex)
            {
                return $"❌ Upload error: {ex.Message}";
            }
        }

        // 6) Download file by name from folder
        public static async Task DownloadFileFromGoogleDriveAsync(
            DriveService service,
            string folderId,
            string fileName,
            string savePath,
            CancellationToken ct = default)
        {
            if (service is null) throw new ArgumentNullException(nameof(service));

            var searchRequest = service.Files.List();
            searchRequest.Q = $"name = '{EscapeDriveQueryLiteral(fileName)}' and '{folderId}' in parents and trashed = false";
            searchRequest.Fields = "files(id, name)";
            searchRequest.PageSize = 1;
            searchRequest.SupportsAllDrives = true;
            searchRequest.IncludeItemsFromAllDrives = true;

            var searchResult = await searchRequest.ExecuteAsync(ct).ConfigureAwait(false);

            if (searchResult.Files == null || searchResult.Files.Count == 0)
                throw new FileNotFoundException($"Drive file not found: {fileName} in folder {folderId}");

            string fileId = searchResult.Files[0].Id;

            var getReq = service.Files.Get(fileId);

            Directory.CreateDirectory(Path.GetDirectoryName(savePath) ?? ".");
            await using var fs = new FileStream(savePath, FileMode.Create, FileAccess.Write, FileShare.None);
            await getReq.DownloadAsync(fs, ct).ConfigureAwait(false);
        }

        // 7) Download then swap local DB (atomic replace) — ใช้ตามเดิมของคุณ
        public static async Task DownloadAndSwapDatabaseAsync(
            DriveService service,
            string folderId,
            string remoteFileName,
            string baseDir,
            CancellationToken ct = default)
        {
            if (service is null) throw new ArgumentNullException(nameof(service));
            if (string.IsNullOrWhiteSpace(baseDir)) baseDir = AppContext.BaseDirectory;

            string targetDb = Path.Combine(baseDir, "data.db");
            string tempDownload = Path.Combine(baseDir, "data_ghost.db.tmp");
            string backupDb = Path.Combine(baseDir, "data.db.bak");

            try
            {
                var searchRequest = service.Files.List();
                searchRequest.Q = $"name = '{EscapeDriveQueryLiteral(remoteFileName)}' and '{folderId}' in parents and trashed = false";
                searchRequest.Fields = "files(id, name)";
                searchRequest.PageSize = 1;
                searchRequest.SupportsAllDrives = true;
                searchRequest.IncludeItemsFromAllDrives = true;

                var searchResult = await searchRequest.ExecuteAsync(ct).ConfigureAwait(false);
                if (searchResult.Files == null || searchResult.Files.Count == 0)
                    throw new FileNotFoundException($"Drive file not found: {remoteFileName} in folder {folderId}");

                string fileId = searchResult.Files[0].Id;

                if (File.Exists(tempDownload)) File.Delete(tempDownload);
                var getReq = service.Files.Get(fileId);

                await using (var fs = new FileStream(tempDownload, FileMode.Create, FileAccess.Write, FileShare.None))
                {
                    await getReq.DownloadAsync(fs, ct).ConfigureAwait(false);
                }

                if (File.Exists(targetDb))
                {
                    try { File.Copy(targetDb, backupDb, true); } catch { }
                }

                if (File.Exists(targetDb))
                    File.Replace(tempDownload, targetDb, backupDb);
                else
                    File.Move(tempDownload, targetDb);

                Console.WriteLine($"✅ Updated DB from cloud: {targetDb}");
            }
            finally
            {
                try { if (File.Exists(tempDownload)) File.Delete(tempDownload); } catch { }
            }
        }

        // 8) Replace with retry
        public static bool ReplaceFileWithRetry(
            string tempFile,
            string targetFile,
            string backupFile,
            out string? lastError,
            int maxRetry = 3,
            int waitSeconds = 30)
        {
            lastError = null;

            for (int attempt = 1; attempt <= maxRetry; attempt++)
            {
                try
                {
                    if (File.Exists(targetFile))
                    {
                        try { File.Copy(targetFile, backupFile, true); } catch { }
                        File.Replace(tempFile, targetFile, backupFile, ignoreMetadataErrors: true);
                    }
                    else
                    {
                        File.Move(tempFile, targetFile);
                    }

                    return true;
                }
                catch (IOException ex)
                {
                    lastError = ex.Message;
                    if (attempt < maxRetry) Thread.Sleep(waitSeconds * 1000);
                }
                catch (UnauthorizedAccessException ex)
                {
                    lastError = ex.Message;
                    if (attempt < maxRetry) Thread.Sleep(waitSeconds * 1000);
                }
            }

            return false;
        }

        private static string EscapeDriveQueryLiteral(string s) => s.Replace("'", "''");

        // PLACEHOLDER — คุณมีของจริงอยู่แล้ว
        private static void SyncMdbToMediaAtStartup(string startupPath, string configFileName)
        {
            // TODO: ใช้ของเดิมคุณ
        }
    }

    // =========================================================
    // Google Drive Auth Helper (CLI)
    // =========================================================
    internal static class GoogleDriveHelper
    {
        private static readonly string[] Scopes = { DriveService.Scope.Drive };
        private const string ApplicationName = "Uroboros";
        private static DriveService? service;

        public static DriveService? GetDriveService()
        {
            if (service != null) return service;

            try
            {
                var credentialPath = "credentials.json";
                if (!File.Exists(credentialPath))
                    throw new FileNotFoundException("Missing credentials.json", credentialPath);

                UserCredential credential;
                using (var stream = new FileStream(credentialPath, FileMode.Open, FileAccess.Read))
                {
                    // NOTE: FileDataStore ใช้เป็น folder name (ตามที่คุณเคยใช้ token.json)
                    var tokenDir = "token.json";

                    credential = GoogleWebAuthorizationBroker.AuthorizeAsync(
                        GoogleClientSecrets.FromStream(stream).Secrets,
                        Scopes,
                        "user",
                        CancellationToken.None,
                        new FileDataStore(tokenDir, true)
                    ).GetAwaiter().GetResult();
                }

                service = new DriveService(new BaseClientService.Initializer
                {
                    HttpClientInitializer = credential,
                    ApplicationName = ApplicationName
                });

                return service;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[Drive] init failed: {ex.Message}");
                return null;
            }
        }

        public static DriveService GetDriveServiceOrThrow()
        {
            var svc = GetDriveService();
            if (svc is null) throw new InvalidOperationException("Google Drive service init failed (null).");
            return svc;
        }
    }

    // =========================================================
    // DB Upload Module (snapshot + upload) — COPY/PASTE
    // =========================================================
    public static class DbUploadModule
    {
        // Validate source
        public static bool ValidateSourceDb(string srcDbPath, out string? error)
        {
            error = null;

            if (string.IsNullOrWhiteSpace(srcDbPath))
            {
                error = "srcDbPath is empty";
                return false;
            }

            if (!File.Exists(srcDbPath))
            {
                error = $"missing source db: {srcDbPath}";
                return false;
            }

            return true;
        }

        // ✅ Create snapshot temp (ไม่ชน data_ghost.db ที่โดน lock)
        // FLOW ยังเหมือนเดิม: รับ snapshotPath = ...\data_ghost.db (ตามที่คุณสั่ง)
        // แต่ไฟล์ snapshot ที่สร้างจริงจะเป็น snapshotPath + ".upload.tmp"
        public static async Task CreateSnapshotAsync(
            string srcDbPath,
            string snapshotPath,
            EngineContext ctx,
            CancellationToken ct)
        {
            if (string.IsNullOrWhiteSpace(srcDbPath))
                throw new ArgumentException("srcDbPath is empty", nameof(srcDbPath));
            if (string.IsNullOrWhiteSpace(snapshotPath))
                throw new ArgumentException("snapshotPath is empty", nameof(snapshotPath));
            if (!File.Exists(srcDbPath))
                throw new FileNotFoundException("Source db not found", srcDbPath);

            var uploadTemp = snapshotPath + ".upload.tmp";

            TryDelete(uploadTemp);

            var dir = Path.GetDirectoryName(uploadTemp);
            if (!string.IsNullOrWhiteSpace(dir))
                Directory.CreateDirectory(dir);

            // best-effort: clear pools เผื่อมี handle ค้าง
            try { SqliteConnection.ClearAllPools(); } catch { }

            try
            {
                var srcCs = new SqliteConnectionStringBuilder
                {
                    DataSource = srcDbPath,
                    Mode = SqliteOpenMode.ReadWrite,
                    Cache = SqliteCacheMode.Shared,
                    Pooling = false
                }.ToString();

                var dstCs = new SqliteConnectionStringBuilder
                {
                    DataSource = uploadTemp,
                    Mode = SqliteOpenMode.ReadWriteCreate,
                    Cache = SqliteCacheMode.Shared,
                    Pooling = false
                }.ToString();

                await using var src = new SqliteConnection(srcCs);
                await using var dst = new SqliteConnection(dstCs);

                await src.OpenAsync(ct).ConfigureAwait(false);

                // optional checkpoint WAL
                try
                {
                    await using var cmd = src.CreateCommand();
                    cmd.CommandText = "PRAGMA wal_checkpoint(FULL);";
                    _ = await cmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    ctx.Log.Warn($"[DB] wal_checkpoint warn: {ex.Message}");
                }

                await dst.OpenAsync(ct).ConfigureAwait(false);

                // ✅ consistent snapshot -> uploadTemp
                src.BackupDatabase(dst);

                await dst.CloseAsync().ConfigureAwait(false);
                await src.CloseAsync().ConfigureAwait(false);

                ctx.Log.Info($"[DB] snapshot temp created: {uploadTemp}");
            }
            catch
            {
                TryDelete(uploadTemp);
                throw;
            }
        }

        public static DriveService GetDriveServiceOrThrow()
            => GoogleDriveHelper.GetDriveServiceOrThrow();

        // ✅ Upload snapshot temp แต่ remote name = data_ghost.db
        public static async Task UploadSnapshotAsync(
            DriveService service,
            string snapshotPath,
            string folderId,
            EngineContext ctx,
            CancellationToken ct)
        {
            var uploadTemp = snapshotPath + ".upload.tmp";
            if (!File.Exists(uploadTemp))
                throw new FileNotFoundException("snapshot temp not found (CreateSnapshotAsync did not produce it)", uploadTemp);

            var remoteName = Path.GetFileName(snapshotPath); // "data_ghost.db"
            await DriveSyncCli.UploadFileToSpecificFolderAsNameAsync(service, uploadTemp, folderId, remoteName, ct)
                .ConfigureAwait(false);

            ctx.Log.Info($"[DB] uploaded '{remoteName}' from '{uploadTemp}'");
        }

        private static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }
    }

    // =========================================================
    // TASK: DB_upload (เรียกแต่ module functions ไม่ยัด logic ใน task)
    // =========================================================
    public sealed class DbUploadTask : IEngineTask
    {
        public TaskSpec Spec { get; } = new TaskSpec(
            Name: "DB_upload",
            Group: "DB",
            Priority: TaskPriority.Normal,
            Policy: RunPolicy.DropIfRunning,
            Timeout: TimeSpan.FromSeconds(120)
        );

        private const string FolderId = "1hLIPn9qgjqm4WliNJGsEwmjq78oaXFgm";

        public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
        {
            var baseDir = AppContext.BaseDirectory;

            // ✅ ตามที่คุณสั่ง ห้ามเปลี่ยน:
            string dbPath = Path.Combine(baseDir, "data.db");
            string snapshotPath = Path.Combine(baseDir, "data_ghost.db");

            if (!DbUploadModule.ValidateSourceDb(dbPath, out var err))
            {
                ctx.Log.Warn($"[DB] {err}");
                return;
            }

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

            try
            {
                await DbUploadModule.CreateSnapshotAsync(dbPath, snapshotPath, ctx, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ctx.Log.Error(ex, $"[DB] snapshot error | file={snapshotPath}");
                return;
            }

            try
            {
                await DbUploadModule.UploadSnapshotAsync(service, snapshotPath, FolderId, ctx, ct).ConfigureAwait(false);
                ctx.Log.Info($"[DB] upload complete | remoteName={Path.GetFileName(snapshotPath)}");
            }
            catch (Exception ex)
            {
                ctx.Log.Error(ex, $"[DB] upload error | file={snapshotPath}");
            }
        }
    }

    public static class DbDownloadModule
    {
        // ✅ MAIN: Download "data_ghost.db" from Drive folder -> swap into local "data.db"
        public static async Task DownloadAndSwapAsync(
            DriveService service,
            string folderId,
            string remoteFileName,
            string baseDir,
            EngineContext ctx,
            CancellationToken ct,
            int maxRetry = 3,
            int waitSeconds = 10)
        {
            if (service is null) throw new ArgumentNullException(nameof(service));
            if (ctx is null) throw new ArgumentNullException(nameof(ctx));
            if (string.IsNullOrWhiteSpace(folderId)) throw new ArgumentException("folderId is required", nameof(folderId));
            if (string.IsNullOrWhiteSpace(remoteFileName)) throw new ArgumentException("remoteFileName is required", nameof(remoteFileName));
            if (string.IsNullOrWhiteSpace(baseDir)) baseDir = AppContext.BaseDirectory;

            string targetDb = Path.Combine(baseDir, "data.db");
            string backupDb = Path.Combine(baseDir, "data.db.bak");
            string tempDownload = Path.Combine(baseDir, $"{remoteFileName}.download.tmp"); // ex: data_ghost.db.download.tmp

            // ensure folder
            Directory.CreateDirectory(baseDir);

            TryDelete(tempDownload);

            // 1) Download to temp
            ctx.Log.Info($"[DB] download start | remote={remoteFileName}");
            await DownloadByNameFromDriveFolderAsync(service, folderId, remoteFileName, tempDownload, ct)
                .ConfigureAwait(false);
            ctx.Log.Info($"[DB] downloaded temp: {tempDownload}");

            // 2) Clear pools (IMPORTANT: Microsoft.Data.Sqlite)
            try { SqliteConnection.ClearAllPools(); } catch { }

            // 3) Swap/rename -> data.db (atomic) with retry (file-lock resistant)
            string? lastErr;
            bool ok = ReplaceFileWithRetry(
                tempFile: tempDownload,
                targetFile: targetDb,
                backupFile: backupDb,
                out lastErr,
                maxRetry: maxRetry,
                waitSeconds: waitSeconds,
                beforeEachAttempt: () =>
                {
                    // best-effort clear again
                    try { SqliteConnection.ClearAllPools(); } catch { }
                });

            if (!ok)
                throw new IOException($"DB swap failed after {maxRetry} tries: {lastErr}");

            ctx.Log.Info($"[DB] download+swap complete | local={targetDb}");
        }

        // ✅ Download a file by name from folder -> savePath (temp)
        // (kept self-contained; consistent with your Upload module query style)
        public static async Task DownloadByNameFromDriveFolderAsync(
            DriveService service,
            string folderId,
            string fileName,
            string savePath,
            CancellationToken ct = default)
        {
            if (service is null) throw new ArgumentNullException(nameof(service));
            if (string.IsNullOrWhiteSpace(folderId)) throw new ArgumentException("folderId is required", nameof(folderId));
            if (string.IsNullOrWhiteSpace(fileName)) throw new ArgumentException("fileName is required", nameof(fileName));

            var search = service.Files.List();
            search.Q = $"name = '{EscapeDriveQueryLiteral(fileName)}' and '{folderId}' in parents and trashed = false";
            search.Fields = "files(id, name, modifiedTime, size)";
            search.PageSize = 1;
            search.SupportsAllDrives = true;
            search.IncludeItemsFromAllDrives = true;

            var result = await search.ExecuteAsync(ct).ConfigureAwait(false);
            if (result.Files is null || result.Files.Count == 0)
                throw new FileNotFoundException($"Drive file not found: {fileName} in folder {folderId}");

            string fileId = result.Files[0].Id;

            Directory.CreateDirectory(Path.GetDirectoryName(savePath) ?? ".");
            var get = service.Files.Get(fileId);

            await using var fs = new FileStream(savePath, FileMode.Create, FileAccess.Write, FileShare.None);
            await get.DownloadAsync(fs, ct).ConfigureAwait(false);
        }

        // ✅ Atomic replace with retries (handles Windows file locks)
        public static bool ReplaceFileWithRetry(
            string tempFile,
            string targetFile,
            string backupFile,
            out string? lastError,
            int maxRetry = 3,
            int waitSeconds = 10,
            Action? beforeEachAttempt = null)
        {
            lastError = null;

            for (int attempt = 1; attempt <= maxRetry; attempt++)
            {
                try
                {
                    beforeEachAttempt?.Invoke();

                    // backup existing
                    if (File.Exists(targetFile))
                    {
                        try { File.Copy(targetFile, backupFile, true); } catch { }
                        File.Replace(tempFile, targetFile, backupFile, ignoreMetadataErrors: true);
                    }
                    else
                    {
                        // first install
                        File.Move(tempFile, targetFile);
                    }

                    return true;
                }
                catch (IOException ex)
                {
                    lastError = ex.Message;
                    if (attempt < maxRetry) ThreadSleepSeconds(waitSeconds);
                }
                catch (UnauthorizedAccessException ex)
                {
                    lastError = ex.Message;
                    if (attempt < maxRetry) ThreadSleepSeconds(waitSeconds);
                }
            }

            return false;
        }

        private static void ThreadSleepSeconds(int seconds)
        {
            if (seconds <= 0) return;
            try { Thread.Sleep(seconds * 1000); } catch { }
        }

        private static void TryDelete(string path)
        {
            try { if (File.Exists(path)) File.Delete(path); } catch { }
        }

        private static string EscapeDriveQueryLiteral(string s) => s.Replace("'", "''");
    }

    // =========================================================
    // TASK: DB_download  (ยึด pattern เดียวกับ DbUploadTask)
    // =========================================================
    public sealed class DbDownloadTask : IEngineTask
    {
        public TaskSpec Spec { get; } = new TaskSpec(
            Name: "DB_download",
            Group: "DB",
            Priority: TaskPriority.Normal,
            Policy: RunPolicy.DropIfRunning,
            Timeout: TimeSpan.FromSeconds(120)
        );

        private const string FolderId = "1hLIPn9qgjqm4WliNJGsEwmjq78oaXFgm";
        private const string RemoteName = "data_ghost.db";

        public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
        {
            var baseDir = AppContext.BaseDirectory;

            DriveService service;
            try
            {
                service = GoogleDriveHelper.GetDriveServiceOrThrow();
            }
            catch (Exception ex)
            {
                ctx.Log.Error(ex, "[DB] drive init error");
                return;
            }

            try
            {
                await DbDownloadModule.DownloadAndSwapAsync(
                    service: service,
                    folderId: FolderId,
                    remoteFileName: RemoteName,
                    baseDir: baseDir,
                    ctx: ctx,
                    ct: ct,
                    maxRetry: 3,
                    waitSeconds: 10
                ).ConfigureAwait(false);
            }
            catch (GoogleApiException ex)
            {
                ctx.Log.Error(ex, "[DB] Google API error (download)");
            }
            catch (Exception ex)
            {
                ctx.Log.Error(ex, "[DB] download/swap error");
            }
            finally
            {
                // ctx.Gate?.StartAll(); // resume if you froze
            }
        }
    }
}
