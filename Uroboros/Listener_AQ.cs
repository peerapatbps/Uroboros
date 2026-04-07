// AqApiModule.cs
// ==============================
// AQFast API Module (verify/process) — C# / .NET 10 CLI
// NO chromelock, NO WinForms, NO polling
//
// Usage pattern (WebListener):
//   if POST /api/verify  => await AqApiModule.HandleVerifyAsync(hc, _ctx, ct);
//   if POST /api/process => await AqApiModule.HandleProcessAsync(hc, _ctx, _aq, ct);
//
// You provide IAqFastApi (or adapter) that does the real work.
// ==============================

#nullable enable
using System;
using System.IO;
using System.Net;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
namespace Uroboros
{
    public static class AqApiModule
    {
        private static readonly JsonSerializerOptions JsonOpt = new(JsonSerializerDefaults.Web);

        // ------------------------------
        // Public contracts
        // ------------------------------
        public sealed record VerifyRequest(string? Username, string? Token);

        public sealed record ProcessRequest(
            string Begin,
            string End,
            string Token,
            bool RemoveOddHour,
            string Opt,
            string CsvFileName,
            string ExternalConfigsJson, // raw JSON array string (เหมือน VB: prop.GetRawText())
            string User
        );

        public sealed record ProcessResult(
            string FileName,
            byte[]? Bytes = null,
            string? FilePath = null,         // optional: if you choose to write file
            bool DeleteAfterSend = false,    // if FilePath is used, allow cleanup
            string ContentType = "text/csv; charset=utf-8"
        );

        // Adapter interface (คุณ implement เองให้เรียก AqFast ที่ port มา)
        public interface IAqFastApi
        {
            Task<ProcessResult> ProcessAsync(ProcessRequest req, CancellationToken ct);
            // (optional) verify token จริง
            Task<bool> VerifyAsync(VerifyRequest req, CancellationToken ct);
        }

        // ------------------------------
        // /api/verify
        // ------------------------------
        public static async Task HandleVerifyAsync(HttpListenerContext hc, EngineContext ctx, IAqFastApi aq, CancellationToken ct)
        {
            try
            {
                var req = hc.Request;
                var body = await ReadBodyAsync(req, ct).ConfigureAwait(false);
                ctx.Log.Info("[API VERIFY] body=" + body);

                // tolerant parse like VB
                string username = "";
                string token = "";

                try
                {
                    using var doc = JsonDocument.Parse(body);
                    var root = doc.RootElement;

                    if (root.TryGetProperty("username", out var p1) && p1.ValueKind == JsonValueKind.String)
                        username = p1.GetString() ?? "";

                    if (root.TryGetProperty("token", out var p2) && p2.ValueKind == JsonValueKind.String)
                        token = p2.GetString() ?? "";
                }
                catch (Exception exJson)
                {
                    ctx.Log.Warn("[API VERIFY] JSON parse error: " + exJson.Message);
                }

                if (string.IsNullOrWhiteSpace(token))
                {
                    await WritePlainAsync(hc, 400, "token missing or empty").ConfigureAwait(false);
                    return;
                }

                bool ok;
                try
                {
                    ok = await aq.VerifyAsync(new VerifyRequest(username, token), ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    ctx.Log.Error(ex, "[API VERIFY] verify backend error");
                    await WritePlainAsync(hc, 500, "server error: verify backend failed").ConfigureAwait(false);
                    return;
                }

                if (!ok)
                {
                    await WritePlainAsync(hc, 401, "invalid token").ConfigureAwait(false);
                    return;
                }

                await WritePlainAsync(hc, 200, "รับทราบ").ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                ctx.Log.Error(ex, "[API VERIFY] error");
                try { await WritePlainAsync(hc, 500, "server error: " + ex.Message).ConfigureAwait(false); } catch { }
            }
        }

        // ------------------------------
        // /api/process
        // ------------------------------
        public static async Task HandleProcessAsync(HttpListenerContext hc, EngineContext ctx, IAqFastApi aq, CancellationToken ct)
        {
            try
            {
                var req = hc.Request;
                var body = await ReadBodyAsync(req, ct).ConfigureAwait(false);

                string beginStr = "";
                string endStr = "";
                string tokenStr = "";
                string opt = "";
                string csvFilename = "output.csv";
                bool removeOddHour = false;
                string externalJson = "";
                string user = "";

                // strict parse like VB HandleProcess (parse fail => 400)
                try
                {
                    using var doc = JsonDocument.Parse(body);
                    var root = doc.RootElement;

                    if (root.TryGetProperty("begin", out var p) && p.ValueKind == JsonValueKind.String)
                        beginStr = p.GetString() ?? "";

                    if (root.TryGetProperty("end", out p) && p.ValueKind == JsonValueKind.String)
                        endStr = p.GetString() ?? "";

                    if (root.TryGetProperty("token", out p) && p.ValueKind == JsonValueKind.String)
                        tokenStr = p.GetString() ?? "";

                    if (root.TryGetProperty("removeOddHour", out p) &&
                        (p.ValueKind == JsonValueKind.True || p.ValueKind == JsonValueKind.False))
                        removeOddHour = p.GetBoolean();

                    if (root.TryGetProperty("opt", out p) && p.ValueKind == JsonValueKind.String)
                        opt = p.GetString() ?? "";

                    if (root.TryGetProperty("csvFilename", out p) && p.ValueKind == JsonValueKind.String)
                        csvFilename = p.GetString() ?? "output.csv";

                    if (root.TryGetProperty("configs", out p) && p.ValueKind == JsonValueKind.Array)
                        externalJson = p.GetRawText(); // raw array JSON

                    if (root.TryGetProperty("user", out p) && p.ValueKind == JsonValueKind.String)
                        user = p.GetString() ?? "";
                }
                catch (Exception exJson)
                {
                    ctx.Log.Warn("[API PROCESS] JSON parse error: " + exJson.Message);
                    await WritePlainAsync(hc, 400, "invalid json payload").ConfigureAwait(false);
                    return;
                }

                ctx.Log.Info($"[AQ API PROCESS] User={user} Request at {DateTime.Now:yyyy-MM-dd HH:mm:ss}");

                if (string.IsNullOrWhiteSpace(beginStr) ||
                    string.IsNullOrWhiteSpace(endStr) ||
                    string.IsNullOrWhiteSpace(externalJson))
                {
                    await WritePlainAsync(hc, 400, "missing begin/end/configs").ConfigureAwait(false);
                    return;
                }

                // call backend (AQFast port)
                ProcessResult result;
                try
                {
                    var pr = new ProcessRequest(
                        Begin: beginStr,
                        End: endStr,
                        Token: tokenStr,
                        RemoveOddHour: removeOddHour,
                        Opt: opt,
                        CsvFileName: string.IsNullOrWhiteSpace(csvFilename) ? "output.csv" : csvFilename,
                        ExternalConfigsJson: externalJson,
                        User: user
                    );

                    result = await aq.ProcessAsync(pr, ct).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    ctx.Log.Error(ex, "[API PROCESS] backend error");
                    await WritePlainAsync(hc, 500, "server error (/api/process): " + ex.Message).ConfigureAwait(false);
                    return;
                }

                // materialize bytes (bytes preferred; file fallback supported)
                byte[] bytes;
                if (result.Bytes is { Length: > 0 })
                {
                    bytes = result.Bytes;
                }
                else if (!string.IsNullOrWhiteSpace(result.FilePath) && File.Exists(result.FilePath))
                {
                    bytes = await File.ReadAllBytesAsync(result.FilePath, ct).ConfigureAwait(false);
                }
                else
                {
                    await WritePlainAsync(hc, 500, "csv not ready (no bytes/file)").ConfigureAwait(false);
                    return;
                }

                // write CSV attachment (เหมือน VB)
                await WriteCsvAsync(hc, 200, bytes, result.FileName, result.ContentType).ConfigureAwait(false);

                // cleanup optional
                if (result.DeleteAfterSend && !string.IsNullOrWhiteSpace(result.FilePath))
                {
                    try
                    {
                        var lower = (result.FileName ?? "").Trim().ToLowerInvariant();
                        if (lower != "output.csv" && File.Exists(result.FilePath))
                            File.Delete(result.FilePath);
                    }
                    catch (Exception ex)
                    {
                        ctx.Log.Warn("Warning: CSV delete failed -> " + ex.Message);
                    }
                }
            }
            catch (Exception ex)
            {
                ctx.Log.Error(ex, "[API PROCESS] error");
                try { await WritePlainAsync(hc, 500, "server error (/api/process): " + ex.Message).ConfigureAwait(false); } catch { }
            }
        }

        // ------------------------------
        // Common IO helpers
        // ------------------------------
        private static async Task<string> ReadBodyAsync(HttpListenerRequest req, CancellationToken ct)
        {
            if (!req.HasEntityBody) return "{}";
            using var sr = new StreamReader(req.InputStream, req.ContentEncoding ?? Encoding.UTF8);
            var s = await sr.ReadToEndAsync(ct).ConfigureAwait(false);
            return string.IsNullOrWhiteSpace(s) ? "{}" : s;
        }

        private static void ApplyCors(HttpListenerResponse res)
        {
            res.Headers["Access-Control-Allow-Origin"] = "*";
            res.Headers["Access-Control-Allow-Methods"] = "GET,POST,OPTIONS";
            res.Headers["Access-Control-Allow-Headers"] = "Content-Type";
        }

        private static async Task WritePlainAsync(HttpListenerContext hc, int statusCode, string text)
        {
            var res = hc.Response;
            ApplyCors(res);
            res.StatusCode = statusCode;
            res.ContentType = "text/plain; charset=utf-8";

            var bytes = Encoding.UTF8.GetBytes(text ?? "");
            res.ContentLength64 = bytes.Length;
            await res.OutputStream.WriteAsync(bytes, 0, bytes.Length).ConfigureAwait(false);
            res.OutputStream.Close();
        }

        private static async Task WriteCsvAsync(HttpListenerContext hc, int statusCode, byte[] fileBytes, string fileName, string contentType)
        {
            var res = hc.Response;
            ApplyCors(res);

            res.StatusCode = statusCode;
            res.ContentType = string.IsNullOrWhiteSpace(contentType) ? "text/csv; charset=utf-8" : contentType;
            res.Headers["Content-Disposition"] = $"attachment; filename=\"{(string.IsNullOrWhiteSpace(fileName) ? "output.csv" : fileName)}\"";

            res.ContentLength64 = fileBytes.LongLength;
            await res.OutputStream.WriteAsync(fileBytes, 0, fileBytes.Length).ConfigureAwait(false);
            res.OutputStream.Flush();
            res.OutputStream.Close();
        }
    }

    public sealed class AqFastApiReal : AqApiModule.IAqFastApi
    {
        public Task<bool> VerifyAsync(AqApiModule.VerifyRequest req, CancellationToken ct)
        {
            // เบื้องต้น: token ไม่ว่าง = ผ่าน (คุณจะไปทำ verify จริงทีหลังได้)
            var ok = !string.IsNullOrWhiteSpace(req.Token);
            return Task.FromResult(ok);
        }

        public async Task<AqApiModule.ProcessResult> ProcessAsync(AqApiModule.ProcessRequest req, CancellationToken ct)
        {
            // 1) สร้าง AQFast query (CLI)
            var aq = new AquadatFastCli.AquadatFastQuery();

            // 2) เตรียม output path (เขียนไฟล์ครั้งเดียว แล้วอ่าน bytes กลับทันที)
            var baseDir = AppContext.BaseDirectory;
            var mediaDir = Path.Combine(baseDir, "media");
            Directory.CreateDirectory(mediaDir);

            var fileName = string.IsNullOrWhiteSpace(req.CsvFileName) ? "output.csv" : req.CsvFileName.Trim();
            var outPath = Path.Combine(mediaDir, fileName);

            // กันไฟล์ค้าง
            try { if (File.Exists(outPath)) File.Delete(outPath); } catch { }

            // 3) Build MultiOutputRequest (key=MAIN)
            var mreq = new AquadatFastCli.MultiOutputRequest
            {
                BeginDtString = req.Begin,
                EndDtString = req.End,
                RemoveOddHour = req.RemoveOddHour,
                Token = req.Token ?? "",
                ForcePlantText = "", // ถ้าต้องการบังคับ plant เช่น "MS" ค่อยใส่ภายหลัง
                Mode = AquadatFastCli.RunMode.ExportCsvOnly,
                IncludeDateTimeColumn = true
            };

            // mapping configs จาก payload (raw JSON array string)
            mreq.ExternalJsonByKey["MAIN"] = req.ExternalConfigsJson;

            // บังคับชื่อไฟล์ที่ output
            mreq.CsvFileByKey["MAIN"] = outPath;

            // 4) Run (NO polling)
            await aq.ProcessMultiAsync(mreq, ct).ConfigureAwait(false);

            // 5) Read bytes แล้วส่งกลับ
            if (!File.Exists(outPath))
                throw new InvalidOperationException("CSV not produced: " + outPath);

            var bytes = await File.ReadAllBytesAsync(outPath, ct).ConfigureAwait(false);

            // 6) ลบไฟล์หลังอ่าน (ถ้าไม่ใช่ output.csv)
            //    หมายเหตุ: คุณจะเลือก "ไม่ลบ" ก็ได้ แค่คอมเมนต์ส่วนนี้
            try
            {
                if (!string.Equals(fileName, "output.csv", StringComparison.OrdinalIgnoreCase))
                    File.Delete(outPath);
            }
            catch { }

            return new AqApiModule.ProcessResult(
                FileName: fileName,
                Bytes: bytes,
                ContentType: "text/csv; charset=utf-8"
            );
        }
    }
}