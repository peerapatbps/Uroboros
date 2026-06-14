using System;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

/// <summary>
/// ส่ง HTTP trigger ไปยัง Openclaw เพื่อให้ Analyst agent ตรวจสอบ plant data
/// </summary>
public sealed class OpenClawTriggerTask : IEngineTask
{
    private static readonly HttpClient _http = new HttpClient
    {
        Timeout = TimeSpan.FromSeconds(25)
    };

    private const string OpenClawUrl = "http://127.0.0.1:19789/api/trigger";

    public TaskSpec Spec { get; } = new TaskSpec(
        Name: "openclaw.trigger",
        Group: "OPENCLAW",
        Priority: TaskPriority.Normal,
        Policy: RunPolicy.DropIfRunning,
        Timeout: TimeSpan.FromSeconds(30)
    );

    public async Task ExecuteAsync(EngineContext ctx, CancellationToken ct)
    {
        ctx.Log.Info("[OPENCLAW] plant data check trigger starting");

        var payload = JsonSerializer.Serialize(new
        {
            source = "uroboros",
            check = "plant_data",
            subsystems = new[] { "tps", "dps", "rws", "chem" },
            triggeredAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        });

        var content = new StringContent(payload, Encoding.UTF8, "application/json");

        try
        {
            var resp = await _http.PostAsync(OpenClawUrl, content, ct);
            ctx.Log.Info($"[OPENCLAW] trigger sent → {(int)resp.StatusCode} {resp.StatusCode}");
        }
        catch (OperationCanceledException)
        {
            ctx.Log.Warn("[OPENCLAW] trigger canceled");
            throw;
        }
        catch (Exception ex)
        {
            ctx.Log.Error(ex, "[OPENCLAW] trigger failed");
            throw;
        }
    }
}
