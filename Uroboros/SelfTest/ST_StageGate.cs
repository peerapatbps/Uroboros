namespace Uroboros.SelfTest;

/// <summary>Self-tests for <see cref="StageGate"/>.</summary>
internal static class ST_StageGate
{
    public static IEnumerable<SelfTestCase> Run()
    {
        yield return SelfTestRunner.Run("StageGate | default IsPaused = false", () =>
        {
            var gate = new StageGate(startPaused: false);
            SelfTestRunner.AssertEqual(false, gate.IsPaused);
        });

        yield return SelfTestRunner.Run("StageGate | startPaused = true → IsPaused = true", () =>
        {
            var gate = new StageGate(startPaused: true);
            SelfTestRunner.AssertEqual(true, gate.IsPaused);
        });

        yield return SelfTestRunner.Run("StageGate | Pause sets IsPaused", () =>
        {
            var gate = new StageGate(false);
            gate.Pause();
            SelfTestRunner.AssertEqual(true, gate.IsPaused);
        });

        yield return SelfTestRunner.Run("StageGate | Pause then Resume clears IsPaused", () =>
        {
            var gate = new StageGate(false);
            gate.Pause();
            gate.Resume();
            SelfTestRunner.AssertEqual(false, gate.IsPaused);
        });

        yield return SelfTestRunner.Run("StageGate | Resume on unpaused stays false", () =>
        {
            var gate = new StageGate(false);
            gate.Resume(); // no-op
            SelfTestRunner.AssertEqual(false, gate.IsPaused);
        });

        yield return SelfTestRunner.Run("StageGate | Pause is idempotent", () =>
        {
            var gate = new StageGate(false);
            gate.Pause();
            gate.Pause();
            SelfTestRunner.AssertEqual(true, gate.IsPaused);
        });
    }
}
