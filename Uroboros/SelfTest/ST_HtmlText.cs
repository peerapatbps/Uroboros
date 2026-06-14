namespace Uroboros.SelfTest;

/// <summary>Self-tests for <see cref="HtmlText.StripTags"/>.</summary>
internal static class ST_HtmlText
{
    public static IEnumerable<SelfTestCase> Run()
    {
        yield return SelfTestRunner.Run("HtmlText.StripTags | null → empty", () =>
            SelfTestRunner.AssertEqual("", HtmlText.StripTags(null)));

        yield return SelfTestRunner.Run("HtmlText.StripTags | empty string → empty", () =>
            SelfTestRunner.AssertEqual("", HtmlText.StripTags("")));

        yield return SelfTestRunner.Run("HtmlText.StripTags | plain text unchanged", () =>
            SelfTestRunner.AssertEqual("hello world", HtmlText.StripTags("hello world")));

        yield return SelfTestRunner.Run("HtmlText.StripTags | <b> tag removed", () =>
            SelfTestRunner.AssertEqual("hello", HtmlText.StripTags("<b>hello</b>")));

        yield return SelfTestRunner.Run("HtmlText.StripTags | <font> with attrs removed", () =>
            SelfTestRunner.AssertEqual("42.5", HtmlText.StripTags("<font color=\"red\">42.5</font>")));

        yield return SelfTestRunner.Run("HtmlText.StripTags | self-closing tag removed", () =>
            SelfTestRunner.AssertEqual("a  b", HtmlText.StripTags("a <br/> b")));

        yield return SelfTestRunner.Run("HtmlText.StripTags | &amp; decoded", () =>
            SelfTestRunner.AssertEqual("a & b", HtmlText.StripTags("a &amp; b")));

        yield return SelfTestRunner.Run("HtmlText.StripTags | &lt; &gt; decoded", () =>
            SelfTestRunner.AssertEqual("a < b > c", HtmlText.StripTags("a &lt; b &gt; c")));

        yield return SelfTestRunner.Run("HtmlText.StripTags | trims surrounding whitespace", () =>
            SelfTestRunner.AssertEqual("trimmed", HtmlText.StripTags("  trimmed  ")));

        yield return SelfTestRunner.Run("HtmlText.StripTags | whitespace-only → empty", () =>
            SelfTestRunner.AssertEqual("", HtmlText.StripTags("   ")));

        yield return SelfTestRunner.Run("HtmlText.StripTags | multiline td stripped", () =>
        {
            var html   = "<td align=\"center\">\n  <b>Channel Name</b>\n</td>";
            var result = HtmlText.StripTags(html);
            SelfTestRunner.AssertContains(result, "Channel Name");
            SelfTestRunner.AssertNotContains(result, "<");
        });

        yield return SelfTestRunner.Run("HtmlText.StripTags | nested tags all stripped", () =>
            SelfTestRunner.AssertEqual("text", HtmlText.StripTags("<div><span>text</span></div>")));
    }
}
