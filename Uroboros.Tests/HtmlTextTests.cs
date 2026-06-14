using Uroboros;
using Xunit;

public sealed class HtmlTextTests
{
    [Fact]
    public void StripTags_Null_ReturnsEmpty()
    {
        Assert.Equal("", HtmlText.StripTags(null));
    }

    [Fact]
    public void StripTags_EmptyString_ReturnsEmpty()
    {
        Assert.Equal("", HtmlText.StripTags(""));
    }

    [Fact]
    public void StripTags_PlainText_Unchanged()
    {
        Assert.Equal("hello world", HtmlText.StripTags("hello world"));
    }

    [Fact]
    public void StripTags_Bold_Tag_Removed()
    {
        Assert.Equal("hello", HtmlText.StripTags("<b>hello</b>"));
    }

    [Fact]
    public void StripTags_Font_Tag_With_Attrs_Removed()
    {
        Assert.Equal("42.5", HtmlText.StripTags("<font color=\"red\">42.5</font>"));
    }

    [Fact]
    public void StripTags_SelfClosing_Tag_Removed()
    {
        Assert.Equal("a  b", HtmlText.StripTags("a <br/> b"));
    }

    [Fact]
    public void StripTags_HtmlEntities_Decoded()
    {
        Assert.Equal("a & b", HtmlText.StripTags("a &amp; b"));
    }

    [Fact]
    public void StripTags_LtGt_Entities_Decoded()
    {
        Assert.Equal("a < b > c", HtmlText.StripTags("a &lt; b &gt; c"));
    }

    [Fact]
    public void StripTags_Trims_Whitespace()
    {
        Assert.Equal("trimmed", HtmlText.StripTags("  trimmed  "));
    }

    [Fact]
    public void StripTags_WhitespaceOnly_ReturnsEmpty()
    {
        Assert.Equal("", HtmlText.StripTags("   "));
    }

    [Fact]
    public void StripTags_MultilineTags_Stripped()
    {
        var html = "<td align=\"center\">\n  <b>Channel Name</b>\n</td>";
        Assert.Contains("Channel Name", HtmlText.StripTags(html));
        Assert.DoesNotContain("<", HtmlText.StripTags(html));
    }

    [Fact]
    public void StripTags_NestedTags_AllStripped()
    {
        var result = HtmlText.StripTags("<div><span>text</span></div>");
        Assert.Equal("text", result);
    }
}
