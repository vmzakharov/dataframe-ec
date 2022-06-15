package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.DateTimeValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateScriptWithContext;
import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateToString;

public class BuiltInFormatterTest
{
    @Test
    public void dateFormats()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("date", new DateValue(LocalDate.of(2022, 11, 25)));
        context.setVariable("dateTime", new DateTimeValue(LocalDateTime.of(2022, 11, 25, 23, 45, 12)));

        Assert.assertEquals("221125", evaluateScriptWithContext("format(date, 'yyMMdd')", context).stringValue());

        Assert.assertEquals("221125", evaluateScriptWithContext("format(dateTime, 'yyMMdd')", context).stringValue());

        Assert.assertEquals("221125234512", evaluateScriptWithContext("format(dateTime, 'yyMMddHHmmss')", context).stringValue());
    }

    @Test
    public void numberFormats()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("whole", new LongValue(12_345));
        context.setVariable("negWhole", new LongValue(-12_345));
        context.setVariable("decimal", new DoubleValue(12345.456));
        context.setVariable("negDecimal", new DoubleValue(-12345.456));
        context.setVariable("formatWithDecimal", new StringValue("#,##0.0##;(#,##0.0##)"));
        context.setVariable("formatWhole", new StringValue("#,##0;(#,##0)"));

        Assert.assertEquals("12,345.0", evaluateToString("format(12345, '#,##0.0#;(#)')"));
        Assert.assertEquals("(12,345.0)", evaluateToString("format(-12345, '#,##0.0#;(#,##0.0#)')"));
        Assert.assertEquals("0", evaluateToString("format(0, '#,##0;(#,##0)')"));
        Assert.assertEquals("12,345", evaluateToString("format(12345, '#,##0;(#,##0)')"));
        Assert.assertEquals("(12,345)", evaluateToString("format(-12345, '#,##0;(#,##0)')"));

        Assert.assertEquals("12,345.456", evaluateScriptWithContext("format(decimal, formatWithDecimal)", context).stringValue());
        Assert.assertEquals("(12,345.456)", evaluateScriptWithContext("format(negDecimal, formatWithDecimal)", context).stringValue());
        Assert.assertEquals("0.0", evaluateScriptWithContext("format(0.0, formatWithDecimal)", context).stringValue());
        Assert.assertEquals("0", evaluateScriptWithContext("format(0.0, formatWhole)", context).stringValue());
        Assert.assertEquals("12,345", evaluateScriptWithContext("format(decimal, formatWhole)", context).stringValue());
        Assert.assertEquals("(12,345)", evaluateScriptWithContext("format(negDecimal, formatWhole)", context).stringValue());
    }

    @Test(expected = RuntimeException.class)
    public void attemptToFormatString()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", new StringValue("abc"));

        evaluateScriptWithContext("format(x, 'yyMMdd')", context);
    }
}
