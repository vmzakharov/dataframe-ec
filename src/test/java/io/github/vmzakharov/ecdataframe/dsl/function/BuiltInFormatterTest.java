package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.DateTimeValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.FloatValue;
import io.github.vmzakharov.ecdataframe.dsl.value.IntValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;

import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateScriptWithContext;
import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateToString;
import static org.junit.jupiter.api.Assertions.*;

public class BuiltInFormatterTest
{
    @Test
    public void dateFormats()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("date", new DateValue(LocalDate.of(2022, 11, 25)));
        context.setVariable("dateTime", new DateTimeValue(LocalDateTime.of(2022, 11, 25, 23, 45, 12)));

        assertEquals("221125", evaluateScriptWithContext("format(date, 'yyMMdd')", context).stringValue());

        assertEquals("221125", evaluateScriptWithContext("format(dateTime, 'yyMMdd')", context).stringValue());

        assertEquals("221125234512", evaluateScriptWithContext("format(dateTime, 'yyMMddHHmmss')", context).stringValue());
    }

    @Test
    public void numberFormats()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("whole", new LongValue(12_345));
        context.setVariable("wholeInt", new IntValue(2_345));
        context.setVariable("negWhole", new LongValue(-12_345));
        context.setVariable("decimal", new DoubleValue(12345.456));
        context.setVariable("decimalFloat", new FloatValue(345.456f));
        context.setVariable("negDecimal", new DoubleValue(-12345.456));
        context.setVariable("formatWithDecimal", new StringValue("#,##0.0##;(#,##0.0##)"));
        context.setVariable("formatWhole", new StringValue("#,##0;(#,##0)"));

        assertEquals("12,345.0", evaluateToString("format(12345, '#,##0.0#;(#)')"));
        assertEquals("(12,345.0)", evaluateToString("format(-12345, '#,##0.0#;(#,##0.0#)')"));
        assertEquals("0", evaluateToString("format(0, '#,##0;(#,##0)')"));
        assertEquals("12,345", evaluateToString("format(12345, '#,##0;(#,##0)')"));
        assertEquals("(12,345)", evaluateToString("format(-12345, '#,##0;(#,##0)')"));

        assertEquals("12,345.456", evaluateScriptWithContext("format(decimal, formatWithDecimal)", context).stringValue());
        assertEquals("(12,345.456)", evaluateScriptWithContext("format(negDecimal, formatWithDecimal)", context).stringValue());

        assertEquals("0.0", evaluateScriptWithContext("format(0.0, formatWithDecimal)", context).stringValue());
        assertEquals("0", evaluateScriptWithContext("format(0.0, formatWhole)", context).stringValue());

        assertEquals("12,345", evaluateScriptWithContext("format(decimal, formatWhole)", context).stringValue());
        assertEquals("(12,345)", evaluateScriptWithContext("format(negDecimal, formatWhole)", context).stringValue());

        assertEquals("2,345", evaluateScriptWithContext("format(wholeInt, formatWhole)", context).stringValue());
        assertEquals("(2,345)", evaluateScriptWithContext("format(-wholeInt, formatWhole)", context).stringValue());

        assertEquals("345.456", evaluateScriptWithContext("format(decimalFloat, formatWithDecimal)", context).stringValue());
        assertEquals("(345.456)", evaluateScriptWithContext("format(-decimalFloat, formatWithDecimal)", context).stringValue());
    }

    @Test
    public void attemptToFormatString()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", new StringValue("abc"));

        assertThrows(RuntimeException.class, () -> evaluateScriptWithContext("format(x, 'yyMMdd')", context));
    }
}
