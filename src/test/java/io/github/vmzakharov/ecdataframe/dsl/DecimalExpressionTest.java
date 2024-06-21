package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.DecimalValue;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.MathContext;

import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateScriptWithContext;
import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateToDecimal;
import static org.junit.jupiter.api.Assertions.*;

public class DecimalExpressionTest
{
    @Test
    public void literals()
    {
        assertEquals(BigDecimal.valueOf(579, 4), evaluateToDecimal("toDecimal(123,4) + toDecimal(456,4)"));

        assertEquals(BigDecimal.valueOf(-333, 4), evaluateToDecimal("toDecimal(123,4) - toDecimal(456,4)"));

        assertEquals(BigDecimal.valueOf(56088, 8), evaluateToDecimal("toDecimal(123,4) * toDecimal(456,4)"));

        assertEquals(
                BigDecimal.valueOf(123.0).divide(BigDecimal.valueOf(456.0), MathContext.DECIMAL128),
                evaluateToDecimal("toDecimal(123, 4) / toDecimal(456, 4)"));
    }

    @Test
    public void expressions()
    {
        EvalContext context = new SimpleEvalContext();
        context.setVariable("a", new DecimalValue(123456, 2));
        context.setVariable("b", new DecimalValue(111, 2));

         assertEquals(
                 BigDecimal.valueOf(123567, 2), this.evaluateToDecimalWithContext("a + b", context));

         assertEquals(
                 BigDecimal.valueOf(123345, 2), this.evaluateToDecimalWithContext("a - b", context));

         assertEquals(
                 BigDecimal.valueOf(13703616, 4), this.evaluateToDecimalWithContext("a * b", context));

         assertEquals(
                 BigDecimal.valueOf(123456).divide(BigDecimal.valueOf(111), MathContext.DECIMAL128),
                 this.evaluateToDecimalWithContext("a / b", context));
    }

    @Test
    public void mixedTypes()
    {
        assertEquals(BigDecimal.valueOf(20123, 4), evaluateToDecimal("toDecimal(123,4) + 2"));
        assertEquals(BigDecimal.valueOf(2623, 4), evaluateToDecimal("toDecimal(123,4) + 0.25"));
    }

    private BigDecimal evaluateToDecimalWithContext(String expression, EvalContext context)
    {
        return ((DecimalValue) evaluateScriptWithContext(expression, context)).decimalValue();
    }
}
