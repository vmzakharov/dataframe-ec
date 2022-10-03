package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.ExpressionTestUtil;
import io.github.vmzakharov.ecdataframe.dsl.value.DecimalValue;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.MathContext;

public class DecimalExpressionTest
{
    @Test
    public void literals()
    {
        Assert.assertEquals(BigDecimal.valueOf(579, 4), ExpressionTestUtil.evaluateToDecimal("toDecimal(123,4) + toDecimal(456,4)"));

        Assert.assertEquals(BigDecimal.valueOf(-333, 4), ExpressionTestUtil.evaluateToDecimal("toDecimal(123,4) - toDecimal(456,4)"));

        Assert.assertEquals(BigDecimal.valueOf(56088, 8), ExpressionTestUtil.evaluateToDecimal("toDecimal(123,4) * toDecimal(456,4)"));

        Assert.assertEquals(
                BigDecimal.valueOf(123.0).divide(BigDecimal.valueOf(456.0), MathContext.DECIMAL128),
                ExpressionTestUtil.evaluateToDecimal("toDecimal(123, 4) / toDecimal(456, 4)"));
    }

    @Test
    public void expressions()
    {
        EvalContext context = new SimpleEvalContext();
        context.setVariable("a", new DecimalValue(123456, 2));
        context.setVariable("b", new DecimalValue(111, 2));

         Assert.assertEquals(
                 BigDecimal.valueOf(123567, 2), this.evaluateToDecimalWithContext("a + b", context));

         Assert.assertEquals(
                 BigDecimal.valueOf(123345, 2), this.evaluateToDecimalWithContext("a - b", context));

         Assert.assertEquals(
                 BigDecimal.valueOf(13703616, 4), this.evaluateToDecimalWithContext("a * b", context));

         Assert.assertEquals(
                 BigDecimal.valueOf(123456).divide(BigDecimal.valueOf(111), MathContext.DECIMAL128),
                 this.evaluateToDecimalWithContext("a / b", context));
    }

    private BigDecimal evaluateToDecimalWithContext(String expression, EvalContext context)
    {
        return ((DecimalValue) ExpressionTestUtil.evaluateScriptWithContext(expression, context)).decimalValue();
    }
}
