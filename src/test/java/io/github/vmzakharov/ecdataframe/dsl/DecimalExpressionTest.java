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
        Assert.assertEquals(BigDecimal.valueOf(579, 4), ExpressionTestUtil.evaluateToDecimal("[123,4] + [456,4]"));

        Assert.assertEquals(BigDecimal.valueOf(-333, 4), ExpressionTestUtil.evaluateToDecimal("[123,4] - [456,4]"));

        Assert.assertEquals(BigDecimal.valueOf(56088, 8), ExpressionTestUtil.evaluateToDecimal("[123,4] * [456,4]"));

        Assert.assertEquals(BigDecimal.valueOf(2697368421052632L, 16), ExpressionTestUtil.evaluateToDecimal("[123, 4] / [456, 4]"));
//        Assert.assertEquals(BigDecimal.valueOf((double) 123/(double) 456), ExpressionTestUtil.evaluateToDecimal("[123, 4] / [456, 4]"));
    }

    @Test
    public void expressions()
    {
        EvalContext context = new SimpleEvalContext();
        context.setVariable("a", new DecimalValue(123456, 2));
        context.setVariable("b", new DecimalValue(111, 2));

         Assert.assertEquals(
                 BigDecimal.valueOf(123567, 2),
                 ((DecimalValue) ExpressionTestUtil.evaluateScriptWithContext("a + b", context)).decimalValue());

         Assert.assertEquals(
                 BigDecimal.valueOf(123345, 2),
                 ((DecimalValue) ExpressionTestUtil.evaluateScriptWithContext("a - b", context)).decimalValue());

         Assert.assertEquals(
                 BigDecimal.valueOf(13703616, 4),
                 ((DecimalValue) ExpressionTestUtil.evaluateScriptWithContext("a * b", context)).decimalValue());

         Assert.assertEquals(
                 new BigDecimal(123456.0 / 111.0, MathContext.DECIMAL64),
                 ((DecimalValue) ExpressionTestUtil.evaluateScriptWithContext("a / b", context)).decimalValue());
    }
}
