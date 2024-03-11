package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.IntValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import org.junit.Assert;
import org.junit.Test;

public class SimpleExpressionTest
{
    @Test
    public void addInts()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", new IntValue(4));
        context.setVariable("y", new IntValue(5));
        Value result = ExpressionTestUtil.toScript("x + y").evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertTrue(result.isLong());
        Assert.assertEquals(9L, ((LongValue) result).longValue());
    }

    @Test
    public void addIntToLong()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", new IntValue(4));
        Value result1 = ExpressionTestUtil.toScript("x + 1").evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertTrue(result1.isLong());
        Assert.assertEquals(5L, ((LongValue) result1).longValue());

        Value result2 = ExpressionTestUtil.toScript("2 + x").evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertTrue(result2.isLong());
        Assert.assertEquals(6L, ((LongValue) result2).longValue());
    }

    @Test
    public void multiplyInts()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", new IntValue(4));
        context.setVariable("y", new IntValue(5));
        Value result = ExpressionTestUtil.toScript("x * y").evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertTrue(result.isLong());
        Assert.assertEquals(20L, ((LongValue) result).longValue());
    }

    @Test
    public void negativeInts()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", new IntValue(4));
        Value result = ExpressionTestUtil.toScript("-x").evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertTrue(result.isInt());
        Assert.assertEquals(-4, ((IntValue) result).intValue());
    }
}
