package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import org.junit.Assert;
import org.junit.Test;

public class ExpressionWithNullValuesTest
{
    @Test
    public void arithmetic()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("a", new LongValue(5));
        context.setVariable("b", Value.VOID);

        AnonymousScript script = ExpressionTestUtil.toScript("a + b");
        Value result = script.evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertTrue(result.isVoid());

        script = ExpressionTestUtil.toScript("2 + a + 1 - b * 7");
        result = script.evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertTrue(result.isVoid());

        script = ExpressionTestUtil.toScript("a + (b - b) + 3");
        result = script.evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertTrue(result.isVoid());
    }

    @Test
    public void stringOps()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("a", new StringValue("abc"));
        context.setVariable("b", Value.VOID);
        context.setVariable("c", new StringValue(null));

        AnonymousScript script = ExpressionTestUtil.toScript("a + b");
        Value result = script.evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertTrue(result.isVoid());

        script = ExpressionTestUtil.toScript("b + 'X'");
        result = script.evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertTrue(result.isVoid());

        script = ExpressionTestUtil.toScript("a + c");
        result = script.evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertTrue(result.isVoid());
    }

    @Test
    public void conditional()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("a", new LongValue(5));
        context.setVariable("b", new LongValue(2));
        context.setVariable("c", Value.VOID);

        AnonymousScript script = ExpressionTestUtil.toScript("c is null ? a + b : a");
        Value result = script.evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertEquals(7L, ((LongValue) result).longValue());

        script = ExpressionTestUtil.toScript("c is not null ? a + b : a");
        result = script.evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertEquals(5L, ((LongValue) result).longValue());

        script = ExpressionTestUtil.toScript("a is not null ? a : a + b");
        result = script.evaluate(new InMemoryEvaluationVisitor(context));
        Assert.assertEquals(5L, ((LongValue) result).longValue());
    }

    @Test(expected = NullPointerException.class)
    public void comparison()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("a", new LongValue(5));
        context.setVariable("b", Value.VOID);

        AnonymousScript script = ExpressionTestUtil.toScript("a > b");
        script.evaluate(new InMemoryEvaluationVisitor(context));
    }

    @Test(expected = NullPointerException.class)
    public void lookingForNullValueInList()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("b", Value.VOID);

        AnonymousScript script = ExpressionTestUtil.toScript("b in ('a', 'b', 'c')");
        script.evaluate(new InMemoryEvaluationVisitor(context));
    }

    @Test(expected = NullPointerException.class)
    public void lookingForValueInListWithNulls()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("b", Value.VOID);

        AnonymousScript script = ExpressionTestUtil.toScript("'foo' in ('a', b, 'c')");
        script.evaluate(new InMemoryEvaluationVisitor(context));
    }
}
