package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

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
        assertTrue(result.isVoid());

        script = ExpressionTestUtil.toScript("2 + a + 1 - b * 7");
        result = script.evaluate(new InMemoryEvaluationVisitor(context));
        assertTrue(result.isVoid());

        script = ExpressionTestUtil.toScript("a + (b - b) + 3");
        result = script.evaluate(new InMemoryEvaluationVisitor(context));
        assertTrue(result.isVoid());
    }

    @Test
    public void stringOps()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("a", new StringValue("abc"));
        context.setVariable("b", Value.VOID);

        AnonymousScript script = ExpressionTestUtil.toScript("a + b");
        Value result = script.evaluate(new InMemoryEvaluationVisitor(context));
        assertTrue(result.isVoid());

        script = ExpressionTestUtil.toScript("b + 'X'");
        result = script.evaluate(new InMemoryEvaluationVisitor(context));
        assertTrue(result.isVoid());
    }

    @Test
    public void stringFunctions()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("a", Value.VOID);

        AnonymousScript script = ExpressionTestUtil.toScript("substr(a, 1, 10)");
        Value result = script.evaluate(new InMemoryEvaluationVisitor(context));
        assertTrue(result.isVoid());
    }

    @Test
    public void stringOperatorsOrderSensitive()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("a", Value.VOID);
        context.setVariable("voidValue", Value.VOID);

        AnonymousScript script = ExpressionTestUtil.toScript("a is not null and a in ('1', '2', '3')");
        Value result = script.evaluate(new InMemoryEvaluationVisitor(context));
        assertTrue(result.isBoolean());
        assertTrue(((BooleanValue) result).isFalse());

        script = ExpressionTestUtil.toScript("a is null or a in ('1', '2', '3')");
        result = script.evaluate(new InMemoryEvaluationVisitor(context));
        assertTrue(result.isBoolean());
        assertTrue(((BooleanValue) result).isTrue());

        script = ExpressionTestUtil.toScript("a in ('1', '2', voidValue, '3')");
        result = script.evaluate(new InMemoryEvaluationVisitor(context));
        assertTrue(result.isBoolean());
        assertTrue(((BooleanValue) result).isTrue());
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
        assertEquals(7L, ((LongValue) result).longValue());

        script = ExpressionTestUtil.toScript("c is not null ? a + b : a");
        result = script.evaluate(new InMemoryEvaluationVisitor(context));
        assertEquals(5L, ((LongValue) result).longValue());

        script = ExpressionTestUtil.toScript("a is not null ? a : a + b");
        result = script.evaluate(new InMemoryEvaluationVisitor(context));
        assertEquals(5L, ((LongValue) result).longValue());
    }

    public void comparison()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("a", new LongValue(5));
        context.setVariable("b", Value.VOID);

        AnonymousScript script = ExpressionTestUtil.toScript("a > b");
        script.evaluate(new InMemoryEvaluationVisitor(context));
        ExpressionTestUtil.scriptEvaluatesToTrue("a > b", context);
    }

    @Test
    public void lookingForNullValueInList()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("a", Value.VOID);
        context.setVariable("b", Value.VOID);

        ExpressionTestUtil.scriptEvaluatesToFalse("b in ('a', 'b', 'c')", context);
        ExpressionTestUtil.scriptEvaluatesToTrue("a in ('a', b, 'c')", context);
        ExpressionTestUtil.scriptEvaluatesToFalse("a not in ('a', b, 'c')", context);
    }

    @Test
    public void lookingForValueInListWithNulls()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("b", Value.VOID);

        ExpressionTestUtil.scriptEvaluatesToFalse("'foo' in ('a', b, 'c')", context);
        ExpressionTestUtil.scriptEvaluatesToTrue("'foo' not in ('a', b, 'c')", context);
    }
}
