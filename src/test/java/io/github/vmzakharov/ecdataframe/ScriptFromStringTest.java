package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import org.junit.Assert;
import org.junit.Test;

public class ScriptFromStringTest
{
    @Test
    public void simpleAssignments()
    {
        String scriptText =
                  "x = 1\n"
                + "y = 2\n"
                + "z = x + y";
        AnonymousScript script = ExpressionTestUtil.toScript(scriptText);
        Value result = script.evaluate(new InMemoryEvaluationVisitor());
        Assert.assertTrue(result.isLong());
        Assert.assertEquals(3, ((LongValue) result).longValue());
    }

    @Test
    public void simpleAssignmentsWithHangingExpression()
    {
        String scriptText =
                  "x= 1\n"
                + "y =2\n"
                + "z=x+ y\n"
                + "3+1 +2";
        Value result = ExpressionTestUtil.evaluateScript(scriptText);
        Assert.assertTrue(result.isLong());
        Assert.assertEquals(6, ((LongValue) result).longValue());
    }

    @Test
    public void inOperator()
    {
        Value result = ExpressionTestUtil.evaluateScript(
                """
                x = 1
                y = 1
                x in (3, 2, y)""");
        Assert.assertTrue(result.isBoolean());
        Assert.assertTrue(((BooleanValue) result).isTrue());

        result = ExpressionTestUtil.evaluateScript(
                """
                x= "a"
                y= "b"
                q= "c"
                x in ("b", y, q)""");
        Assert.assertTrue(result.isBoolean());
        Assert.assertFalse(((BooleanValue) result).isTrue());

        result = ExpressionTestUtil.evaluateScript(
                """
                y= "b"
                q= "c"
                "c" in ("b", y, q)""");
        Assert.assertTrue(result.isBoolean());
        Assert.assertTrue(((BooleanValue) result).isTrue());
    }

    @Test
    public void ifStatement()
    {
        Value result = ExpressionTestUtil.evaluateScript(
                """
                x = "a"
                if x in ("a", "b", "c")
                then
                   result = "in"
                else
                   result = "not in"
                endif
                result
                """
        );

        Assert.assertEquals("in", result.stringValue());
    }

    @Test
    public void nestedIfStatement()
    {
        Value result = ExpressionTestUtil.evaluateScript(
                """
                x = "a"
                if x in ("a", "b", "c") then
                  2 + 2
                  result = "in"
                  if x == "b" then y = 5 else y = 6 endif
                else
                  result = "not in"
                  if x == "q" \
                    then y = 7
                    else y = 8
                  endif
                endif
                y
                """
        );

        Assert.assertEquals(6, ((LongValue) result).longValue());
    }

    @Test
    public void ifStatementAsExpression()
    {
        Value result = ExpressionTestUtil.evaluateScript(
                """
                x = "aa"
                if x in ("a", "b", "c")
                then
                   result = "in"
                else
                   result = "not in"
                endif
                """
        );
        Assert.assertEquals("not in", result.stringValue());
    }

    @Test
    public void nestedIfStatementAsExpression()
    {
        Value result = ExpressionTestUtil.evaluateScript(
                """
                x = "a"
                if x in ("a", "b", "c")
                then
                  2 + 2
                  if x == "b" then 5 else 6 endif
                else
                  if x == "q" \
                    then 7
                    else 8
                  endif
                endif
                """
        );

        Assert.assertEquals(6, ((LongValue) result).longValue());
    }

    @Test
    public void escapedVariableNames()
    {
        String scriptText =
                """
                ${x} = 1
                ${a-b} = 2
                ${It was a cold day} = ${x} + ${a-b}
                2 * ${It was a cold day}""";

        Value result = ExpressionTestUtil.evaluateScript(scriptText);
        Assert.assertTrue(result.isLong());
        Assert.assertEquals(6, ((LongValue) result).longValue());
    }

    @Test
    public void escapedVariableNamesWithQuotes()
    {
        String scriptText =
                """
                ${Bob's Number} = 1
                ${"Alice"} = 2
                ${"Alice"} - ${Bob's Number}
                """;

        Value result = ExpressionTestUtil.evaluateScript(scriptText);
        Assert.assertTrue(result.isLong());
        Assert.assertEquals(1, ((LongValue) result).longValue());
    }

    @Test
    public void scriptWithMixedQuotes()
    {
        String scriptText =
                "x = 'ba\"r'\n"
                + "x in (\"qux\", 'ba\"r', 'baz', \"wal'do\")";

        AnonymousScript script = ExpressionTestUtil.toScript(scriptText);
        Value result = script.evaluate(new InMemoryEvaluationVisitor());
        Assert.assertTrue(result.isBoolean());
        Assert.assertTrue(((BooleanValue) result).isTrue());
    }
}
