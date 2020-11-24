package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.junit.Assert;
import org.junit.Test;

public class ScriptFromStringTest
{
    @Test
    public void simpleAssignments()
    {
        String scriptText =
                "x = 1\n" +
                "y = 2\n" +
                "z = x + y";
        AnonymousScript script = ExpressionParserHelper.toScript(scriptText);
        Value result = script.evaluate(new InMemoryEvaluationVisitor());
        Assert.assertTrue(result.isLong());
        Assert.assertEquals(3, ((LongValue) result).longValue());
    }

    @Test
    public void simpleAssignmentsWithHangingExpression()
    {
        String scriptText =
                "x= 1\n" +
                "y =2\n" +
                "z=x+ y\n" +
                "3+1 +2";
        AnonymousScript script = ExpressionParserHelper.toScript(scriptText);
        Value result = script.evaluate();
        Assert.assertTrue(result.isLong());
        Assert.assertEquals(6, ((LongValue) result).longValue());
    }

    @Test
    public void inOperator()
    {
        AnonymousScript script = ExpressionParserHelper.toScript(
                "x = 1\n" +
                "y = 1\n" +
                "x in (3, 2, y)");
        Value result = script.evaluate();
        Assert.assertTrue(result.isBoolean());
        Assert.assertTrue(((BooleanValue) result).isTrue());

        script = ExpressionParserHelper.toScript(
                "x= \"a\"\n" +
                "y= \"b\"\n" +
                "q= \"c\"\n" +
                "x in (\"b\", y, q)");
        result = script.evaluate();
        Assert.assertTrue(result.isBoolean());
        Assert.assertFalse(((BooleanValue) result).isTrue());

        script = ExpressionParserHelper.toScript(
                "y= \"b\"\n" +
                "q= \"c\"\n" +
                "\"c\" in (\"b\", y, q)");
        result = script.evaluate();
        Assert.assertTrue(result.isBoolean());
        Assert.assertTrue(((BooleanValue) result).isTrue());
    }

    @Test
    public void ifStatement()
    {
        AnonymousScript script = ExpressionParserHelper.toScript(
                "x = \"a\"\n" +
                "if x in (\"a\", \"b\", \"c\")\n" +
                "then\n" +
                "   result = \"in\"\n" +
                "else\n" +
                "   result = \"not in\"\n" +
                "endif\n" +
                "result\n"
        );

        Value result = script.evaluate();
        Assert.assertEquals("in", result.stringValue());
    }

    @Test
    public void nestedIfStatement()
    {
        AnonymousScript script = ExpressionParserHelper.toScript(
                "x = \"a\"\n" +
                "if x in (\"a\", \"b\", \"c\") then\n" +
                "  2 + 2\n" +
                "  result = \"in\"\n" +
                "  if x == \"b\" then y = 5 else y = 6 endif\n" +
                "else\n" +
                "  result = \"not in\"\n" +
                "  if x == \"q\" " +
                "    then y = 7\n" +
                "    else y = 8\n" +
                "  endif\n" +
                "endif\n" +
                "y\n"
        );

        Value result = script.evaluate();
        Assert.assertEquals(6, ((LongValue) result).longValue());
    }

    @Test
    public void ifStatementAsExpression()
    {
        AnonymousScript script = ExpressionParserHelper.toScript(
                "x = \"aa\"\n" +
                "if x in (\"a\", \"b\", \"c\")\n" +
                "then\n" +
                "   result = \"in\"\n" +
                "else\n" +
                "   result = \"not in\"\n" +
                "endif\n"
        );
        Value result = script.evaluate();
        Assert.assertEquals("not in", result.stringValue());
    }

    @Test
    public void nestedIfStatementAsExpressionm()
    {
        AnonymousScript script = ExpressionParserHelper.toScript(
                "x = \"a\"\n" +
                "if x in (\"a\", \"b\", \"c\")\n" +
                "then\n" +
                "  2 + 2\n" +
                "  if x == \"b\" then 5 else 6 endif\n" +
                "else\n" +
                "  if x == \"q\" " +
                "    then 7\n" +
                "    else 8\n" +
                "  endif\n" +
                "endif\n"
        );

        Value result = script.evaluate();
        Assert.assertEquals(6, ((LongValue) result).longValue());
    }
}
