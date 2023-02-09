package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.ExpressionTestUtil;
import io.github.vmzakharov.ecdataframe.dsl.visitor.PrettyPrintVisitor;
import org.junit.Assert;
import org.junit.Test;

public class PrettyPrintingTest
{
    @Test
    public void script()
    {
        String scriptText =
                  "x= 1\n"
                + "y =2\n"
                + "z=x+ y\n"
                + "3+1 +2";
        AnonymousScript script = ExpressionTestUtil.toScript(scriptText);

        String result = PrettyPrintVisitor.exprToString(script);

        Assert.assertEquals("x = 1\n"
                + "y = 2\n"
                + "z = (x + y)\n"
                + "((3 + 1) + 2)\n"
                , result);
    }

    @Test
    public void scriptWithFunctionDeclaration()
    {
        String scriptText =
                  "function sum(a, b)\n"
                + "{  a + b\n"
                + "}\n"
                + "x= 1\n"
                + "y =2\n"
                + "sum(x, y)";
        AnonymousScript script = ExpressionTestUtil.toScript(scriptText);

        String result = PrettyPrintVisitor.exprToString(script);

        Assert.assertEquals(
                  "function sum(a, b)\n"
                + "{\n"
                + "  (a + b)\n"
                + "}\n"
                + "x = 1\n"
                + "y = 2\n"
                + "sum(x, y)\n",
                result);
    }

    @Test
    public void printFunctionCalls()
    {
        String scriptText =
                  "x= 1\n"
                + "y =2\n"
                + "foo(x, y + 2, time(), \"Oy\")\n"
                + "3 + bar(1, 2) - zzz()";
        AnonymousScript script = ExpressionTestUtil.toScript(scriptText);

        String result = PrettyPrintVisitor.exprToString(script);

        Assert.assertEquals("x = 1\n"
                + "y = 2\n"
                + "foo(x, (y + 2), time(), \"Oy\")\n"
                + "((3 + bar(1, 2)) - zzz())\n"
                , result);
    }

    @Test
    public void printProjection()
    {
        String scriptText =
                  "project {Foo.bar, Foo.baz}\n"
                + "where\n"
                + "Foo.qux > 7";
        AnonymousScript script = ExpressionTestUtil.toScript(scriptText);

        String result = PrettyPrintVisitor.exprToString(script);

        Assert.assertEquals("project {Foo.bar, Foo.baz} where (Foo.qux > 7)\n", result);
    }

    @Test
    public void printCondition()
    {
        String scriptText =
                  "if a > b then\n"
                + "\"a\" else\n"
                + "\"b\"\n"
                + "endif";
        AnonymousScript script = ExpressionTestUtil.toScript(scriptText);

        String result = PrettyPrintVisitor.exprToString(script);

        Assert.assertEquals(
                  "if (a > b) then\n"
                + "  \"a\"\n"
                + "else\n"
                + "  \"b\"\n"
                + "endif\n", result);
    }

    @Test
    public void printTernaryOperator()
    {
        String scriptText = "a > b ? 5 + 6 : 12";

        Expression expression = ExpressionTestUtil.toExpression(scriptText);

        String result = PrettyPrintVisitor.exprToString(expression);

        Assert.assertEquals("(a > b) ? (5 + 6) : 12", result);
    }

    @Test
    public void printConditionWithTernary()
    {
        String scriptText =
                  "if a > b then\n"
                + "a > b ? 5 + 6 : 12 else\n"
                + "a < b / 2 ? 1 : 2\n"
                + "endif";
        AnonymousScript script = ExpressionTestUtil.toScript(scriptText);

        String result = PrettyPrintVisitor.exprToString(script);

        Assert.assertEquals(
                  "if (a > b) then\n"
                + "  (a > b) ? (5 + 6) : 12\n"
                + "else\n"
                + "  (a < (b / 2)) ? 1 : 2\n"
                + "endif\n", result);
    }

    @Test
    public void weirdVariableNames()
    {
        String scriptText =
                  "${hello, friend} = 'hello'\n"
                + "if ${a b} > ${b-c} then\n"
                + "  ${a b}\n"
                + "else\n"
                + "  ${b-c}\n"
                + "endif";
        AnonymousScript script = ExpressionTestUtil.toScript(scriptText);

        String result = PrettyPrintVisitor.exprToString(script);

        Assert.assertEquals(
                  "${hello, friend} = \"hello\"\n"
                + "if (${a b} > ${b-c}) then\n"
                + "  ${a b}\n"
                + "else\n"
                + "  ${b-c}\n"
                + "endif\n", result);
    }

    @Test
    public void nestedQuotes()
    {
        String  expressionAsString = "'foo' in (\"qux\", 'ba\"r', 'baz', \"wal'do\")";
        Expression expression = ExpressionTestUtil.toExpression(expressionAsString);

        String result = PrettyPrintVisitor.exprToString(expression);

        Assert.assertEquals("(\"foo\" in (\"qux\", 'ba\"r', \"baz\", \"wal'do\"))", result);
    }

    @Test
    public void expressionToString()
    {
        String expressionAsString = "(1 + 2)/ (7.0 +9)";
        Expression expression = ExpressionTestUtil.toExpression(expressionAsString);

        String result = PrettyPrintVisitor.exprToString(expression);

        Assert.assertEquals("((1 + 2) / (7.0 + 9))", result);
    }
}
