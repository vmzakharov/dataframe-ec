package org.modelscript.expr;

import org.junit.Assert;
import org.junit.Test;
import org.modelscript.ExpressionTestUtil;
import org.modelscript.expr.visitor.PrettyPrintVisitor;
import org.modelscript.util.ExpressionParserHelper;

public class Printing
{
    @Test
    public void printScript()
    {
        String scriptText =
                "x= 1\n" +
                "y =2\n" +
                "z=x+ y\n" +
                "3+1 +2";
        AnonymousScript script = ExpressionParserHelper.toScript(scriptText);

        PrettyPrintVisitor visitor = new PrettyPrintVisitor();
        script.accept(visitor);
    }

    @Test
    public void printFunctionCalls()
    {
        String scriptText =
                "x= 1\n" +
                "y =2\n" +
                "foo(x, y + 2, time(), \"Oy\")\n" +
                "3 + bar(1, 2) - zzz()";
        AnonymousScript script = ExpressionParserHelper.toScript(scriptText);

        PrettyPrintVisitor visitor = new PrettyPrintVisitor();
        script.accept(visitor);
    }

    @Test
    public void printProjection()
    {
        String scriptText =
                "project {Foo.bar, Foo.baz}\n" +
                "where\n" +
                "Foo.qux > 7";
        AnonymousScript script = ExpressionParserHelper.toScript(scriptText);

        PrettyPrintVisitor visitor = new PrettyPrintVisitor();
        script.accept(visitor);
    }

    @Test
    public void expressionToString()
    {
        String expressionAsString = "(1 + 2)/ (7.0 +9)";
        Expression expression = ExpressionParserHelper.toExpression(expressionAsString);

        String result = PrettyPrintVisitor.exprToString(expression);
        System.out.println(result);

        Assert.assertEquals("((1 + 2) / (7.0 + 9))", result);
    }
}
