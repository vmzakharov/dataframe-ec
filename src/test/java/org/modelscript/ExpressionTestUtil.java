package org.modelscript;

import org.junit.Assert;
import org.modelscript.expr.value.*;
import org.modelscript.expr.visitor.InMemoryEvaluationVisitor;
import org.modelscript.util.ExpressionParserHelper;

public class ExpressionTestUtil
{
    static public long evaluateToInt(String s)
    {
        return ((LongValue) ExpressionParserHelper.toExpression(s).evaluate(new InMemoryEvaluationVisitor())).longValue();
    }

    static public double evaluateToDouble(String s)
    {
        return ((DoubleValue) ExpressionParserHelper.toExpression(s).evaluate(new InMemoryEvaluationVisitor())).doubleValue();
    }

    static public boolean evaluateToBoolean(String s)
    {
        return ((BooleanValue) ExpressionParserHelper.toExpression(s).evaluate(new InMemoryEvaluationVisitor())).isTrue();
    }

    public static String evaluateToString(String s)
    {
        Value result = ExpressionParserHelper.toExpression(s).evaluate(new InMemoryEvaluationVisitor());
        Assert.assertTrue(result.isString());
        return result.stringValue();
    }
}
