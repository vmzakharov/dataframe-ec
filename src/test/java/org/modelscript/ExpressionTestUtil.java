package org.modelscript;

import org.junit.Assert;
import org.modelscript.expr.value.*;
import org.modelscript.expr.visitor.InMemoryEvaluationVisitor;
import org.modelscript.util.ExpressionParserHelper;

import java.time.LocalDate;

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

    public static LocalDate evaluateToDate(String s)
    {
        return ((DateValue) ExpressionParserHelper.toExpression(s).evaluate(new InMemoryEvaluationVisitor())).dateValue();
    }
}
