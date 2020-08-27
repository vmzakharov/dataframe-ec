package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.expr.value.*;
import io.github.vmzakharov.ecdataframe.expr.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.junit.Assert;

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
