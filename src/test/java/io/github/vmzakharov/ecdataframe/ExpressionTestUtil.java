package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.*;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.junit.Assert;

import java.time.LocalDate;

public class ExpressionTestUtil
{
    static public long evaluateToLong(String s)
    {
        return ((LongValue) evaluate(s)).longValue();
    }

    public static Value evaluate(String s)
    {
        return ExpressionParserHelper.toExpression(s).evaluate(new InMemoryEvaluationVisitor());
    }

    static public double evaluateToDouble(String s)
    {
        return ((DoubleValue) evaluate(s)).doubleValue();
    }

    static public boolean evaluateToBoolean(String s)
    {
        return ((BooleanValue) evaluate(s)).isTrue();
    }

    public static String evaluateToString(String s)
    {
        Value result = evaluate(s);
        Assert.assertTrue(result.isString());
        return result.stringValue();
    }

    public static LocalDate evaluateToDate(String s)
    {
        return ((DateValue) evaluate(s)).dateValue();
    }
}
