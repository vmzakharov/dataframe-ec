package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.BinaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.BinaryOp;
import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateTimeValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DecimalValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.junit.Assert;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

final public class ExpressionTestUtil
{
    private ExpressionTestUtil()
    {
        // Utility class
    }

    static public long evaluateToLong(String s)
    {
        return ((LongValue) evaluateExpression(s)).longValue();
    }

    public static AnonymousScript toScript(String s)
    {
        return ExpressionParserHelper.DEFAULT.toScript(s);
    }

    public static Expression toExpression(String s)
    {
        return ExpressionParserHelper.DEFAULT.toExpression(s);
    }

    public static Value evaluateExpression(String s)
    {
        return ExpressionParserHelper.DEFAULT.toExpression(s).evaluate(new InMemoryEvaluationVisitor());
    }

    static public double evaluateToDouble(String s)
    {
        return ((DoubleValue) evaluateExpression(s)).doubleValue();
    }

    static public boolean evaluateToBoolean(String s)
    {
        return ((BooleanValue) evaluateExpression(s)).isTrue();
    }

    public static String evaluateToString(String s)
    {
        Value result = evaluateExpression(s);
        Assert.assertTrue(result.isString());
        return result.stringValue();
    }

    public static BigDecimal evaluateToDecimal(String s)
    {
        Value result = evaluateExpression(s);
        Assert.assertTrue(result.isDecimal());
        return ((DecimalValue) result).decimalValue();
    }

    public static LocalDate evaluateToDate(String s)
    {
        return ((DateValue) evaluateExpression(s)).dateValue();
    }

    public static LocalDateTime evaluateToDateTime(String s)
    {
        return ((DateTimeValue) evaluateExpression(s)).dateTimeValue();
    }

    public static boolean evaluate(BinaryOp op, Value value1, Value value2)
    {
        InMemoryEvaluationVisitor evaluationVisitor = new InMemoryEvaluationVisitor();
        Expression expression = new BinaryExpr(value1, value2, op);
        return ((BooleanValue) expression.evaluate(evaluationVisitor)).isTrue();
    }

    public static void scriptEvaluatesToTrue(String scriptAsString, EvalContext context)
    {
        Value result = evaluateScriptWithContext(scriptAsString, context);
        assertTrueValue(result);
    }

    public static void scriptEvaluatesToFalse(String scriptAsString, EvalContext context)
    {
        Value result = evaluateScriptWithContext(scriptAsString, context);
        assertFalseValue(result);
    }

    public static Value evaluateScriptWithContext(String scriptAsString, EvalContext context)
    {
        AnonymousScript script = ExpressionTestUtil.toScript(scriptAsString);
        return script.evaluate(new InMemoryEvaluationVisitor(context));
    }

    private static void assertTrueValue(Value aValue)
    {
        Assert.assertTrue(aValue.isBoolean());
        Assert.assertTrue(((BooleanValue) aValue).isTrue());
    }

    private static void assertFalseValue(Value aValue)
    {
        Assert.assertTrue(aValue.isBoolean());
        Assert.assertTrue(((BooleanValue) aValue).isFalse());
    }
}
