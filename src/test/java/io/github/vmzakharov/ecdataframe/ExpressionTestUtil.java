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
import io.github.vmzakharov.ecdataframe.dsl.value.FloatValue;
import io.github.vmzakharov.ecdataframe.dsl.value.IntValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.VectorValue;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.list.ListIterable;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final public class ExpressionTestUtil
{
    static private final double TOLERANCE = 0.00000001;

    private ExpressionTestUtil()
    {
        // Utility class
    }

    static public long evaluateToLong(String s)
    {
        return ((LongValue) evaluateExpression(s)).longValue();
    }

    static public int evaluateToInt(String s)
    {
        return ((IntValue) evaluateExpression(s)).intValue();
    }

    static public ListIterable<Value> evaluateToVector(String s)
    {
        return ((VectorValue) evaluateExpression(s)).getElements();
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

    static public double evaluateToFloat(String s)
    {
        return ((FloatValue) evaluateExpression(s)).floatValue();
    }

    static public boolean evaluateToBoolean(String s)
    {
        return ((BooleanValue) evaluateExpression(s)).isTrue();
    }

    public static String evaluateToString(String s)
    {
        Value result = evaluateExpression(s);
        assertTrue(result.isString());
        return result.stringValue();
    }

    public static BigDecimal evaluateToDecimal(String s)
    {
        Value result = evaluateExpression(s);
        assertTrue(result.isDecimal());
        return ((DecimalValue) result).decimalValue();
    }

    public static void assertDoubleResult(double expected, String scriptString, EvalContext context)
    {
        Value value = evaluateScriptWithContext(scriptString, context);

        assertTrue(value.isDouble());
        assertEquals(expected, ((DoubleValue) value).doubleValue(), TOLERANCE);
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

    public static void scriptEvaluatesToTrue(String scriptAsString)
    {
        Value result = evaluateScript(scriptAsString);
        assertTrueValue(result);
    }

    public static void scriptEvaluatesToFalse(String scriptAsString, EvalContext context)
    {
        Value result = evaluateScriptWithContext(scriptAsString, context);
        assertFalseValue(result);
    }

    public static void scriptEvaluatesToFalse(String scriptAsString)
    {
        Value result = evaluateScript(scriptAsString);
        assertFalseValue(result);
    }

    public static Value evaluateScriptWithContext(String scriptAsString, EvalContext context)
    {
        AnonymousScript script = ExpressionTestUtil.toScript(scriptAsString);
        return script.evaluate(new InMemoryEvaluationVisitor(context));
    }

    public static Value evaluateScript(String scriptAsString)
    {
        AnonymousScript script = ExpressionTestUtil.toScript(scriptAsString);
        return script.evaluate(new InMemoryEvaluationVisitor());
    }

    private static void assertTrueValue(Value aValue)
    {
        assertTrue(aValue.isBoolean());
        assertTrue(((BooleanValue) aValue).isTrue());
    }

    private static void assertFalseValue(Value aValue)
    {
        assertTrue(aValue.isBoolean());
        assertTrue(((BooleanValue) aValue).isFalse());
    }
}
