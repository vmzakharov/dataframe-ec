package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.dsl.Script;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.FloatValue;
import io.github.vmzakharov.ecdataframe.dsl.value.IntValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.CollectingPrinter;
import io.github.vmzakharov.ecdataframe.util.PrinterFactory;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.*;
import static org.junit.jupiter.api.Assertions.*;

public class BuiltInFunctionTest
{
    final private static double TOLERANCE = 0.000001;

    @Test
    public void printFunction()
    {
        CollectingPrinter collectingPrinter = new CollectingPrinter();
        PrinterFactory.setPrinter(collectingPrinter);

        Script script = toScript(
                  "println(1 + 3)\n"
                + "print(\"ene\")\n"
                + "println(\"-mene\")\n"
                + "println(\"Hello!\")");

        script.evaluate(new InMemoryEvaluationVisitor());

        assertEquals("4\nene-mene\nHello!\n", collectingPrinter.toString());
    }

    @Test
    public void printWithVariableParameters()
    {
        CollectingPrinter collectingPrinter = new CollectingPrinter();
        PrinterFactory.setPrinter(collectingPrinter);

        String s = "println(1 + 3, \" = \", 4)\n"
                + "print(\"ene\", \"-mene\")\n"
                + "println(\", hello!\")";

        Script script = toScript(s);

        script.evaluate(new InMemoryEvaluationVisitor());

        assertEquals("4 = 4\nene-mene, hello!\n", collectingPrinter.toString());
    }

    @Test
    public void substring()
    {
        assertEquals("there", evaluateToString("substr(\"Hello, there!\", 7, 12)"));
        assertEquals("there!", evaluateToString("substr(\"Hello, there!\", 7)"));
        assertEquals("Hello, there", evaluateToString("substr(\"Hello, there!\", 0, 12)"));

        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", Value.VOID);
        assertEquals(Value.VOID, evaluateScriptWithContext("substr(x, 10, 20)", context));
    }

    @Test
    public void toUpper()
    {
        assertEquals("HELLO", evaluateToString("toUpper('Hello')"));
        assertEquals("THERE! 123", evaluateToString("toUpper(\"THERE! 123\")"));
    }

    @Test
    public void trim()
    {
        assertEquals("Hello", evaluateToString("trim('Hello    ')"));
        assertEquals("Hello", evaluateToString("trim('   Hello')"));
        assertEquals("Hello", evaluateToString("trim('    Hello    ')"));
        assertEquals("Hello  there", evaluateToString("trim('    Hello  there    ')"));
    }

    @Test
    public void withinDays()
    {
        assertTrue(evaluateToBoolean("withinDays(toDate(2020, 11, 22), toDate(2020, 11, 20), 4)"));
        assertTrue(evaluateToBoolean("withinDays(toDate(2020, 11, 20), toDate(2020, 11, 22), 4)"));
        assertFalse(evaluateToBoolean("withinDays(toDate(2020, 11, 22), toDate(2020, 11, 20), 1)"));
        assertFalse(evaluateToBoolean("withinDays(toDate(2020, 11, 20), toDate(2020, 11, 24), 2)"));
    }

    @Test
    public void functionNamesNotCaseSensitive()
    {
        assertEquals("HELLO", evaluateToString("toUpper('Hello')"));
        assertEquals("HELLO", evaluateToString("toupper('Hello')"));
        assertEquals("HELLO", evaluateToString("TOUPPER('Hello')"));
        assertEquals("HELLO", evaluateToString("ToUpper('Hello')"));
    }

    @Test
    public void startsWith()
    {
        assertTrue(evaluateToBoolean("startsWith(\"Hello, there!\", \"Hello\")"));
        assertFalse(evaluateToBoolean("startsWith(\"Hello, there!\", \"Hola\")"));
        assertFalse(evaluateToBoolean("startsWith(\"Hello, there!\", \"there\")"));
    }

    @Test
    public void stringContains()
    {
        assertTrue(evaluateToBoolean("contains(\"Hello, there!\", \"Hello\")"));
        assertTrue(evaluateToBoolean("contains(\"Hello, there!\", \"lo, th\")"));
        assertTrue(evaluateToBoolean("contains(\"Hello, there!\", \"there!\")"));
        assertFalse(evaluateToBoolean("contains(\"Hello, there!\", \"Hola\")"));
    }

    @Test
    public void vectorFactory()
    {
        assertEquals(
                LongLists.immutable.of(1L, 2L, 3L),
                evaluateToVector("v(1, 2, 3)").collectLong(each -> ((LongValue) each).longValue())
        );

        assertEquals(
                Lists.immutable.of("abc"),
                evaluateToVector("v('abc')").collect(Value::stringValue)
        );

        assertEquals(
                Lists.immutable.of(1L, "two", 3L),
                evaluateToVector("v(1, 'two', 3)")
                        .collect(each -> each.isLong() ? ((LongValue) each).longValue() : each.stringValue())
        );
    }

    @Test
    public void abs()
    {
        assertEquals(10, evaluateToLong("abs(-10)"));
        assertEquals(10, evaluateToLong("abs(10)"));
        assertEquals(10.0, evaluateToDouble("abs(-10.0)"), 0.0);
        assertEquals(10.0, evaluateToDouble("abs(10.0)"), 0.0);

        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", Value.VOID);
        assertEquals(Value.VOID, evaluateScriptWithContext("abs(x)", context));
    }

    @Test
    public void absForInt()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("y", new IntValue(-5));
        IntValue result1 = (IntValue) evaluateScriptWithContext("abs(y)", context);
        assertEquals(5, result1.intValue());
        IntValue result2 = (IntValue) evaluateScriptWithContext("abs(-y)", context);
        assertEquals(5, result2.intValue());
    }

    @Test
    public void toStringForInt()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("y", new IntValue(123));
        context.setVariable("z", new IntValue(-456));
        Value result1 = evaluateScriptWithContext("toString(y)", context);
        assertEquals("123", result1.stringValue());
        Value result2 = evaluateScriptWithContext("toString(z)", context);
        assertEquals("-456", result2.stringValue());
    }

    @Test
    public void absForString()
    {
        assertThrows(RuntimeException.class, () -> evaluateExpression("abs('Hello')"));
    }

    @Test
    public void absForFloat()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("y", new FloatValue(-5.25f));
        FloatValue result1 = (FloatValue) evaluateScriptWithContext("abs(y)", context);
        assertEquals(5.25f, result1.floatValue(), TOLERANCE);
        FloatValue result2 = (FloatValue) evaluateScriptWithContext("abs(-y)", context);
        assertEquals(5.25f, result2.floatValue(), TOLERANCE);
    }

    @Test
    public void toStringForFloat()
    {
        SimpleEvalContext context = new SimpleEvalContext();

        context.setVariable("y", new FloatValue(123.125f)); // keeping the fractions binary friendly to avoid noise
        context.setVariable("z", new FloatValue(-456.25f));

        Value result1 = evaluateScriptWithContext("toString(y)", context);
        assertEquals("123.125", result1.stringValue());

        Value result2 = evaluateScriptWithContext("toString(z)", context);
        assertEquals("-456.25", result2.stringValue());
    }

    @Test
    public void toStringFunction()
    {
        assertEquals("12345", evaluateToString("toString(12345)"));
        assertEquals("-10", evaluateToString("toString(-10)"));

        assertEquals("123.45", evaluateToString("toString(123.45)"));

        assertEquals("Boo", evaluateToString("toString('Boo')"));

        assertEquals("2020-10-06", evaluateToString("toString(toDate('2020-10-06'))"));

        assertEquals(LocalDate.of(2020, 10, 6), evaluateToDate("toDate(toString(toDate('2020-10-06')))"));
    }

    @Test
    public void toDate()
    {
        assertEquals(LocalDate.of(2021, 11, 1), evaluateToDate("toDate('2021-11-01')"));
        assertEquals(LocalDate.of(2021, 11, 21), evaluateToDate("toDate(2021, 11, 21)"));
        assertEquals(LocalDate.of(2021, 11, 21), evaluateToDate("toDate(\"2021-11-21\")"));
        assertEquals(LocalDate.of(2011, 12, 20), evaluateToDate("toDate('2011-12-20')"));
    }

    @Test
    public void toDateTime()
    {
        assertEquals(LocalDateTime.of(2021, 11,  1, 10, 35), evaluateToDateTime("toDateTime('2021-11-01T10:35')"));
        assertEquals(LocalDateTime.of(2021, 11, 21, 10, 35), evaluateToDateTime("toDateTime(2021, 11, 21, 10, 35)"));
        assertEquals(LocalDateTime.of(2021, 11, 21, 15, 5, 35), evaluateToDateTime("toDateTime(\"2021-11-21T15:05:35\")"));
        assertEquals(LocalDateTime.of(2011, 12, 20, 20, 45, 5), evaluateToDateTime("toDateTime('2011-12-20T20:45:05')"));
    }

    @Test
    public void toLong()
    {
        assertEquals(123L, evaluateToLong("toLong('123')"));
        assertEquals(-567L, evaluateToLong("toLong(\"-567\")"));
    }

    @Test
    public void toInt()
    {
        assertEquals(123, evaluateToInt("toInt('123')"));
        assertEquals(-567, evaluateToInt("toInt(\"-567\")"));
    }

    @Test
    public void toDouble()
    {
        assertEquals(123.0, evaluateToDouble("toDouble('123')"), TOLERANCE);
        assertEquals(-456.789, evaluateToDouble("toDouble('-456.789')"), TOLERANCE);
    }

    @Test
    public void toFloat()
    {
        assertEquals(123.0f, evaluateToFloat("toFloat('123')"), TOLERANCE);
        assertEquals(-456.789f, evaluateToFloat("toFloat('-456.789')"), TOLERANCE);
    }

    @Test
    public void toDecimal()
    {
        assertEquals(BigDecimal.valueOf(123, 5), evaluateToDecimal("toDecimal(123, 5)"));
        assertEquals(BigDecimal.valueOf(-456, -2), evaluateToDecimal("toDecimal(-456, -2)"));
    }

    @Test
    public void listAllFunctions()
    {
        BuiltInFunctions.resetFunctionList();

        ImmutableSet<String> actualFunctionNames = BuiltInFunctions.getFunctionsByName().keysView().toImmutableSet();

        ImmutableSet<String> expectedFunctionNames = Sets.immutable.of(
                "abs", "contains", "print", "println", "startsWith", "substr", "toDate", "toDateTime", "toDouble",
                "toFloat",  "toLong", "toInt", "toString", "toUpper", "trim", "withinDays", "format", "toDecimal", "v"
        );

        assertEquals(expectedFunctionNames.collect(String::toUpperCase), actualFunctionNames);
    }
}
