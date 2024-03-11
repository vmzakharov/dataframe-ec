package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.dsl.Script;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.IntValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.CollectingPrinter;
import io.github.vmzakharov.ecdataframe.util.PrinterFactory;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateScriptWithContext;
import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateToBoolean;
import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateToDate;
import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateToDateTime;
import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateToDecimal;
import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateToDouble;
import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateToLong;
import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateToString;
import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.evaluateToVector;
import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.toScript;

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

        Assert.assertEquals("4\nene-mene\nHello!\n", collectingPrinter.toString());
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

        Assert.assertEquals("4 = 4\nene-mene, hello!\n", collectingPrinter.toString());
    }

    @Test
    public void substring()
    {
        Assert.assertEquals("there", evaluateToString("substr(\"Hello, there!\", 7, 12)"));
        Assert.assertEquals("there!", evaluateToString("substr(\"Hello, there!\", 7)"));
        Assert.assertEquals("Hello, there", evaluateToString("substr(\"Hello, there!\", 0, 12)"));

        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", Value.VOID);
        Assert.assertEquals(Value.VOID, evaluateScriptWithContext("substr(x, 10, 20)", context));
    }

    @Test
    public void toUpper()
    {
        Assert.assertEquals("HELLO", evaluateToString("toUpper('Hello')"));
        Assert.assertEquals("THERE! 123", evaluateToString("toUpper(\"THERE! 123\")"));
    }

    @Test
    public void trim()
    {
        Assert.assertEquals("Hello", evaluateToString("trim('Hello    ')"));
        Assert.assertEquals("Hello", evaluateToString("trim('   Hello')"));
        Assert.assertEquals("Hello", evaluateToString("trim('    Hello    ')"));
        Assert.assertEquals("Hello  there", evaluateToString("trim('    Hello  there    ')"));
    }

    @Test
    public void withinDays()
    {
        Assert.assertTrue(evaluateToBoolean("withinDays(toDate(2020, 11, 22), toDate(2020, 11, 20), 4)"));
        Assert.assertTrue(evaluateToBoolean("withinDays(toDate(2020, 11, 20), toDate(2020, 11, 22), 4)"));
        Assert.assertFalse(evaluateToBoolean("withinDays(toDate(2020, 11, 22), toDate(2020, 11, 20), 1)"));
        Assert.assertFalse(evaluateToBoolean("withinDays(toDate(2020, 11, 20), toDate(2020, 11, 24), 2)"));
    }

    @Test
    public void functionNamesNotCaseSensitive()
    {
        Assert.assertEquals("HELLO", evaluateToString("toUpper('Hello')"));
        Assert.assertEquals("HELLO", evaluateToString("toupper('Hello')"));
        Assert.assertEquals("HELLO", evaluateToString("TOUPPER('Hello')"));
        Assert.assertEquals("HELLO", evaluateToString("ToUpper('Hello')"));
    }

    @Test
    public void startsWith()
    {
        Assert.assertTrue(evaluateToBoolean("startsWith(\"Hello, there!\", \"Hello\")"));
        Assert.assertFalse(evaluateToBoolean("startsWith(\"Hello, there!\", \"Hola\")"));
        Assert.assertFalse(evaluateToBoolean("startsWith(\"Hello, there!\", \"there\")"));
    }

    @Test
    public void stringContains()
    {
        Assert.assertTrue(evaluateToBoolean("contains(\"Hello, there!\", \"Hello\")"));
        Assert.assertTrue(evaluateToBoolean("contains(\"Hello, there!\", \"lo, th\")"));
        Assert.assertTrue(evaluateToBoolean("contains(\"Hello, there!\", \"there!\")"));
        Assert.assertFalse(evaluateToBoolean("contains(\"Hello, there!\", \"Hola\")"));
    }

    @Test
    public void vectorFactory()
    {
        Assert.assertEquals(
                LongLists.immutable.of(1L, 2L, 3L),
                evaluateToVector("v(1, 2, 3)").collectLong(each -> ((LongValue) each).longValue())
        );

        Assert.assertEquals(
                Lists.immutable.of("abc"),
                evaluateToVector("v('abc')").collect(Value::stringValue)
        );

        Assert.assertEquals(
                Lists.immutable.of(1L, "two", 3L),
                evaluateToVector("v(1, 'two', 3)")
                        .collect(each -> each.isLong() ? ((LongValue) each).longValue() : each.stringValue())
        );
    }

    @Test
    public void abs()
    {
        Assert.assertEquals(10, evaluateToLong("abs(-10)"));
        Assert.assertEquals(10, evaluateToLong("abs(10)"));
        Assert.assertEquals(10.0, evaluateToDouble("abs(-10.0)"), 0.0);
        Assert.assertEquals(10.0, evaluateToDouble("abs(10.0)"), 0.0);

        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", Value.VOID);
        Assert.assertEquals(Value.VOID, evaluateScriptWithContext("abs(x)", context));
    }

    @Test
    public void absForInt()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("y", new IntValue(-5));
        IntValue result1 = (IntValue) evaluateScriptWithContext("abs(y)", context);
        Assert.assertEquals(5, result1.intValue());
        IntValue result2 = (IntValue) evaluateScriptWithContext("abs(-y)", context);
        Assert.assertEquals(5, result2.intValue());
    }

    @Test
    public void toStringForInt()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("y", new IntValue(123));
        context.setVariable("z", new IntValue(-456));
        Value result1 = evaluateScriptWithContext("toString(y)", context);
        Assert.assertEquals("123", result1.stringValue());
        Value result2 = evaluateScriptWithContext("toString(z)", context);
        Assert.assertEquals("-456", result2.stringValue());
    }

    @Test
    public void toStringFunction()
    {
        Assert.assertEquals("12345", evaluateToString("toString(12345)"));
        Assert.assertEquals("-10", evaluateToString("toString(-10)"));

        Assert.assertEquals("123.45", evaluateToString("toString(123.45)"));

        Assert.assertEquals("Boo", evaluateToString("toString('Boo')"));

        Assert.assertEquals("2020-10-06", evaluateToString("toString(toDate('2020-10-06'))"));

        Assert.assertEquals(LocalDate.of(2020, 10, 6), evaluateToDate("toDate(toString(toDate('2020-10-06')))"));
    }

    @Test
    public void toDate()
    {
        Assert.assertEquals(LocalDate.of(2021, 11, 1), evaluateToDate("toDate('2021-11-01')"));
        Assert.assertEquals(LocalDate.of(2021, 11, 21), evaluateToDate("toDate(2021, 11, 21)"));
        Assert.assertEquals(LocalDate.of(2021, 11, 21), evaluateToDate("toDate(\"2021-11-21\")"));
        Assert.assertEquals(LocalDate.of(2011, 12, 20), evaluateToDate("toDate('2011-12-20')"));
    }

    @Test
    public void toDateTime()
    {
        Assert.assertEquals(LocalDateTime.of(2021, 11,  1, 10, 35), evaluateToDateTime("toDateTime('2021-11-01T10:35')"));
        Assert.assertEquals(LocalDateTime.of(2021, 11, 21, 10, 35), evaluateToDateTime("toDateTime(2021, 11, 21, 10, 35)"));
        Assert.assertEquals(LocalDateTime.of(2021, 11, 21, 15, 5, 35), evaluateToDateTime("toDateTime(\"2021-11-21T15:05:35\")"));
        Assert.assertEquals(LocalDateTime.of(2011, 12, 20, 20, 45, 5), evaluateToDateTime("toDateTime('2011-12-20T20:45:05')"));
    }

    @Test
    public void toLong()
    {
        Assert.assertEquals(123L, evaluateToLong("toLong('123')"));
        Assert.assertEquals(-567L, evaluateToLong("toLong(\"-567\")"));
    }

    @Test
    public void toDouble()
    {
        Assert.assertEquals(123.0, evaluateToDouble("toDouble('123')"), TOLERANCE);
        Assert.assertEquals(-456.789, evaluateToDouble("toDouble('-456.789')"), TOLERANCE);
    }

    @Test
    public void toDecimal()
    {
        Assert.assertEquals(BigDecimal.valueOf(123, 5), evaluateToDecimal("toDecimal(123, 5)"));
        Assert.assertEquals(BigDecimal.valueOf(-456, -2), evaluateToDecimal("toDecimal(-456, -2)"));
    }

    @Test
    public void listAllFunctions()
    {
        BuiltInFunctions.resetFunctionList();

        MapIterable<String, IntrinsicFunctionDescriptor> functionsByName = BuiltInFunctions.getFunctionsByName();

        MutableList<String> expectedFunctionNames = Lists.mutable.of(
            "abs", "contains", "print", "println", "startsWith", "substr", "toDate", "toDateTime", "toDouble", "toLong",
            "toString", "toUpper", "trim", "withinDays", "format", "toDecimal", "v"
        );

        Assert.assertEquals(expectedFunctionNames.size(), functionsByName.size());

        Assert.assertTrue(expectedFunctionNames.collect(String::toUpperCase).allSatisfy(functionsByName::containsKey));
    }
}
