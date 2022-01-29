package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.dsl.Script;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.CollectingPrinter;
import io.github.vmzakharov.ecdataframe.util.PrinterFactory;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.impl.factory.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.*;

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
    }

    @Test
    public void toUpper()
    {
        Assert.assertEquals("HELLO", evaluateToString("toUpper('Hello')"));
        Assert.assertEquals("THERE! 123", evaluateToString("toUpper(\"THERE! 123\")"));
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
    public void abs()
    {
        Assert.assertEquals(10, evaluateToLong("abs(-10)"));
        Assert.assertEquals(10, evaluateToLong("abs(10)"));
        Assert.assertEquals(10.0, evaluateToDouble("abs(-10.0)"), 0.0);
        Assert.assertEquals(10.0, evaluateToDouble("abs(10.0)"), 0.0);
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
        Assert.assertEquals(LocalDate.of(2021, 11, 21), evaluateToDate("toDate(2021, 11, 21)"));
        Assert.assertEquals(LocalDate.of(2021, 11, 21), evaluateToDate("toDate(\"2021-11-21\")"));
        Assert.assertEquals(LocalDate.of(2011, 12, 20), evaluateToDate("toDate('2011-12-20')"));
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
    public void listAllFunctions()
    {
        BuiltInFunctions.resetFunctionList();

        MapIterable<String, IntrinsicFunctionDescriptor> functionsByName = BuiltInFunctions.getFunctionsByName();

        MutableList<String> expectedFunctionNames = Lists.mutable.of(
            "abs", "contains", "print", "println", "startsWith", "substr", "toDate", "toDouble", "toLong", "toString",
            "toUpper", "withinDays"
        );

        Assert.assertEquals(expectedFunctionNames.size(), functionsByName.size());

        Assert.assertTrue(expectedFunctionNames.collect(String::toUpperCase).allSatisfy(functionsByName::containsKey));
    }
}
