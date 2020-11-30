package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.dsl.Script;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.junit.Assert;
import org.junit.Test;
import io.github.vmzakharov.ecdataframe.ExpressionTestUtil;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.CollectingPrinter;
import io.github.vmzakharov.ecdataframe.util.PrinterFactory;

import java.time.LocalDate;

public class BuiltInFunctionTest
{
    @Test
    public void printFunction()
    {
        CollectingPrinter collectingPrinter = new CollectingPrinter();
        PrinterFactory.setPrinter(collectingPrinter);

        Script script = ExpressionTestUtil.toScript("println(1 + 3)\n" +
                "print(\"ene\")\n" +
                "println(\"-mene\")\n" +
                "println(\"Hello!\")");

        script.evaluate(new InMemoryEvaluationVisitor());

        Assert.assertEquals("4\nene-mene\nHello!\n", collectingPrinter.toString());
    }

    @Test
    public void printWithVariableParameters()
    {
        CollectingPrinter collectingPrinter = new CollectingPrinter();
        PrinterFactory.setPrinter(collectingPrinter);

        String s = "println(1 + 3, \" = \", 4)\n" +
                "print(\"ene\", \"-mene\")\n" +
                "println(\", hello!\")";

        Script script = ExpressionTestUtil.toScript(s);

        script.evaluate(new InMemoryEvaluationVisitor());

        Assert.assertEquals("4 = 4\nene-mene, hello!\n", collectingPrinter.toString());
    }

    @Test
    public void substring()
    {
        Assert.assertEquals("there", ExpressionTestUtil.evaluateToString("substr(\"Hello, there!\", 7, 12)"));
        Assert.assertEquals("there!", ExpressionTestUtil.evaluateToString("substr(\"Hello, there!\", 7)"));
        Assert.assertEquals("Hello, there", ExpressionTestUtil.evaluateToString("substr(\"Hello, there!\", 0, 12)"));
    }

    @Test
    public void toUpper()
    {
        Assert.assertEquals("HELLO", ExpressionTestUtil.evaluateToString("toUpper('Hello')"));
        Assert.assertEquals("THERE! 123", ExpressionTestUtil.evaluateToString("toUpper(\"THERE! 123\")"));
    }

    @Test
    public void functionNamesNotCaseSensitive()
    {
        Assert.assertEquals("HELLO", ExpressionTestUtil.evaluateToString("toUpper('Hello')"));
        Assert.assertEquals("HELLO", ExpressionTestUtil.evaluateToString("toupper('Hello')"));
        Assert.assertEquals("HELLO", ExpressionTestUtil.evaluateToString("TOUPPER('Hello')"));
        Assert.assertEquals("HELLO", ExpressionTestUtil.evaluateToString("ToUpper('Hello')"));
    }

    @Test
    public void startsWith()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("startsWith(\"Hello, there!\", \"Hello\")"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("startsWith(\"Hello, there!\", \"Hola\")"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("startsWith(\"Hello, there!\", \"there\")"));
    }

    @Test
    public void stringContains()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("contains(\"Hello, there!\", \"Hello\")"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("contains(\"Hello, there!\", \"lo, th\")"));
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("contains(\"Hello, there!\", \"there!\")"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("contains(\"Hello, there!\", \"Hola\")"));
    }

    @Test
    public void abs()
    {
        Assert.assertEquals(10, ExpressionTestUtil.evaluateToLong("abs(-10)"));
        Assert.assertEquals(10, ExpressionTestUtil.evaluateToLong("abs(10)"));
        Assert.assertEquals(10.0, ExpressionTestUtil.evaluateToDouble("abs(-10.0)"), 0.0);
        Assert.assertEquals(10.0, ExpressionTestUtil.evaluateToDouble("abs(10.0)"), 0.0);
    }

    @Test
    public void toStringFunction()
    {
        Assert.assertEquals("12345", ExpressionTestUtil.evaluateToString("toString(12345)"));
        Assert.assertEquals("-10", ExpressionTestUtil.evaluateToString("toString(-10)"));

        Assert.assertEquals("123.45", ExpressionTestUtil.evaluateToString("toString(123.45)"));

        Assert.assertEquals("Boo", ExpressionTestUtil.evaluateToString("toString('Boo')"));

        Assert.assertEquals("2020-10-06", ExpressionTestUtil.evaluateToString("toString(toDate('2020-10-06'))"));
        
        Assert.assertEquals(LocalDate.of(2020, 10, 6), ExpressionTestUtil.evaluateToDate("toDate(toString(toDate('2020-10-06')))"));
    }
}
