package io.github.vmzakharov.ecdataframe.dsl.function;

import io.github.vmzakharov.ecdataframe.dsl.Script;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.junit.Assert;
import org.junit.Test;
import io.github.vmzakharov.ecdataframe.ExpressionTestUtil;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.CollectingPrinter;
import io.github.vmzakharov.ecdataframe.util.PrinterFactory;

public class BuiltInFunctionTest
{
    @Test
    public void printFunction()
    {
        CollectingPrinter collectingPrinter = new CollectingPrinter();
        PrinterFactory.setPrinter(collectingPrinter);

        Script script = ExpressionParserHelper.toScript("println(1 + 3)\n" +
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

        Script script = ExpressionParserHelper.toScript(s);

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
    public void startsWith()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("startsWith(\"Hello, there!\", \"Hello\")"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("startsWith(\"Hello, there!\", \"Hola\")"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("startsWith(\"Hello, there!\", \"there\")"));
    }
}
