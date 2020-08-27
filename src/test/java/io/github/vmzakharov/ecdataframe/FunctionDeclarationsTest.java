package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.expr.AnonymousScript;
import io.github.vmzakharov.ecdataframe.expr.value.LongValue;
import io.github.vmzakharov.ecdataframe.expr.value.Value;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.junit.Assert;
import org.junit.Test;

public class FunctionDeclarationsTest
{
    @Test
    public void simpleFunction()
    {
        String scriptText =
                "function x (a, b)\n" +
                "{\n" +
                "   z = a - b\n" +
                "   a + b\n" +
                "}\n" +
                "\n" +
                "y = 2\n" +
                "x(5, y * 7)";

        AnonymousScript script = ExpressionParserHelper.toScript(scriptText);
        Value result = script.evaluate();
        Assert.assertTrue(result.isLong());
        Assert.assertEquals(19, ((LongValue) result).longValue());
    }

    @Test
    public void manyFunctions()
    {
        String scriptText =
                "function sum(a, b)\n" +
                "{\n" +
                "   a + b\n" +
                "}\n" +
                "\n" +
                "function mul(a, b)\n" +
                "{\n" +
                "   a * b\n" +
                "}\n" +
                "\n" +
                "one = 1\n" +
                "two = 2\n" +
                "sum( mul(5, two), sum(one, two) )";

        AnonymousScript script = ExpressionParserHelper.toScript(scriptText);
        Value result = script.evaluate();
        Assert.assertTrue(result.isLong());
        Assert.assertEquals(13, ((LongValue) result).longValue());
    }

    @Test
    public void noArgFunction()
    {
        String scriptText =
                "function hello\n" +
                "{\n" +
                "   \"hello\"\n" +
                "}\n" +
                "\n" +
                "function bang\n" +
                "{\n" +
                "   \"!\"\n" +
                "}\n" +
                "\n" +
                "hello() + \" there\" + bang()";

        AnonymousScript script = ExpressionParserHelper.toScript(scriptText);
        Value result = script.evaluate();
        Assert.assertTrue(result.isString());
        Assert.assertEquals("hello there!", result.stringValue());
    }
}
