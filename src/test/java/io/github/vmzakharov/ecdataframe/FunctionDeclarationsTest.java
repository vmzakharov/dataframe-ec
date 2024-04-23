package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.junit.Assert;
import org.junit.Test;

public class FunctionDeclarationsTest
{
    @Test
    public void simpleFunction()
    {
        String scriptText =
                  "function x (a, b)\n"
                + "{\n"
                + "   z = a - b\n"
                + "   a + b\n"
                + "}\n"
                + "\n"
                + "y = 2\n"
                + "x(5, y * 7)";

        Value result = ExpressionTestUtil.evaluateScript(scriptText);
        Assert.assertTrue(result.isLong());
        Assert.assertEquals(19, ((LongValue) result).longValue());
    }

    @Test
    public void manyFunctions()
    {
        String scriptText =
                """
                function sum(a, b)
                {
                   a + b
                }

                function mul(a, b)
                {
                   a * b
                }

                one = 1
                two = 2
                sum( mul(5, two), SUM(one, two) )""";

        Value result = ExpressionTestUtil.evaluateScript(scriptText);
        Assert.assertTrue(result.isLong());
        Assert.assertEquals(13, ((LongValue) result).longValue());
    }

    @Test
    public void noArgFunction()
    {
        String scriptText =
                  "function hello\n"
                + "{\n"
                + "   \"hello\"\n"
                + "}\n"
                + "\n"
                + "function bang\n"
                + "{\n"
                + "   \"!\"\n"
                + "}\n"
                + "\n"
                + "hello() + \" there\" + bang()";

        Value result = ExpressionTestUtil.evaluateScript(scriptText);
        Assert.assertTrue(result.isString());
        Assert.assertEquals("hello there!", result.stringValue());
    }
}
