package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class FunctionDeclarationsTest
{
    @Test
    public void simpleFunction()
    {
        String scriptText = """
                function x (a, b)
                {
                   z = a - b
                   a + b
                }
    
                y = 2
                x(5, y * 7)""";

        Value result = ExpressionTestUtil.evaluateScript(scriptText);
        assertTrue(result.isLong());
        assertEquals(19, ((LongValue) result).longValue());
    }

    @Test
    public void manyFunctions()
    {
        String scriptText = """
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
        assertTrue(result.isLong());
        assertEquals(13, ((LongValue) result).longValue());
    }

    @Test
    public void noArgFunction()
    {
        String scriptText = """
                function hello
                {
                   "hello"
                }

                function bang
                {
                   "!"
                }

                hello() + " there" + bang()""";

        Value result = ExpressionTestUtil.evaluateScript(scriptText);
        assertTrue(result.isString());
        assertEquals("hello there!", result.stringValue());
    }
}
