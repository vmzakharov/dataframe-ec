package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class VectorExpressionTest
{
    @Test
    public void vectorVariableIn()
    {
        Value result = ExpressionTestUtil.evaluateScript(
                """
                x = ("a", "b", "c")
                y = "a"
                y in x
                """
        );

        assertTrue(((BooleanValue) result).isTrue());
    }

    @Test
    public void index()
    {
        assertEquals(2L, ExpressionTestUtil.evaluateToLong("(1, 2, 3)[1]"));
    }

    @Test
    public void indexWithVariables()
    {
        Value result = ExpressionTestUtil.evaluateScript(
                """
                x = ("a", "b", "c")
                y = 2
                x[y]
                """
        );

        assertEquals("c", result.stringValue());
    }

    @Test
    public void indexWithFunction()
    {
        Value result = ExpressionTestUtil.evaluateScript(
                """
                function foo(switch)
                {
                  if switch == 1 then
                    (1, 2, 3)
                  else
                    (4, 5, 6)
                  endif
                }

                foo(2)[2] + foo(1)[1]\s"""
        );

        assertEquals(8L, ((LongValue) result).longValue());
    }

}
