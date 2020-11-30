package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class VectorExpressionTest
{
    @Test
    public void vectorVariableIn()
    {
        AnonymousScript script = ExpressionTestUtil.toScript(
                "x = (\"a\", \"b\", \"c\")\n" +
                "y = \"a\"\n" +
                "y in x\n"
        );

        Value result = script.evaluate();
        Assert.assertTrue(((BooleanValue) result).isTrue());
    }

    @Test
    public void index()
    {
        Assert.assertEquals(2L, ExpressionTestUtil.evaluateToLong("(1, 2, 3)[1]"));
    }

    @Test
    public void indexWithVariables()
    {
        AnonymousScript script = ExpressionTestUtil.toScript(
                "x = (\"a\", \"b\", \"c\")\n" +
                "y = 2\n" +
                "x[y]\n"
        );

        Value result = script.evaluate();
        Assert.assertEquals("c", result.stringValue());
    }

    @Test
    public void indexWithFunction()
    {
        AnonymousScript script = ExpressionTestUtil.toScript(
                "function foo(switch)\n" +
                "{\n" +
                "  if switch == 1 then\n" +
                "    (1, 2, 3)\n" +
                "  else\n" +
                "    (4, 5, 6)\n" +
                "  endif\n" +
                "}\n" +
                "\n" +
                "foo(2)[2] + foo(1)[1] "
        );

        Value result = script.evaluate();
        Assert.assertEquals(8L, ((LongValue) result).longValue());
    }

}
