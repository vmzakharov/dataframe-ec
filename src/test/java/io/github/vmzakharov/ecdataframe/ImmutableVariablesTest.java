package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.junit.Test;

public class ImmutableVariablesTest
{
    @Test(expected = RuntimeException.class)
    public void immutableVariables()
    {
        ExpressionParserHelper.toScript(
                "a = 1\n" +
                "a = 2").evaluate();
    }
}
