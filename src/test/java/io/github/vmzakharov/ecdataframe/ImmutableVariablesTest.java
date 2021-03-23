package io.github.vmzakharov.ecdataframe;

import org.junit.Test;

public class ImmutableVariablesTest
{
    @Test(expected = RuntimeException.class)
    public void immutableVariables()
    {
        ExpressionTestUtil.toScript(
                  "a = 1\n"
                + "a = 2").evaluate();
    }
}
