package org.modelscript;

import org.junit.Test;
import org.modelscript.util.ExpressionParserHelper;

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
