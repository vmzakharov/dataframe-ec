package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ImmutableVariablesTest
{
    @Test
    public void immutableVariables()
    {
        Exception exception = assertThrows(
                RuntimeException.class,
                () -> ExpressionTestUtil.evaluateScript(
                        "a = 1\n"
                        + "a = 2")
                );

        Assertions.assertEquals(
                FormatWithPlaceholders.messageFromKey("DSL_VAR_IMMUTABLE").with("variableName", "a").toString(),
                exception.getMessage()
        );
    }
}
