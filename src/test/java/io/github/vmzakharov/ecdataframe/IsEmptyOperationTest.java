package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import org.junit.jupiter.api.Test;

import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.scriptEvaluatesToFalse;
import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.scriptEvaluatesToTrue;
import static org.junit.jupiter.api.Assertions.*;

public class IsEmptyOperationTest
{
    @Test
    public void stringIsEmptyOrNot()
    {
        scriptEvaluatesToFalse("\"Foo\" is empty");
        scriptEvaluatesToFalse("\"Foo\" is empty or \"Bar\" is empty");

        scriptEvaluatesToTrue("\"Foo\" is not empty");
        scriptEvaluatesToTrue("\"Foo\" is not empty and 2 > 1");

        scriptEvaluatesToTrue("\"\" is empty");
        scriptEvaluatesToFalse("\"\" is not empty");

        scriptEvaluatesToTrue("\"Foo\" is not empty and \"\" is empty");
    }

    @Test
    public void dateIsEmptyOrNot()
    {
        scriptEvaluatesToFalse("toDate(\"2020-10-06\") is empty");
        scriptEvaluatesToFalse("toDate(\"2020-10-06\") is empty or \"Bar\" is empty");

        scriptEvaluatesToTrue("toDate(\"2020-10-06\") is not empty");
    }

    @Test
    public void vectorIsEmptyOrNot()
    {
        scriptEvaluatesToFalse("(1, 2, 3) is empty");
        scriptEvaluatesToFalse("(1, 2, 3) is empty or \"Bar\" is empty");

        scriptEvaluatesToTrue("(1, 2, 3) is not empty");

        scriptEvaluatesToTrue("() is empty");
        scriptEvaluatesToFalse("() is not empty");
    }

    @Test
    public void voidValueIsNull()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", Value.VOID);

        scriptEvaluatesToTrue("x is null", context);
        scriptEvaluatesToFalse("x is not null", context);
    }

    @Test
    public void twoVoidValuesInExpression()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", Value.VOID);
        context.setVariable("y", Value.VOID);

        scriptEvaluatesToTrue("x is null and y is null", context);
        scriptEvaluatesToTrue("x is null or y is null", context);
        scriptEvaluatesToFalse("x is not null and y is not null", context);
        scriptEvaluatesToFalse("x is not null or y is not null", context);
    }

    @Test
    public void voidAndNotVoidValuesInExpression()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", Value.VOID);
        context.setVariable("z", new StringValue("hello"));

        scriptEvaluatesToFalse("x is null and z is null", context);
        scriptEvaluatesToTrue("x is null or z is null", context);
        scriptEvaluatesToFalse("x is not null and z is not null", context);
        scriptEvaluatesToTrue("x is not null or z is not null", context);
    }

    @Test
    public void voidValueNotIsNotEmpty()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", Value.VOID);

        assertThrows(
                UnsupportedOperationException.class,
                () -> scriptEvaluatesToFalse("x is not empty", context)
        );
    }

    @Test
    public void voidValueNotIsEmpty()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", Value.VOID);

        assertThrows(
                UnsupportedOperationException.class,
                () -> scriptEvaluatesToFalse("x is empty", context)
        );
    }
}
