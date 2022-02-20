package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.junit.Assert;
import org.junit.Test;

public class IsEmptyOperationTest
{
    @Test
    public void stringIsEmptyOrNot()
    {
        this.evaluatesToFalse("\"Foo\" is empty");
        this.evaluatesToFalse("\"Foo\" is empty or \"Bar\" is empty");

        this.evaluatesToTrue("\"Foo\" is not empty");
        this.evaluatesToTrue("\"Foo\" is not empty and 2 > 1");

        this.evaluatesToTrue("\"\" is empty");
        this.evaluatesToFalse("\"\" is not empty");

        this.evaluatesToTrue("\"Foo\" is not empty and \"\" is empty");
    }

    @Test
    public void dateIsEmptyOrNot()
    {
        this.evaluatesToFalse("toDate(\"2020-10-06\") is empty");
        this.evaluatesToFalse("toDate(\"2020-10-06\") is empty or \"Bar\" is empty");

        this.evaluatesToTrue("toDate(\"2020-10-06\") is not empty");
    }

    @Test
    public void vectorIsEmptyOrNot()
    {
        this.evaluatesToFalse("(1, 2, 3) is empty");
        this.evaluatesToFalse("(1, 2, 3) is empty or \"Bar\" is empty");

        this.evaluatesToTrue("(1, 2, 3) is not empty");

        this.evaluatesToTrue("() is empty");
        this.evaluatesToFalse("() is not empty");
    }

    @Test
    public void voidValueIsNull()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", Value.VOID);

        ExpressionTestUtil.scriptEvaluatesToTrue("x is null", context);
        ExpressionTestUtil.scriptEvaluatesToFalse("x is not null", context);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void voidValueNotIsNotEmpty()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", Value.VOID);

        ExpressionTestUtil.scriptEvaluatesToFalse("x is not empty", context);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void voidValueNotIsEmpty()
    {
        SimpleEvalContext context = new SimpleEvalContext();
        context.setVariable("x", Value.VOID);

        ExpressionTestUtil.scriptEvaluatesToFalse("x is empty", context);
    }

    public void evaluatesToTrue(String expression)
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean(expression));
    }

    public void evaluatesToFalse(String expression)
    {
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean(expression));
    }
}
