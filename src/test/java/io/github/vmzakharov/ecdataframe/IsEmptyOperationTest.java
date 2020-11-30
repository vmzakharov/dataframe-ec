package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.AnonymousScript;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
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
    public void dateNullIsEmpty()
    {
        InMemoryEvaluationVisitor evaluationVisitor = new InMemoryEvaluationVisitor();
        evaluationVisitor.getContext().setVariable("x", new DateValue(null));

        AnonymousScript script = ExpressionTestUtil.toScript("x is empty");
        Value result = script.evaluate(evaluationVisitor);

        Assert.assertTrue(((BooleanValue) result).isTrue());

        script = ExpressionTestUtil.toScript("x is not empty");
        result = script.evaluate(evaluationVisitor);
        Assert.assertFalse(((BooleanValue) result).isTrue());
    }

    @Test
    public void stringNullIsEmpty()
    {
        InMemoryEvaluationVisitor evaluationVisitor = new InMemoryEvaluationVisitor();
        evaluationVisitor.getContext().setVariable("x", new StringValue(null));

        AnonymousScript script = ExpressionTestUtil.toScript("x is empty");
        Value result = script.evaluate(evaluationVisitor);

        Assert.assertTrue(((BooleanValue) result).isTrue());

        script = ExpressionTestUtil.toScript("x is not empty");
        result = script.evaluate(evaluationVisitor);
        Assert.assertFalse(((BooleanValue) result).isTrue());
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
