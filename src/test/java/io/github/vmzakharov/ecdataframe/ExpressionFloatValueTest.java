package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.FloatValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.*;

public class ExpressionFloatValueTest
{
    static private final double TOLERANCE = 0.00000001;
    private EvalContext context;

    @Before
    public void setContextVariables()
    {
        this.context = new SimpleEvalContext();
        this.context.setVariable("x", new FloatValue(4.25f));
        this.context.setVariable("y", new FloatValue(5.5f));
    }

    @Test
    public void addFloats()
    {
        this.assertDoubleResult(9.75, "x + y");
    }

    @Test
    public void addFloatToDouble()
    {
        this.assertDoubleResult(5.75, "x + 1.5");
        this.assertDoubleResult(6.375, "2.125 + x");
    }

    @Test
    public void multiplyFloats()
    {
        this.assertDoubleResult(23.375, "x * y");
    }

    @Test
    public void negativeFloats()
    {
        this.assertFloatResult(-4.25f, "-x");
    }

    @Test
    public void floatComparison()
    {
        scriptEvaluatesToTrue("x < y", this.context);
        scriptEvaluatesToTrue("x <= y", this.context);

        scriptEvaluatesToFalse("x > y", this.context);
        scriptEvaluatesToFalse("x >= y", this.context);

        scriptEvaluatesToTrue("x == x", this.context);

        scriptEvaluatesToFalse("y < x", this.context);
        scriptEvaluatesToFalse("y <= x", this.context);

        scriptEvaluatesToTrue("y > x", this.context);
        scriptEvaluatesToTrue("y >= x", this.context);
    }

    @Test
    public void floatPredicates()
    {
        scriptEvaluatesToFalse("x is null", this.context);
        scriptEvaluatesToTrue("x is not null", this.context);
    }

    private void assertDoubleResult(double expected, String scriptString)
    {
        Value value = evaluateScriptWithContext(scriptString, this.context);

        Assert.assertTrue(value.isDouble());
        Assert.assertEquals(expected, ((DoubleValue) value).doubleValue(), TOLERANCE);
    }

    private void assertFloatResult(float expected, String scriptString)
    {
        Value value = evaluateScriptWithContext(scriptString, this.context);

        Assert.assertTrue(value.isFloat());
        Assert.assertEquals(expected, ((FloatValue) value).floatValue(), TOLERANCE);
    }
}
