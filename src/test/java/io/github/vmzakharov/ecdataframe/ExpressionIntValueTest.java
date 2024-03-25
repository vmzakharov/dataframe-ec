package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.IntValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.*;

public class ExpressionIntValueTest
{
    private EvalContext context;

    @Before
    public void setContextVariables()
    {
        this.context = new SimpleEvalContext();
        this.context.setVariable("x", new IntValue(4));
        this.context.setVariable("y", new IntValue(5));
    }
    
    @Test
    public void addInts()
    {
        this.assertLongResult(9L, "x + y");
    }

    @Test
    public void intAndLong()
    {
        this.assertLongResult(5L, "x + 1");
        this.assertLongResult(5L, "1 + x");
        this.assertLongResult(6L, "2 + x");
        this.assertLongResult(-2L, "2 - x");
        this.assertLongResult(8L, "2 * x");
        this.assertLongResult(8L, "x * 2");
        this.assertLongResult(2L, "x / 2");
        this.assertLongResult(2L, "8 / x");

        scriptEvaluatesToTrue("x > 3", this.context);
        scriptEvaluatesToFalse("x < 3", this.context);
        scriptEvaluatesToTrue("x >= 4", this.context);
        scriptEvaluatesToTrue("x < 5", this.context);
        scriptEvaluatesToFalse("x > 5", this.context);
    }

    @Test
    public void intAndDouble()
    {
        assertDoubleResult(5.0, "x + 1.0", this.context);
        assertDoubleResult(5.0, "1.0 + x", this.context);
        assertDoubleResult(6.0, "2.0 + x", this.context);
        assertDoubleResult(-2.0, "2.0 - x", this.context);
        assertDoubleResult(8.0, "2.0 * x", this.context);
        assertDoubleResult(8.0, "x * 2.0", this.context);
        assertDoubleResult(2.0, "x / 2.0", this.context);
        assertDoubleResult(2.0, "8.0 / x", this.context);

        scriptEvaluatesToTrue("x > 3.0", this.context);
        scriptEvaluatesToFalse("3.0 > x", this.context);
        scriptEvaluatesToFalse("x < 3.0", this.context);
        scriptEvaluatesToTrue("x >= 4.0", this.context);
        scriptEvaluatesToTrue("x < 5.0", this.context);
        scriptEvaluatesToFalse("5.0 < x", this.context);
    }

    @Test
    public void multiplyInts()
    {
        this.assertLongResult(20L, "x * y");
    }

    @Test
    public void negativeInts()
    {
        this.assertIntResult(-4, "-x");
    }

    @Test
    public void intComparison()
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
    public void intPredicates()
    {
        scriptEvaluatesToFalse("x is null", this.context);
        scriptEvaluatesToTrue("x is not null", this.context);
    }

    @Test
    public void intContains()
    {
        this.context.setVariable("z", new IntValue(25));

        scriptEvaluatesToTrue("x in (x, y, z)", this.context);
        scriptEvaluatesToFalse("x in (y, z)", this.context);

        scriptEvaluatesToTrue("x not in (y, z)", this.context);
        scriptEvaluatesToFalse("x not in (y, x, z)", this.context);
    }

    private void assertLongResult(long expected, String scriptString)
    {
        Value value = evaluateScriptWithContext(scriptString, this.context);

        Assert.assertTrue(value.isLong());
        Assert.assertEquals(expected, ((LongValue) value).longValue());
    }

    private void assertIntResult(int expected, String scriptString)
    {
        Value value = evaluateScriptWithContext(scriptString, this.context);

        Assert.assertTrue(value.isInt());
        Assert.assertEquals(expected, ((IntValue) value).intValue());
    }
}
