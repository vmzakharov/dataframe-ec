package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.FloatValue;
import io.github.vmzakharov.ecdataframe.dsl.value.IntValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;

import static io.github.vmzakharov.ecdataframe.ExpressionTestUtil.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ExpressionFloatValueTest
{
    static private final double TOLERANCE = 0.00000001;
    private EvalContext context;

    @BeforeEach
    public void setContextVariables()
    {
        this.context = new SimpleEvalContext();
        this.context.setVariable("x", new FloatValue(4.25f));
        this.context.setVariable("y", new FloatValue(5.5f));
    }

    @Test
    public void addFloats()
    {
        assertDoubleResult(9.75, "x + y", this.context);
    }

    @Test
    public void floatAndDouble()
    {
        assertDoubleResult(5.75, "x + 1.5", this.context);
        assertDoubleResult(6.375, "2.125 + x", this.context);

        scriptEvaluatesToTrue("x > 4.0", this.context);
        scriptEvaluatesToFalse("x < 4.0", this.context);
    }

    @Test
    public void floatAndLong()
    {
        assertDoubleResult(8.25, "x + 4", this.context);
        assertDoubleResult(8.25, "4 + x", this.context);
        assertDoubleResult(-2.25, "2 - x", this.context);
        assertDoubleResult(2.25, "x - 2", this.context);
        assertDoubleResult(8.5, "x * 2", this.context);
        assertDoubleResult(2.125, "x / 2", this.context);

        scriptEvaluatesToTrue("x > 4", this.context);
        scriptEvaluatesToFalse("x < 4", this.context);
    }

    @Test
    public void floatAndInt()
    {
        this.context.setVariable("i", new IntValue(4));
        this.context.setVariable("seventeen", new IntValue(17));

        assertDoubleResult(8.25, "x + i", this.context);
        assertDoubleResult(8.25, "i + x", this.context);
        assertDoubleResult(12.75, "seventeen - x", this.context);
        assertDoubleResult(-12.75, "x - seventeen", this.context);
        assertDoubleResult(17.0, "x * i", this.context);
        assertDoubleResult(17.0, "i * x", this.context);
        assertDoubleResult(1.0625, "x / i", this.context);
        assertDoubleResult(4.0, "seventeen / x", this.context);

        scriptEvaluatesToTrue("x > i", this.context);
        scriptEvaluatesToFalse("x < i", this.context);
    }

    @Test
    public void multiplyFloats()
    {
        assertDoubleResult(23.375, "x * y", this.context);
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

    @Test
    public void floatContains()
    {
        this.context.setVariable("z", new FloatValue(25.25f));

        scriptEvaluatesToTrue("x in (x, y, z)", this.context);
        scriptEvaluatesToFalse("x in (y, z)", this.context);

        scriptEvaluatesToTrue("x not in (y, z)", this.context);
        scriptEvaluatesToFalse("x not in (y, x, z)", this.context);
    }

    private void assertFloatResult(float expected, String scriptString)
    {
        Value value = evaluateScriptWithContext(scriptString, this.context);

        assertTrue(value.isFloat());
        assertEquals(expected, ((FloatValue) value).floatValue(), TOLERANCE);
    }
}
