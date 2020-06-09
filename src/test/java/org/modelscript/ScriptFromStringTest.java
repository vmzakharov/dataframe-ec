package org.modelscript;

import org.junit.Assert;
import org.junit.Test;
import org.modelscript.expr.AnonymousScript;
import org.modelscript.expr.value.LongValue;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.InMemoryEvaluationVisitor;
import org.modelscript.util.ExpressionParserHelper;

public class ScriptFromStringTest
{
    @Test
    public void simpleAssignments()
    {
        String scriptText =
                "x = 1\n" +
                "y = 2\n" +
                "z = x + y";
        AnonymousScript script = ExpressionParserHelper.toScript(scriptText);
        Value result = script.evaluate(new InMemoryEvaluationVisitor());
        Assert.assertTrue(result.isInt());
        Assert.assertEquals(3, ((LongValue) result).longValue());
    }

    @Test
    public void simpleAssignmentsWithHangingExpression()
    {
        String scriptText =
                "x= 1\n" +
                "y =2\n" +
                "z=x+ y\n" +
                "3+1 +2";
        AnonymousScript script = ExpressionParserHelper.toScript(scriptText);
        Value result = script.evaluate();
        Assert.assertTrue(result.isInt());
        Assert.assertEquals(6, ((LongValue) result).longValue());
    }
}
