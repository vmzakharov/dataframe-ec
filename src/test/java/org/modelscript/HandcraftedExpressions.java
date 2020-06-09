package org.modelscript;

import org.junit.Before;
import org.modelscript.expr.*;
import org.modelscript.expr.value.DoubleValue;
import org.modelscript.expr.value.LongValue;
import org.modelscript.expr.value.StringValue;
import org.modelscript.expr.value.Value;
import org.junit.Assert;
import org.junit.Test;
import org.modelscript.expr.visitor.InMemoryEvaluationVisitor;

public class HandcraftedExpressions
{
    private InMemoryEvaluationVisitor evaluationVisitor;

    @Before
    public void setEvalContext()
    {
        this.evaluationVisitor = new InMemoryEvaluationVisitor();
    }

    @Test
    public void intAddition()
    {
        Value result = new BinaryExpr(
                new ConstExpr(new LongValue(123)),
                new ConstExpr(new LongValue(456)),
                ArithmeticOp.ADD
        ).evaluate(this.evaluationVisitor);

        Assert.assertEquals(579, ((LongValue) result).longValue());
    }

    @Test
    public void doubleAddition()
    {
        Value result = new BinaryExpr(
                new ConstExpr(new DoubleValue(123.321)),
                new ConstExpr(new DoubleValue(456.025)),
                ArithmeticOp.ADD
        ).evaluate(this.evaluationVisitor);

        Assert.assertEquals(579.346, ((DoubleValue) result).doubleValue(), 0.0000000001);
    }

    @Test
    public void stringAddition()
    {
        Value result = new BinaryExpr(
                new ConstExpr(new StringValue("Oompa")),
                new ConstExpr(new StringValue("Loompa")),
                ArithmeticOp.ADD
        ).evaluate(this.evaluationVisitor);

        Assert.assertEquals("OompaLoompa", ((StringValue) result).stringValue());
    }
}
