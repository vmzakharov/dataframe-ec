package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.ArithmeticOp;
import io.github.vmzakharov.ecdataframe.dsl.BinaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class HandcraftedExpressionTreeTest
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
                new LongValue(123),
                new LongValue(456),
                ArithmeticOp.ADD
        ).evaluate(this.evaluationVisitor);

        Assert.assertEquals(579, ((LongValue) result).longValue());
    }

    @Test
    public void doubleAddition()
    {
        Value result = new BinaryExpr(
                new DoubleValue(123.321),
                new DoubleValue(456.025),
                ArithmeticOp.ADD
        ).evaluate(this.evaluationVisitor);

        Assert.assertEquals(579.346, ((DoubleValue) result).doubleValue(), 0.0000000001);
    }

    @Test
    public void stringAddition()
    {
        Value result = new BinaryExpr(
                new StringValue("Oompa"),
                new StringValue("Loompa"),
                ArithmeticOp.ADD
        ).evaluate(this.evaluationVisitor);

        Assert.assertEquals("OompaLoompa", ((StringValue) result).stringValue());
    }
}
