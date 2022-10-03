package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.AssingExpr;
import io.github.vmzakharov.ecdataframe.dsl.BinaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.FunctionCallExpr;
import io.github.vmzakharov.ecdataframe.dsl.ProjectionExpr;
import io.github.vmzakharov.ecdataframe.dsl.Script;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.UnaryExpr;
import io.github.vmzakharov.ecdataframe.dsl.UnaryOp;
import io.github.vmzakharov.ecdataframe.dsl.VectorExpr;
import io.github.vmzakharov.ecdataframe.dsl.value.LongValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import org.eclipse.collections.impl.factory.Lists;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class SimpleExpressionParsingTest
{
    @Test
    public void negativeNumber()
    {
        Expression expression = ExpressionTestUtil.toExpression("-1\n");

        Assert.assertEquals(UnaryExpr.class, expression.getClass());
        UnaryExpr unaryExpr = (UnaryExpr) expression;
        Assert.assertEquals(UnaryOp.MINUS, unaryExpr.getOperation());
    }

    @Test
    public void assignment()
    {
        Expression expression = ExpressionTestUtil.toExpression("a = 5\n");
        Assert.assertEquals("a", ((AssingExpr) expression).getVarName());

        EvalContext evalContext = new SimpleEvalContext();

        LongValue result = (LongValue) expression.evaluate(new InMemoryEvaluationVisitor(evalContext));

        Assert.assertEquals(5, result.longValue());

        Assert.assertEquals(5, ((LongValue) evalContext.getVariable("a")).longValue());
    }

    @Test
    public void functionCall()
    {
        Expression expression = ExpressionTestUtil.toExpression("x(1, y * 7)");

        Assert.assertEquals(FunctionCallExpr.class, expression.getClass());

        FunctionCallExpr fcExpr = (FunctionCallExpr) expression;

        Assert.assertEquals("x", fcExpr.getFunctionName());
        Expression param1 = fcExpr.getParameters().get(0);
        Expression param2 = fcExpr.getParameters().get(1);

        Assert.assertEquals(LongValue.class, param1.getClass());
        Assert.assertEquals(BinaryExpr.class, param2.getClass());
    }

    @Test
    public void noArgFunctionCall()
    {
        Expression expression = ExpressionTestUtil.toExpression("fff()");

        Assert.assertEquals(FunctionCallExpr.class, expression.getClass());

        FunctionCallExpr fcExpr = (FunctionCallExpr) expression;

        Assert.assertEquals("fff", fcExpr.getFunctionName());
        Assert.assertEquals(0, fcExpr.getParameters().size());
    }

    @Test
    public void projection()
    {
        Script script = ExpressionTestUtil.toScript("project {Foo.bar, Foo.baz} where Foo.qux > 7");

        Expression expression = script.getExpressions().get(0);

        Assert.assertEquals(ProjectionExpr.class, expression.getClass());

        ProjectionExpr projectionExpr = (ProjectionExpr) expression;

        Assert.assertEquals(2, projectionExpr.getProjectionElements().size());
        Assert.assertNotNull(projectionExpr.getWhereClause());
    }

    @Test
    public void projectionWithAlias()
    {
        Script script = ExpressionTestUtil.toScript(
                  "project {\n"
                + "barbar : Foo.bar, "
                + "bazbaz : Foo.baz"
                + "} where Foo.qux > 7");

        Expression expression = script.getExpressions().get(0);

        Assert.assertEquals(ProjectionExpr.class, expression.getClass());

        ProjectionExpr projectionExpr = (ProjectionExpr) expression;

        Assert.assertEquals(2, projectionExpr.getProjectionElements().size());
        Assert.assertEquals(Lists.mutable.of("barbar", "bazbaz"), projectionExpr.getElementNames());
        Assert.assertNotNull(projectionExpr.getWhereClause());
    }

    @Test
    public void vectorExpression()
    {
        Script script = ExpressionTestUtil.toScript("(\"a\", \"b\", \"c\")");

        Expression expression = script.getExpressions().get(0);

        Assert.assertEquals(VectorExpr.class, expression.getClass());

        VectorExpr vectorExpr = (VectorExpr) expression;

        Assert.assertEquals(3, vectorExpr.getElements().size());

        Assert.assertEquals(
                Lists.immutable.of("\"a\"", "\"b\"", "\"c\""),
                vectorExpr.getElements().collect(e -> ((Value) e).asStringLiteral()));
    }

    @Test
    @Ignore
    public void singleElementVectorExpression()
    {
        // TODO: remove ambiguous syntax for single element arrays
        Script script = ExpressionTestUtil.toScript("('a')");

        Expression expression = script.getExpressions().get(0);

        Assert.assertEquals(VectorExpr.class, expression.getClass());

        VectorExpr vectorExpr = (VectorExpr) expression;

        Assert.assertEquals(3, vectorExpr.getElements().size());

        Assert.assertEquals(
                Lists.immutable.of("\"a\""),
                vectorExpr.getElements().collect(e -> ((Value) e).asStringLiteral()));
    }
}
