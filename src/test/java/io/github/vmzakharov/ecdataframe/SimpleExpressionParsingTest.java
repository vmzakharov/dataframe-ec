package io.github.vmzakharov.ecdataframe;

import io.github.vmzakharov.ecdataframe.dsl.AssignExpr;
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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SimpleExpressionParsingTest
{
    @Test
    public void negativeNumber()
    {
        Expression expression = ExpressionTestUtil.toExpression("-1\n");

        assertEquals(UnaryExpr.class, expression.getClass());
        UnaryExpr unaryExpr = (UnaryExpr) expression;
        assertEquals(UnaryOp.MINUS, unaryExpr.operation());
    }

    @Test
    public void assignment()
    {
        Expression expression = ExpressionTestUtil.toExpression("a = 5\n");
        assertEquals("a", ((AssignExpr) expression).varName());

        EvalContext evalContext = new SimpleEvalContext();

        LongValue result = (LongValue) expression.evaluate(new InMemoryEvaluationVisitor(evalContext));

        assertEquals(5, result.longValue());

        assertEquals(5, ((LongValue) evalContext.getVariable("a")).longValue());
    }

    @Test
    public void functionCall()
    {
        Expression expression = ExpressionTestUtil.toExpression("x(1, y * 7)");

        assertEquals(FunctionCallExpr.class, expression.getClass());

        FunctionCallExpr fcExpr = (FunctionCallExpr) expression;

        assertEquals("x", fcExpr.functionName());
        Expression param1 = fcExpr.parameters().get(0);
        Expression param2 = fcExpr.parameters().get(1);

        assertEquals(LongValue.class, param1.getClass());
        assertEquals(BinaryExpr.class, param2.getClass());
    }

    @Test
    public void noArgFunctionCall()
    {
        Expression expression = ExpressionTestUtil.toExpression("fff()");

        assertEquals(FunctionCallExpr.class, expression.getClass());

        FunctionCallExpr fcExpr = (FunctionCallExpr) expression;

        assertEquals("fff", fcExpr.functionName());
        assertEquals(0, fcExpr.parameters().size());
    }

    @Test
    public void projection()
    {
        Script script = ExpressionTestUtil.toScript("project {Foo.bar, Foo.baz} where Foo.qux > 7");

        Expression expression = script.getExpressions().getFirst();

        assertEquals(ProjectionExpr.class, expression.getClass());

        ProjectionExpr projectionExpr = (ProjectionExpr) expression;

        assertEquals(2, projectionExpr.projectionElements().size());
        assertNotNull(projectionExpr.whereClause());
    }

    @Test
    public void projectionWithAlias()
    {
        Script script = ExpressionTestUtil.toScript(
                  "project {\n"
                + "barbar : Foo.bar, "
                + "bazbaz : Foo.baz"
                + "} where Foo.qux > 7");

        Expression expression = script.getExpressions().getFirst();

        assertEquals(ProjectionExpr.class, expression.getClass());

        ProjectionExpr projectionExpr = (ProjectionExpr) expression;

        assertEquals(2, projectionExpr.projectionElements().size());
        assertEquals(Lists.mutable.of("barbar", "bazbaz"), projectionExpr.elementNames());
        assertNotNull(projectionExpr.whereClause());
    }

    @Test
    public void vectorExpression()
    {
        Script script = ExpressionTestUtil.toScript("(\"a\", \"b\", \"c\")");

        Expression expression = script.getExpressions().getFirst();

        assertEquals(VectorExpr.class, expression.getClass());

        VectorExpr vectorExpr = (VectorExpr) expression;

        assertEquals(3, vectorExpr.elements().size());

        assertEquals(
                Lists.immutable.of("\"a\"", "\"b\"", "\"c\""),
                vectorExpr.elements().collect(e -> ((Value) e).asStringLiteral()));
    }

    @Test
    @Disabled
    public void singleElementVectorExpression()
    {
        // TODO: remove ambiguous syntax for single element arrays
        Script script = ExpressionTestUtil.toScript("('a')");

        Expression expression = script.getExpressions().getFirst();

        assertEquals(VectorExpr.class, expression.getClass());

        VectorExpr vectorExpr = (VectorExpr) expression;

        assertEquals(3, vectorExpr.elements().size());

        assertEquals(
                Lists.immutable.of("\"a\""),
                vectorExpr.elements().collect(e -> ((Value) e).asStringLiteral()));
    }
}
