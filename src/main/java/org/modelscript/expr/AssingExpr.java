package org.modelscript.expr;

import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.ExpressionEvaluationVisitor;
import org.modelscript.expr.visitor.ExpressionVisitor;

public class AssingExpr
implements Expression
{
    private final String varName;
    private final Expression expression;

    public AssingExpr(String newVarName, Expression newExpression)
    {
        this.varName = newVarName;
        this.expression = newExpression;
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor visitor)
    {
        return visitor.visitAssignExpr(this);
    }

    public String getVarName()
    {
        return this.varName;
    }

    public Expression getExpression()
    {
        return this.expression;
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitAssignExpr(this);
    }
}
