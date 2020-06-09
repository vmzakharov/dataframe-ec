package org.modelscript.expr;

import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.ExpressionEvaluationVisitor;
import org.modelscript.expr.visitor.ExpressionVisitor;

public class AliasExpr
implements Expression
{
    private String alias;
    private Expression expression;

    public AliasExpr(String newAlias, Expression newExpression)
    {
        this.alias = newAlias;
        this.expression = newExpression;
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor evaluationVisitor)
    {
        return this.expression.evaluate(evaluationVisitor);
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitAliasExpr(this);
    }

    public String getAlias()
    {
        return this.alias;
    }

    public Expression getExpression()
    {
        return this.expression;
    }
}
