package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;

public class DecimalExpr
implements Expression
{
    private final Expression unscaledValueExpr;
    private final Expression scaleExpr;

    public DecimalExpr(Expression unscaledValueExpr, Expression scaleExpr)
    {
        this.unscaledValueExpr = unscaledValueExpr;
        this.scaleExpr = scaleExpr;
    }

    public Expression unscaledValueExpr()
    {
        return this.unscaledValueExpr;
    }

    public Expression scaleExpr()
    {
        return this.scaleExpr;
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor evaluationVisitor)
    {
        return evaluationVisitor.visitDecimalExpr(this);
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitDecimalExpr(this);
    }
}
