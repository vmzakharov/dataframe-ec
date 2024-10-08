package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;

public record DecimalExpr(Expression unscaledValueExpr, Expression scaleExpr)
implements Expression
{

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
