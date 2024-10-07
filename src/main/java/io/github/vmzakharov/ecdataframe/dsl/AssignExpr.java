package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;

public record AssignExpr(String varName, boolean isEscaped, Expression expression)
implements Expression
{
    @Override
    public Value evaluate(ExpressionEvaluationVisitor visitor)
    {
        return visitor.visitAssignExpr(this);
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitAssignExpr(this);
    }
}
