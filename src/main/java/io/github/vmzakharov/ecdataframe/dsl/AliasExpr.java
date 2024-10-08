package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;

public record AliasExpr(String alias, Expression expression)
implements Expression
{

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
}
