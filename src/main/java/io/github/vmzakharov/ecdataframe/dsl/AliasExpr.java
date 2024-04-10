package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;

public class AliasExpr
implements Expression
{
    private final String alias;
    private final Expression expression;

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
