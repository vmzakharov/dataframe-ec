package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;

public class AssingExpr
implements Expression
{
    private final String varName;
    private final boolean escaped;
    private final Expression expression;

    public AssingExpr(String newVarName, boolean newEscaped, Expression newExpression)
    {
        this.varName = newVarName;
        this.escaped = newEscaped;
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

    public boolean isEscaped()
    {
        return this.escaped;
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
