package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;

public class VarExpr implements Expression
{
    private final String variableName;
    private final boolean escaped;

    public VarExpr(String newVariableName, boolean newEscaped)
    {
        this.variableName = newVariableName;
        this.escaped = newEscaped;
    }

    public boolean isEscaped()
    {
        return this.escaped;
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor visitor)
    {
        return visitor.visitVarExpr(this);
    }

    public String getVariableName()
    {
        return this.variableName;
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitVarExpr(this);
    }
}
