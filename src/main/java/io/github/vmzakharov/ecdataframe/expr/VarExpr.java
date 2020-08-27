package io.github.vmzakharov.ecdataframe.expr;

import io.github.vmzakharov.ecdataframe.expr.value.Value;
import io.github.vmzakharov.ecdataframe.expr.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.expr.visitor.ExpressionVisitor;

public class VarExpr implements Expression
{
    private String variableName;

    public VarExpr(String newVariableName)
    {
        this.variableName = newVariableName;
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
