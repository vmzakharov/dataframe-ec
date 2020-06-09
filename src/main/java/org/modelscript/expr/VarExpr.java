package org.modelscript.expr;

import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.ExpressionEvaluationVisitor;
import org.modelscript.expr.visitor.ExpressionVisitor;

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
