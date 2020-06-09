package org.modelscript.expr;

import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.ExpressionEvaluationVisitor;
import org.modelscript.expr.visitor.ExpressionVisitor;

public class ConstExpr
implements Expression
{
    private Value value;

    public ConstExpr(Value newValue)
    {
        this.value = newValue;
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor visitor)
    {
        return visitor.visitConstExpr(this);
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitConstExpr(this);
    }

    public Value getValue()
    {
        return this.value;
    }
}
