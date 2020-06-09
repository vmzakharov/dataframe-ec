package org.modelscript.expr;

import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.ExpressionEvaluationVisitor;
import org.modelscript.expr.visitor.ExpressionVisitor;

public class BinaryExpr
implements Expression
{
    private Expression operand1;
    private Expression operand2;

    private BinaryOp operation;

    public BinaryExpr(Expression newOperand1, Expression newOperand2, BinaryOp newOperation)
    {
        this.operand1 = newOperand1;
        this.operand2 = newOperand2;
        this.operation = newOperation;
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor visitor)
    {
        return visitor.visitBinaryExpr(this);
    }

    public Expression getOperand1()
    {
        return this.operand1;
    }

    public Expression getOperand2()
    {
        return this.operand2;
    }

    public BinaryOp getOperation()
    {
        return this.operation;
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitBinaryExpr(this);
    }
}
