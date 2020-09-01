package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;

public class UnaryExpr
implements Expression
{
    private final UnaryOp operation;
    private final Expression operand;

    public UnaryExpr(UnaryOp newOperation, Expression newOperand)
    {
        this.operand = newOperand;
        this.operation = newOperation;
    }

    public Expression getOperand()
    {
        return this.operand;
    }

    public UnaryOp getOperation()
    {
        return this.operation;
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor evaluationVisitor)
    {
        return evaluationVisitor.visitUnaryExpr(this);
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitUnaryExpr(this);
    }
}
