package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;

public record IfElseExpr(Expression condition, Expression ifScript, Expression elseScript, boolean isTernary)
implements Expression
{
    public IfElseExpr(Expression condition, Expression ifScript, Expression elseScript)
    {
        this(condition, ifScript, elseScript, false);
    }

    public IfElseExpr(Expression condition, Script ifScript)
    {
        this(condition, ifScript, null, false);
    }

    public boolean hasElseSection()
    {
        return this.elseScript != null;
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor evaluationVisitor)
    {
        return evaluationVisitor.visitIfElseExpr(this);
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitIfElseExpr(this);
    }
}
