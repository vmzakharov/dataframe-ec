package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;

public class IfElseExpr
implements Expression
{
    private final Expression condition;
    private final Expression ifScript;
    private final Expression elseScript;
    private final boolean ternary;

    public IfElseExpr(Expression newCondition, Expression newIfScript, Expression newElseScript)
    {
        this(newCondition, newIfScript, newElseScript, false);
    }

    public IfElseExpr(Expression newCondition, Expression newIfScript, Expression newElseScript, boolean newTernary)
    {
        this.condition = newCondition;
        this.ifScript = newIfScript;
        this.elseScript = newElseScript;
        this.ternary = newTernary;
    }

    public IfElseExpr(Expression newCondition, Script newIfScript)
    {
        this.condition = newCondition;
        this.ifScript = newIfScript;
        this.elseScript = null;
        this.ternary = false;
    }

    public Expression getCondition()
    {
        return this.condition;
    }

    public Expression getIfScript()
    {
        return this.ifScript;
    }

    public Expression getElseScript()
    {
        return this.elseScript;
    }

    public boolean hasElseSection()
    {
        return this.elseScript != null;
    }

    public boolean isTernary()
    {
        return this.ternary;
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
