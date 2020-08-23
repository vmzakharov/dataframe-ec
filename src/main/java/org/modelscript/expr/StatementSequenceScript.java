package org.modelscript.expr;

import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.ExpressionEvaluationVisitor;
import org.modelscript.expr.visitor.ExpressionVisitor;

public class StatementSequenceScript
extends AbstractScript
{

    @Override
    public Value evaluate(ExpressionEvaluationVisitor evaluationVisitor)
    {
        return evaluationVisitor.visitStatementSequenceScriptExpr(this);
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitStatementSequenceScript(this);
    }
}
