package org.modelscript.expr;

import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.ExpressionEvaluationVisitor;
import org.modelscript.expr.visitor.ExpressionVisitor;

public interface Expression
{
    Value evaluate(ExpressionEvaluationVisitor evaluationVisitor);

    void accept(ExpressionVisitor visitor);
}
