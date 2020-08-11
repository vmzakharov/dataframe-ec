package org.modelscript.expr;

import org.eclipse.collections.api.list.ListIterable;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.ExpressionEvaluationVisitor;
import org.modelscript.expr.visitor.ExpressionVisitor;

public class VectorExpr
implements Expression
{
    final private ListIterable<Expression> elements;

    public VectorExpr(ListIterable<Expression> newElements)
    {
        this.elements = newElements;
    }

    public ListIterable<Expression> getElements()
    {
        return this.elements;
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor evaluationVisitor)
    {
        return evaluationVisitor.visitVectorExpr(this);
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitVectorExpr(this);
    }
}
