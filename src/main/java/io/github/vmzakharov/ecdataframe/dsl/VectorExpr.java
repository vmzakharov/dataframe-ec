package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;
import org.eclipse.collections.api.list.ListIterable;

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
