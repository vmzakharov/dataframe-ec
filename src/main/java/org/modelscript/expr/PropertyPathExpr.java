package org.modelscript.expr;

import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.list.ListIterable;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.ExpressionEvaluationVisitor;
import org.modelscript.expr.visitor.ExpressionVisitor;

public class PropertyPathExpr
implements Expression
{
    private ListIterable<String> pathElements;

    public PropertyPathExpr(ListIterable<String> pathElements)
    {
        this.pathElements = pathElements;
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor visitor)
    {
        return visitor.visitPropertyPathExpr(this);
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitPropertyPathExpr(this);
    }

    public RichIterable<String> getPathElements()
    {
        return this.pathElements;
    }

    public String getEntityName()
    {
        return this.pathElements.get(0);
    }

    public String getPropertyChainString()
    {
        return this.pathElements.drop(1).makeString(".");
    }
}
