package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;
import org.eclipse.collections.api.list.ListIterable;

public record PropertyPathExpr(ListIterable<String> pathElements)
implements Expression
{
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

    public String getEntityName()
    {
        return this.pathElements.getFirst();
    }

    public String getPropertyChainString()
    {
        return this.pathElements.drop(1).makeString(".");
    }
}
