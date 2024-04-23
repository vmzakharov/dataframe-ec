package io.github.vmzakharov.ecdataframe.dsl;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;

abstract public class AbstractScript
implements Script
{
    private final MutableList<Expression> expressions = Lists.mutable.of();

    @Override
    public Expression addStatement(Expression anExpression)
    {
        this.expressions.add(anExpression);
        return anExpression;
    }

    @Override
    public ListIterable<Expression> getExpressions()
    {
        return this.expressions;
    }
}
