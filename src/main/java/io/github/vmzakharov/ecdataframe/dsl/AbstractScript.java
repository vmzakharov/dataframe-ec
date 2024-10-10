package io.github.vmzakharov.ecdataframe.dsl;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;

/**
 * An abstract superclass for the {@code Script} hierarchy, adds behavior to keep track of the script's expressions
 * (statements)
 */
abstract public class AbstractScript
implements Script
{
    private final MutableList<Expression> expressions = Lists.mutable.of();

    @Override
    public Expression addExpression(Expression anExpression)
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
