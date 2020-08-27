package io.github.vmzakharov.ecdataframe.expr;

import io.github.vmzakharov.ecdataframe.expr.value.Value;
import io.github.vmzakharov.ecdataframe.expr.visitor.InMemoryEvaluationVisitor;
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

    public Value evaluate()
    {
        return this.evaluate(new InMemoryEvaluationVisitor());
    }

    @Override
    public ListIterable<Expression> getExpressions()
    {
        return this.expressions;
    }
}
