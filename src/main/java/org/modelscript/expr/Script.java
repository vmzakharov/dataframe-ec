package org.modelscript.expr;

import org.eclipse.collections.api.list.ListIterable;

public interface Script
extends Expression
{
    Expression addStatement(Expression anExpression);

    ListIterable<Expression> getExpressions();
}
