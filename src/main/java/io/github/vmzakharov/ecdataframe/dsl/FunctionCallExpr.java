package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

public record FunctionCallExpr(String functionName, String normalizedFunctionName, ListIterable<Expression> parameters)
implements Expression
{
    public FunctionCallExpr(String functionName, ListIterable<Expression> parameters)
    {
        this(functionName, functionName.toUpperCase(), parameters);
    }

    public FunctionCallExpr(String newFunctionName)
    {
        this(newFunctionName, Lists.immutable.of());
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor visitor)
    {
        return visitor.visitFunctionCallExpr(this);
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitFunctionCallExpr(this);
    }
}
