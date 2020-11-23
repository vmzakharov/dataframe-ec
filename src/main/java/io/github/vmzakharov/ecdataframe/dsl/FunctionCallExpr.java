package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

public class FunctionCallExpr
implements Expression
{
    final private String functionName;
    final private String normalizedFunctionName;
    final private ListIterable<Expression> parameters;

    public FunctionCallExpr(String newFunctionName, ListIterable<Expression> newParameters)
    {
        this.functionName = newFunctionName;
        this.normalizedFunctionName = this.functionName.toUpperCase();
        this.parameters = newParameters;
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

    public String getFunctionName()
    {
        return this.functionName;
    }

    public String getNormalizedFunctionName()
    {
        return this.normalizedFunctionName;
    }

    public ListIterable<Expression> getParameters()
    {
        return this.parameters;
    }
}
