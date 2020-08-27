package io.github.vmzakharov.ecdataframe.expr;

import io.github.vmzakharov.ecdataframe.expr.value.Value;
import io.github.vmzakharov.ecdataframe.expr.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.expr.visitor.ExpressionVisitor;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

public class FunctionCallExpr
implements Expression
{
    final private String functionName;
    final private ListIterable<Expression> parameters;

    public FunctionCallExpr(String newFunctionName, ListIterable<Expression> newParameters)
    {
        this.functionName = newFunctionName;
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

    public ListIterable<Expression> getParameters()
    {
        return this.parameters;
    }
}
