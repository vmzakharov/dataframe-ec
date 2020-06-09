package org.modelscript.expr;

import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.ExpressionEvaluationVisitor;
import org.modelscript.expr.visitor.ExpressionVisitor;

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
