package org.modelscript.expr;

import org.eclipse.collections.api.list.ListIterable;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.ExpressionEvaluationVisitor;
import org.modelscript.expr.visitor.ExpressionVisitor;

public class FunctionScript
extends AbstractScript
implements FunctionDescriptor
{
    private final String name;

    private final ListIterable<String> parameterNames;

    public FunctionScript(String newName, ListIterable<String> newParameterNames)
    {
        this.name = newName;
        this.parameterNames = newParameterNames;
    }

    public String getName()
    {
        return this.name;
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitFunctionScriptExpr(this);
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor evaluationVisitor)
    {
        return evaluationVisitor.visitFunctionScriptExpr(this);
    }

    public ListIterable<String> getParameterNames()
    {
        return this.parameterNames;
    }
}