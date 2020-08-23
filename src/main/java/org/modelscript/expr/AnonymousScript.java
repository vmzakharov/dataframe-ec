package org.modelscript.expr;

import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.visitor.ExpressionEvaluationVisitor;
import org.modelscript.expr.visitor.ExpressionVisitor;

public class AnonymousScript
extends AbstractScript
{
    private final MutableMap<String, FunctionScript> functions = Maps.mutable.of();

    public AnonymousScript()
    {
    }

    @Override
    public Value evaluate(ExpressionEvaluationVisitor visitor)
    {
        return visitor.visitAnonymousScriptExpr(this);
    }

    @Override
    public void accept(ExpressionVisitor visitor)
    {
        visitor.visitAnonymousScriptExpr(this);
    }

    public void addFunctionScript(FunctionScript functionScript)
    {
        this.functions.put(functionScript.getName(), functionScript);
    }

    public MutableMap<String, FunctionScript> getFunctions()
    {
        return this.functions;
    }
}
