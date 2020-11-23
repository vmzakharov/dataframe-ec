package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionVisitor;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;

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
        this.functions.put(functionScript.getNormalizedName(), functionScript);
    }

    public MutableMap<String, FunctionScript> getFunctions()
    {
        return this.functions;
    }
}
