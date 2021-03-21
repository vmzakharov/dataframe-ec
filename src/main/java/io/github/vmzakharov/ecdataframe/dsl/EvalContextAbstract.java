package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;

abstract public class EvalContextAbstract
implements EvalContext
{
    private MutableMap<String, FunctionScript> declaredFunctions = Maps.mutable.of();
    private final MutableMap<String, Value> contextVariables = Maps.mutable.of();

    @Override
    public boolean hasVariable(String variableName)
    {
        return this.contextVariables.containsKey(variableName);
    }

    @Override
    public void removeVariable(String variableName)
    {
        this.contextVariables.remove(variableName);
    }

    @Override
    public void removeAllVariables()
    {
        this.contextVariables.clear();
    }

    protected MutableMap<String, Value> getContextVariables()
    {
        return this.contextVariables;
    }

    @Override
    public Value setVariable(String variableName, Value newValue)
    {
        if (this.hasVariable(variableName))
        {
            throw new RuntimeException("Attempting to change immutable variable '" + variableName + "'");
        }
        this.getContextVariables().put(variableName, newValue);
        return newValue;
    }

    @Override
    public MapIterable<String, FunctionScript> getDeclaredFunctions()
    {
        return this.declaredFunctions;
    }

    @Override
    public void setDeclaredFunctions(MutableMap<String, FunctionScript> newDeclaredFunctions)
    {
        this.declaredFunctions = newDeclaredFunctions;
    }

    @Override
    public FunctionScript getDeclaredFunction(String functionName)
    {
        return this.declaredFunctions.get(functionName);
    }

    // helps avoid recursion
    public void loadFunctionsExcept(MapIterable<String, FunctionScript> functionsFromOutside, String nameToExclude)
    {
        functionsFromOutside.forEachKeyValue((k, v) -> {
            if (!k.equals(nameToExclude))
            {
                this.declaredFunctions.put(k, v);
            }
        });
    }
}
