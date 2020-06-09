package org.modelscript.expr;

import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;
import org.modelscript.dataset.AvroDataSet;
import org.modelscript.expr.value.Value;

public class SimpleEvalContext
implements EvalContext
{
    private MutableMap<String, FunctionScript> declaredFunctions = Maps.mutable.of();
    private final MutableMap<String, AvroDataSet> dataSets = Maps.mutable.of();
    private final MutableMap<String, Value> variables = Maps.mutable.of();

    @Override
    public boolean hasVariable(String variableName)
    {
        return this.variables.containsKey(variableName);
    }

    @Override
    public void removeVariable(String variableName)
    {
        this.variables.remove(variableName);
    }

    @Override
    public void removeAllVariables()
    {
        this.variables.clear();
    }

    @Override
    public Value setVariable(String variableName, Value newValue)
    {
        if (this.hasVariable(variableName))
        {
            throw new RuntimeException("Attempting to change immutable variable '" + variableName + "'");
        }
        this.variables.put(variableName, newValue);
        return newValue;
    }

    @Override
    public Value getVariable(String newVariableName)
    {
        Value value = this.variables.get(newVariableName);

        if (value == null)
        {
            throw new RuntimeException("Uninitialized variable: " + newVariableName);
        }

        return value;
    }

    @Override
    public Value getVariableOrDefault(String newVariableName, Value defaultValue)
    {
        Value value = this.variables.get(newVariableName);

        return (value == null) ? defaultValue : value;
    }

    @Override
    public void addDataSet(AvroDataSet dataSet)
    {
        this.dataSets.put(dataSet.getName(), dataSet);
    }

    @Override
    public AvroDataSet getDataSet(String dataSetName)
    {
        return this.dataSets.get(dataSetName);
    }

    @Override
    public RichIterable<String> getVariableNames()
    {
        return this.variables.keysView();
    }

    @Override
    public MapIterable<String, FunctionScript> getDeclaredFunctions()
    {
        return this.declaredFunctions;
    }

    public void loadFunctionsExcept(MapIterable<String, FunctionScript> functionsFromOutside, String nameToExclude)
    {
        functionsFromOutside.forEachKeyValue((k, v) -> {
            if (!k.equals(nameToExclude))
            {
                declaredFunctions.put(k, v);
            }
        });
    }

    @Override
    public void setDeclaredFunctions(MutableMap<String, FunctionScript> newDeclaredFunctions)
    {
        this.declaredFunctions = newDeclaredFunctions;
    }
}
