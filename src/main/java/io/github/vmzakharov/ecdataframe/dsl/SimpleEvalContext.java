package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dataset.AvroDataSet;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;

public class SimpleEvalContext
extends EvalContextAbstract
{
    private final MutableMap<String, AvroDataSet> dataSets = Maps.mutable.of();

    @Override
    public Value getVariable(String newVariableName)
    {
        Value value = this.getContextVariables().get(newVariableName);

        if (value == null)
        {
            throw new RuntimeException("Uninitialized variable: " + newVariableName);
        }

        return value;
    }

    @Override
    public Value getVariableOrDefault(String newVariableName, Value defaultValue)
    {
        Value value = this.getContextVariables().get(newVariableName);

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
        return this.getContextVariables().keysView();
    }
}
