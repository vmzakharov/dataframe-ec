package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dataset.HierarchicalDataSet;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public class SimpleEvalContext
extends EvalContextAbstract
{
    private final MutableMap<String, HierarchicalDataSet> dataSets = Maps.mutable.of();

    @Override
    public Value getVariable(String newVariableName)
    {
        Value value = this.getContextVariables().get(newVariableName);

        if (value == null)
        {
            exceptionByKey("DSL_VAR_UNINITIALIZED").with("variableName", newVariableName).fire();
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
    public void addDataSet(HierarchicalDataSet dataSet)
    {
        this.dataSets.put(dataSet.getName(), dataSet);
    }

    @Override
    public HierarchicalDataSet getDataSet(String dataSetName)
    {
        return this.dataSets.get(dataSetName);
    }

    @Override
    public RichIterable<String> getVariableNames()
    {
        return this.getContextVariables().keysView();
    }
}
