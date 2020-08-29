package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dataset.AvroDataSet;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.api.map.MutableMap;

public interface EvalContext
{
    Value setVariable(String newVarName, Value newValue);

    Value getVariable(String newVariableName);

    Value getVariableOrDefault(String newVariableName, Value defaultValue);

    boolean hasVariable(String variableName);

    void removeVariable(String variableName);

    MapIterable<String, FunctionScript> getDeclaredFunctions();

    void setDeclaredFunctions(MutableMap<String, FunctionScript> newDeclaredFunctions);

    void addDataSet(AvroDataSet dataSet);

    AvroDataSet getDataSet(String dataSetName);

    RichIterable<String> getVariableNames();

    void removeAllVariables();
}
