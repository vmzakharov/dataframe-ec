package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataset.HierarchicalDataSet;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public class DataFrameEvalContext
extends EvalContextAbstract
{
    final private DataFrame dataFrame;
    private EvalContext nestedContext;
    private int rowIndex; // todo: thread local?

    private final MutableMap<String, ValueGetter> resolvedVariables = Maps.mutable.of();

    private interface ValueGetter
    {
        Value getValue();
    }

    public DataFrameEvalContext(DataFrame newDataFrame)
    {
        this(newDataFrame, new SimpleEvalContext());
    }

    public DataFrameEvalContext(DataFrame newDataFrame, EvalContext newNestedContext)
    {
        this.dataFrame = newDataFrame;
        this.nestedContext = newNestedContext;
    }

    public int getRowIndex()
    {
        return this.rowIndex;
    }

    public void setRowIndex(int newRowIndex)
    {
        this.rowIndex = newRowIndex;
    }

    @Override
    public Value getVariable(String variableName)
    {
        ValueGetter valueGetter = this.resolvedVariables.get(variableName);

        if (valueGetter == null)
        {
            if (this.getDataFrame().hasColumn(variableName))
            {
                DfColumn column = this.dataFrame.getColumnNamed(variableName);
                valueGetter = () -> column.getValue(this.getRowIndex());
            }
            else if (this.getContextVariables().containsKey(variableName))
            {
                valueGetter = () -> this.getContextVariables().get(variableName);
            }
            else
            {
                valueGetter = () -> this.getNestedContext().getVariable(variableName);
            }

            this.resolvedVariables.put(variableName, valueGetter);
        }

        return valueGetter.getValue();
    }

    @Override
    public Value getVariableOrDefault(String variableName, Value defaultValue)
    {
        Value value = this.getVariable(variableName);

        return value == Value.VOID ? defaultValue : value;
    }

    @Override
    public boolean hasVariable(String variableName)
    {
        return this.getDataFrame().hasColumn(variableName) || this.getNestedContext().hasVariable(variableName);
    }

    @Override
    public void removeVariable(String variableName)
    {
        exceptionByKey("DSL_ATTEMPT_TO_REMOVE_DF_VAR").fire();
    }

    @Override
    public MapIterable<String, FunctionScript> getDeclaredFunctions()
    {
        return this.getNestedContext().getDeclaredFunctions();
    }

    @Override
    public FunctionScript getDeclaredFunction(String functionName)
    {
        FunctionScript functionScript =  super.getDeclaredFunction(functionName);
        if (functionScript == null)
        {
            functionScript = this.getNestedContext().getDeclaredFunction(functionName);
        }

        return functionScript;
    }

    @Override
    public void addDataSet(HierarchicalDataSet dataSet)
    {
        exceptionByKey("DSL_DF_EVAL_NO_DATASET").fire();
    }

    @Override
    public HierarchicalDataSet getDataSet(String dataSetName)
    {
        return this.getNestedContext().getDataSet(dataSetName);
    }

    @Override
    public RichIterable<String> getVariableNames()
    {
        return this.getNestedContext().getVariableNames();
    }

    @Override
    public void removeAllVariables()
    {
        throw exceptionByKey("DSL_ATTEMPT_TO_REMOVE_DF_VAR").getUnsupported();
    }

    public DataFrame getDataFrame()
    {
        return this.dataFrame;
    }

    public EvalContext getNestedContext()
    {
        return this.nestedContext;
    }

    public void setNestedContext(EvalContext newEvalContext)
    {
        this.nestedContext = newEvalContext;
    }
}
