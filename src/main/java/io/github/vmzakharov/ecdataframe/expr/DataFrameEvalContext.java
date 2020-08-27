package io.github.vmzakharov.ecdataframe.expr;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataset.AvroDataSet;
import io.github.vmzakharov.ecdataframe.expr.value.Value;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Maps;

public class DataFrameEvalContext
implements EvalContext
{
    final private DataFrame dataFrame;
    private EvalContext nestedContext;
    private int rowIndex; // todo: thread local?

    private final MutableMap<String, ValueGetter> resolvedVariables = Maps.mutable.of();

    private static interface ValueGetter
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
    public Value setVariable(String newVarName, Value newEvaluate)
    {
        throw new UnsupportedOperationException("Cannot assign values to variables for Data Frame based expressions");
    }

    @Override
    public Value getVariable(String variableName)
    {
        // todo: SORT OUT ROW INDEX!!!
        ValueGetter valueGetter = this.resolvedVariables.get(variableName);

        if (valueGetter == null)
        {
            if (this.getDataFrame().hasColumn(variableName))
            {
                valueGetter = () -> dataFrame.getValueAtPhysicalRow(variableName, this.getRowIndex());
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
        // TODO... throw?
        DfColumn column = this.getDataFrame().getColumnNamed(variableName);

        return (column != null) ?
                column.getValue(this.getRowIndex()) :
                this.getNestedContext().getVariableOrDefault(variableName, defaultValue);
    }

    @Override
    public boolean hasVariable(String variableName)
    {
        return this.getDataFrame().hasColumn(variableName) || this.getNestedContext().hasVariable(variableName);
    }

    @Override
    public void removeVariable(String variableName)
    {
        throw new UnsupportedOperationException("Cannot remove a variable from a data frame evaluation context");
    }

    @Override
    public MapIterable<String, FunctionScript> getDeclaredFunctions()
    {
        return this.getNestedContext().getDeclaredFunctions();
    }

    @Override
    public void setDeclaredFunctions(MutableMap<String, FunctionScript> newDeclaredFunctions)
    {
        throw new UnsupportedOperationException("Cannot add a function declaration to a data frame evaluation context");
    }

    @Override
    public void addDataSet(AvroDataSet dataSet)
    {
        throw new UnsupportedOperationException("Cannot add a data set to a data frame evaluation context");
    }

    @Override
    public AvroDataSet getDataSet(String dataSetName)
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
        throw new UnsupportedOperationException("Cannot remove variables from a data frame evaluation context");
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
