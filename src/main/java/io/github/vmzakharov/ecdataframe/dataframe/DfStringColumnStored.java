package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;

public class DfStringColumnStored
extends DfObjectColumnStored<String>
implements DfStringColumn
{
    public DfStringColumnStored(DataFrame owner, String newName)
    {
        super(owner, newName);
    }

    public DfStringColumnStored(DataFrame owner, String newName, ListIterable<String> newValues)
    {
        super(owner, newName, newValues);
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isVoid())
        {
            this.addObject(null);
        }
        else if (value.isString())
        {
            this.addMyType(value.stringValue());
        }
        else
        {
            throw new RuntimeException(
                    "Attempting to add a value of type " + value.getType()
                    + " to a string column " + this.getName()
                    + ": " + value.asStringLiteral());
        }
    }

    @Override
    public ValueType getType()
    {
        return ValueType.STRING;
    }

    @Override
    public void addObject(Object newObject)
    {
        this.addMyType((String) newObject);
    }

    public void addString(String newObject)
    {
        this.addMyType(newObject);
    }

    @Override
    public String getString(int rowIndex)
    {
        return this.getValues().get(rowIndex);
    }

    @Override
    public String getTypedObject(int rowIndex)
    {
        return this.getString(rowIndex);
    }

    @Override
    public void applyAggregator(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregateFunction)
    {
        throw new UnsupportedOperationException("Not implemented");
    }
}
