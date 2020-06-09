package org.modelscript.dataframe;

import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.modelscript.expr.value.StringValue;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.value.ValueType;

import java.lang.reflect.Constructor;

public class DfStringColumnStored
extends DfStringColumn
implements DfColumnStored
{
    MutableList<String> values = Lists.mutable.of();

    public DfStringColumnStored(DataFrame owner, String newName)
    {
        super(owner, newName);
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
            this.addString(value.stringValue());
        }
        else
        {
            throw new RuntimeException(
                    "Attempting to add a value of type " + value.getType()
                    + " to a string column " + this.getName()
                    + ": " + value.asStringLiteral());
        }
    }

    public void addString(String s)
    {
        this.values.add(s);
    }

    @Override
    public void addObject(Object newObject)
    {
        this.values.add((String) newObject);
    }

    @Override
    public String getValueAsString(int rowIndex)
    {
        return this.getString(rowIndex);
    }

    public String getString(int rowIndex)
    {
        return this.values.get(rowIndex);
    }

    @Override
    public ValueType getType()
    {
        return ValueType.STRING;
    }

    @Override
    public int getSize()
    {
        return this.values.size();
    }

    @Override
    public void setObject(int rowIndex, Object anObject)
    {
        this.values.set(rowIndex, (String) anObject);
    }

    @Override
    public void addEmptyValue()
    {
        this.values.add(null);
    }

    @Override
    public ImmutableList<String> toList()
    {
        return this.values.toImmutable();
    }

    @Override
    public void incrementFrom(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
    {
        throw new RuntimeException("Not sure if this makes sense");
    }
}
