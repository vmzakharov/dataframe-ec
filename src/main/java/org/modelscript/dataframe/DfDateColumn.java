package org.modelscript.dataframe;

import org.eclipse.collections.api.list.ImmutableList;
import org.modelscript.expr.value.StringValue;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.value.ValueType;

abstract public class DfDateColumn
extends DfColumnAbstract
{
    public DfDateColumn(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    abstract public String getString(int rowIndex);

    @Override
    public String getValueAsString(int rowIndex)
    {
        return this.getString(rowIndex);
    }

    @Override
    public String getValueAsStringLiteral(int rowIndex)
    {
        String value = this.getString(rowIndex);
        return value == null ? "" : '"' + this.getValueAsString(rowIndex) + '"';
    }

    @Override
    public Object getObject(int rowIndex)
    {
        return this.getString(rowIndex);
    }

    @Override
    public Value getValue(int rowIndex)
    {
        return new StringValue(this.getString(rowIndex));
    }

    public abstract ImmutableList<String> toList();

    public ValueType getType()
    {
        return ValueType.STRING  ;
    }

    @Override
    public void addRowToColumn(int rowIndex, DfColumn target)
    {
//        ((DfStringColumnStored) target).addDate(this.getDate(rowIndex));
    }
}
