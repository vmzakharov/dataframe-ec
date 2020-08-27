package org.modelscript.dataframe;

import org.eclipse.collections.api.list.ImmutableList;
import org.modelscript.expr.value.DateValue;
import org.modelscript.expr.value.StringValue;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.value.ValueType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

abstract public class DfDateColumn
extends DfColumnAbstract
{
    private DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE;
    public DfDateColumn(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    abstract public LocalDate getDate(int rowIndex);

    @Override
    public String getValueAsString(int rowIndex)
    {
        LocalDate value = this.getDate(rowIndex);
        return value == null ? "" : this.formatter.format(value);
    }

    @Override
    public String getValueAsStringLiteral(int rowIndex)
    {
        return this.getValueAsString(rowIndex);
    }

    @Override
    public Object getObject(int rowIndex)
    {
        return this.getDate(rowIndex);
    }

    @Override
    public Value getValue(int rowIndex)
    {
        return new DateValue(this.getDate(rowIndex));
    }

    public abstract ImmutableList<LocalDate> toList();

    public ValueType getType()
    {
        return ValueType.DATE;
    }

    @Override
    public void addRowToColumn(int rowIndex, DfColumn target)
    {
        ((DfDateColumnStored) target).addDate(this.getDate(rowIndex));
    }
}
