package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;

abstract public class DfStringColumn
extends DfColumnAbstract
{
    public DfStringColumn(DataFrame newDataFrame, String newName)
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
        ((DfStringColumnStored) target).addString(this.getString(rowIndex));
    }

    abstract protected void addAllItems(ListIterable<String> items);

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        DfStringColumn mergedCol = (DfStringColumn) this.validateAndCreateTargetColumn(other, target);

        mergedCol.addAllItems(this.toList());
        mergedCol.addAllItems(((DfStringColumn) other).toList());
        return mergedCol;
    }
}
