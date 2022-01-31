package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;

abstract public class DfObjectColumn<T>
extends DfColumnAbstract
{
    public DfObjectColumn(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    abstract public ImmutableList<T> toList();

    abstract public T getTypedObject(int rowIndex);

    abstract protected void addAllItems(ListIterable<T> items);

    @Override
    public boolean isNull(int rowIndex)
    {
        return this.getObject(rowIndex) == null;
    }

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        DfObjectColumn<T> mergedCol = (DfObjectColumn<T>) this.validateAndCreateTargetColumn(other, target);

        mergedCol.addAllItems(this.toList());
        mergedCol.addAllItems(((DfObjectColumn<T>) other).toList());
        return mergedCol;
    }
}
