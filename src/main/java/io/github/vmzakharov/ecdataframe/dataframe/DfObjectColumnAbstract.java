package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.list.ListIterable;

abstract public class DfObjectColumnAbstract<T>
extends DfColumnAbstract
implements DfObjectColumn<T>
{
    public DfObjectColumnAbstract(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    @Override
    public boolean isNull(int rowIndex)
    {
        return this.getObject(rowIndex) == null;
    }

    abstract protected void addAllItems(ListIterable<T> items);

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        DfObjectColumnAbstract<T> mergedCol = (DfObjectColumnAbstract<T>) this.validateAndCreateTargetColumn(other, target);

        mergedCol.addAllItems(this.toList());
        mergedCol.addAllItems(((DfObjectColumnAbstract<T>) other).toList());
        return mergedCol;
    }
}
