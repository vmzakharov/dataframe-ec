package io.github.vmzakharov.ecdataframe.dataframe;

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

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        DfObjectColumnStored<T> mergedCol = (DfObjectColumnStored<T>) this.validateAndCreateTargetColumn(other, target);

        mergedCol.addAllItems(this.toList());
        mergedCol.addAllItems(((DfObjectColumnAbstract<T>) other).toList());
        return mergedCol;
    }

    @Override
    public DfColumn copyTo(DataFrame target)
    {
        DfObjectColumnStored<T> targetCol = (DfObjectColumnStored<T>) this.copyColumnSchemaAndEnsureCapacity(target);

        targetCol.addAllItems(this.toList());
        return targetCol;
    }
}
