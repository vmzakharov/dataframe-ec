package org.modelscript.dataframe;

import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.modelscript.expr.value.LongValue;
import org.modelscript.expr.value.Value;

public class DfLongColumnStored
extends DfLongColumn
implements DfColumnStored
{
    private MutableLongList values = LongLists.mutable.of();

    public DfLongColumnStored(DataFrame newDataFrame, String newName)
    {
        super(newDataFrame, newName);
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isLong())
        {
            this.addLong(((LongValue) value).longValue());
        }
        else
        {
            throw new RuntimeException(
                    "Attempting to add a value of type " + value.getType()
                    + " to a long integer column " + this.getName()
                    + ": " + value.asStringLiteral());
        }
    }

    public void addLong(long aLong)
    {
        this.values.add(aLong);
    }

    public long getLong(int rowIndex)
    {
        return this.values.get(rowIndex);
    }

    @Override
    public ImmutableLongList toLongList()
    {
        return this.values.toImmutable();
    }

    @Override
    public long sum()
    {
        return this.values.sum();
    }

    @Override
    public void addObject(Object newObject)
    {
        this.addLong(((Number) newObject).longValue());
    }

    @Override
    public int getSize()
    {
        return this.values.size();
    }

    @Override
    public void setObject(int rowIndex, Object anObject)
    {
        this.values.set(rowIndex, (Long) anObject);
    }

    @Override
    public void incrementFrom(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
    {
        long stored = this.values.get(targetRowIndex);
        this.values.set(targetRowIndex, stored + ((DfLongColumn) sourceColumn).getLong(sourceRowIndex));
    }

    @Override
    public void addEmptyValue()
    {
        this.values.add(0L);
    }

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        ErrorReporter.reportAndThrow(!this.getClass().equals(other.getClass()), "Attempting to merge colums of different types");

        DfLongColumnStored mergedCol = (DfLongColumnStored) this.cloneSchemaAndAttachTo(target);

        mergedCol.values = LongLists.mutable.withInitialCapacity(this.getSize() + other.getSize());
        mergedCol.values.addAll(this.values);
        mergedCol.values.addAll(((DfLongColumnStored) other).values);
        return mergedCol;
    }
}
