package org.modelscript.dataframe;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.set.Pool;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.modelscript.expr.value.DateValue;
import org.modelscript.expr.value.Value;

import java.time.LocalDate;

public class DfDateColumnStored
extends DfDateColumn
implements DfColumnStored
{
    private MutableList<LocalDate> values = Lists.mutable.of();

    private Pool<LocalDate> pool = null;

    public DfDateColumnStored(DataFrame owner, String newName)
    {
        super(owner, newName);
    }

    public void addDate(LocalDate d)
    {
        if (this.pool == null)
        {
            this.values.add(d);
        }
        else
        {
            this.values.add(this.pool.put(d));
        }
    }

    @Override
    public void enablePooling()
    {
        this.pool = new UnifiedSet<>();
    }

    @Override
    public void seal()
    {
        this.pool = null;
    }

    @Override
    public LocalDate getDate(int rowIndex)
    {
        return this.values.get(rowIndex);
    }

    @Override
    public ImmutableList<LocalDate> toList()
    {
        return this.values.toImmutable();
    }

    @Override
    public void addObject(Object newObject)
    {
        this.addDate((LocalDate) newObject);
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isVoid())
        {
            this.addObject(null);
        }
        else if (value.isDate())
        {
            this.addDate(((DateValue) value).dateValue());
        }
        else
        {
            throw new RuntimeException(
                    "Attempting to add a value of type " + value.getType()
                            + " to a date column " + this.getName()
                            + ": " + value.asStringLiteral());
        }
    }

    @Override
    public int getSize()
    {
        return this.values.size();
    }

    @Override
    public void setObject(int rowIndex, Object anObject)
    {
        this.values.set(rowIndex, (LocalDate) anObject);
    }

    @Override
    public void addEmptyValue()
    {
        this.values.add(null);
    }

    @Override
    public void incrementFrom(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
    {
        throw new RuntimeException("Not implemented yet");
    }

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        ErrorReporter.reportAndThrow(!this.getClass().equals(other.getClass()), "Attempting to merge columns of different types");

        DfDateColumnStored mergedCol = (DfDateColumnStored) this.cloneSchemaAndAttachTo(target);

        mergedCol.values = Lists.mutable.withInitialCapacity(this.getSize() + other.getSize());

        mergedCol.values.addAll(this.values);
        mergedCol.values.addAll(((DfDateColumnStored) other).values);
        return mergedCol;
    }
}
