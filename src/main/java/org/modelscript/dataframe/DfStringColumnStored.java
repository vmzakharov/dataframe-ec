package org.modelscript.dataframe;

import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.set.Pool;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.DoubleLists;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.value.ValueType;

public class DfStringColumnStored
extends DfStringColumn
implements DfColumnStored
{
    private MutableList<String> values = Lists.mutable.of();

    private Pool<String> pool = null;

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
        if (pool == null)
        {
            this.values.add(s);
        }
        else
        {
            this.values.add(this.pool.put(s));
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
    public void addObject(Object newObject)
    {
        this.addString((String) newObject);
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

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        ErrorReporter.reportAndThrow(!this.getClass().equals(other.getClass()), "Attempting to merge columns of different types");

        DfStringColumnStored mergedCol = (DfStringColumnStored) this.cloneSchemaAndAttachTo(target);

        mergedCol.values = Lists.mutable.withInitialCapacity(this.getSize() + other.getSize());

        mergedCol.values.addAll(this.values);
        mergedCol.values.addAll(((DfStringColumnStored) other).values);
        return mergedCol;
    }
}
