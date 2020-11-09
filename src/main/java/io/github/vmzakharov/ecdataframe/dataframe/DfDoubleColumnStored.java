package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.NumberValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.primitive.ImmutableDoubleList;
import org.eclipse.collections.api.list.primitive.MutableDoubleList;
import org.eclipse.collections.impl.factory.primitive.DoubleLists;

public class DfDoubleColumnStored
extends DfDoubleColumn
{
    private MutableDoubleList values = DoubleLists.mutable.of();

    public DfDoubleColumnStored(DataFrame owner, String newName)
    {
        super(owner, newName);
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isNumber())
        {
            this.addDouble(((NumberValue) value).doubleValue());
        }
        else
        {
            throw new RuntimeException(
                    "Attempting to add a value of type " + value.getType()
                    + " to a double column " + this.getName()
                    + ": " + value.asStringLiteral());
        }
    }

    public void addDouble(double d)
    {
        this.values.add(d);
    }

    @Override
    public void addObject(Object newObject)
    {
        if (newObject == null)
        {
            this.addDouble(0.0);
        }
        else
        {
            this.addDouble(((Number) newObject).doubleValue());
        }
    }

    @Override
    public String getValueAsString(int rowIndex)
    {
        return Double.toString(this.getDouble(rowIndex));
    }

    @Override
    public Object getObject(int rowIndex)
    {
        return this.getDouble(rowIndex);
    }

    public double getDouble(int rowIndex)
    {
        return this.values.get(rowIndex);
    }

    @Override
    public Value getValue(int rowIndex)
    {
        return new DoubleValue(this.getDouble(rowIndex));
    }

    @Override
    public ValueType getType()
    {
        return ValueType.DOUBLE;
    }

    @Override
    public double sum()
    {
        return this.values.sum();
    }

    @Override
    public int getSize()
    {
        return this.values.size();
    }

    @Override
    public void setObject(int rowIndex, Object anObject)
    {
        this.values.set(rowIndex, (Double) anObject);
    }

    @Override
    public void addEmptyValue()
    {
        this.values.add(0.0);
    }

    @Override
    public void incrementFrom(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
    {
        double stored = this.values.get(targetRowIndex);
        this.values.set(targetRowIndex, stored + ((DfDoubleColumn) sourceColumn).getDouble(sourceRowIndex));
    }

    @Override
    public DfColumn mergeWithInto(DfColumn other, DataFrame target)
    {
        ErrorReporter.reportAndThrow(!this.getClass().equals(other.getClass()), "Attempting to merge colums of different types");

        DfDoubleColumnStored mergedCol = (DfDoubleColumnStored) this.cloneSchemaAndAttachTo(target);

        mergedCol.values = DoubleLists.mutable.withInitialCapacity(this.getSize() + other.getSize());
        mergedCol.values.addAll(this.values);
        mergedCol.values.addAll(((DfDoubleColumnStored) other).values);
        return mergedCol;
    }

    @Override
    public boolean isStored()
    {
        return true;
    }

    @Override
    public ImmutableDoubleList toDoubleList()
    {
        return this.values.toImmutable();
    }
}
