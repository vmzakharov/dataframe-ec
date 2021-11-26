package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DoubleValue;
import io.github.vmzakharov.ecdataframe.dsl.value.NumberValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.DoubleIterable;
import org.eclipse.collections.api.list.primitive.ImmutableDoubleList;
import org.eclipse.collections.api.list.primitive.MutableDoubleList;
import org.eclipse.collections.impl.factory.primitive.DoubleLists;

public class DfDoubleColumnStored
extends DfDoubleColumn
implements DfColumnStored
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
            this.addDouble(Double.NaN);
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
        if (this.isNull(rowIndex))
        {
            return null;
        }

        return this.getDoubleWithoutNullCheck(rowIndex);
    }

    public double getDouble(int rowIndex)
    {
        if (this.isNull(rowIndex))
        {
            throw new NullPointerException("Null value at " + this.getName() + "[" + rowIndex + "]");
        }

        return this.getDoubleWithoutNullCheck(rowIndex);
    }

    private double getDoubleWithoutNullCheck(int rowIndex)
    {
        return this.values.get(rowIndex);
    }

    @Override
    public Value getValue(int rowIndex)
    {
        return new DoubleValue(this.getDouble(rowIndex));
    }

    @Override
    public Number aggregate(AggregateFunction aggregateFunction)
    {
        return aggregateFunction.applyDoubleIterable(this.values);
    }

    @Override
    public int getSize()
    {
        return this.values.size();
    }

    @Override
    public void setObject(int rowIndex, Object anObject)
    {
        if (anObject == null)
        {
            this.values.set(rowIndex, Double.NaN);
        }
        else
        {
            this.values.set(rowIndex, (Double) anObject);
        }
    }

    public void setDouble(int rowIndex, double value)
    {
        this.values.set(rowIndex, value);
    }

    @Override
    public void addEmptyValue()
    {
        this.values.add(Double.NaN);
    }

    @Override
    public void applyAggregator(int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex, AggregateFunction aggregator)
    {
        double stored = this.isNull(targetRowIndex) ? aggregator.doubleInitialValue() : this.values.get(targetRowIndex);

        this.values.set(targetRowIndex, aggregator.doubleAccumulator(
                stored,
                ((DfDoubleColumn) sourceColumn).getDouble(sourceRowIndex)));
    }

    @Override
    public void ensureCapacity(int newCapacity)
    {
        this.values = DoubleLists.mutable.withInitialCapacity(newCapacity);
    }

    @Override
    public boolean isNull(int rowIndex)
    {
        return Double.isNaN(this.values.get(rowIndex));
    }

    @Override
    protected void addAllItems(DoubleIterable items)
    {
        this.values.addAll(items);
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
