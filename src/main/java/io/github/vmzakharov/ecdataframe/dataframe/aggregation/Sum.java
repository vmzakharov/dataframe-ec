package io.github.vmzakharov.ecdataframe.dataframe.aggregation;

import io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

public class Sum
extends AggregateFunction
{
    private static final ListIterable<ValueType> SUPPORTED_TYPES = Lists.immutable.of(ValueType.LONG, ValueType.DOUBLE);

    public Sum(String newColumnName)
    {
        super(newColumnName);
    }

    public Sum(String newColumnName, String newTargetColumnName)
    {
        super(newColumnName, newTargetColumnName);
    }

    @Override
    public ListIterable<ValueType> supportedSourceTypes()
    {
        return SUPPORTED_TYPES;
    }

    @Override
    public String getDescription()
    {
        return "Sum";
    }

    @Override
    public Object applyToDoubleColumn(DfDoubleColumn doubleColumn)
    {
        return doubleColumn.toDoubleList().sum();
    }

    @Override
    public Object applyToLongColumn(DfLongColumn longColumn)
    {
        return longColumn.toLongList().sum();
    }

    @Override
    public long longInitialValue()
    {
        return 0L;
    }

    @Override
    public double doubleInitialValue()
    {
        return 0.0;
    }

    @Override
    protected long longAccumulator(long currentAggregate, long newValue)
    {
        return currentAggregate + newValue;
    }

    @Override
    protected double doubleAccumulator(double currentAggregate, double newValue)
    {
        return currentAggregate + newValue;
    }

    @Override
    public long defaultLongIfEmpty()
    {
        return 0L;
    }

    @Override
    public double defaultDoubleIfEmpty()
    {
        return 0.0;
    }
}
