package io.github.vmzakharov.ecdataframe.dataframe.aggregation;

import io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DOUBLE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.LONG;

public class Max
extends AggregateFunction
{
    private static final ListIterable<ValueType> SUPPORTED_TYPES = Lists.immutable.of(LONG, DOUBLE);

    public Max(String newColumnName)
    {
        super(newColumnName);
    }

    public Max(String newColumnName, String newTargetColumnName)
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
        return "Maximum numeric value";
    }

    @Override
    public Object applyToDoubleColumn(DfDoubleColumn doubleColumn)
    {
        return doubleColumn.toDoubleList().max();
    }

    @Override
    public Object applyToLongColumn(DfLongColumn longColumn)
    {
        return longColumn.toLongList().max();
    }

    @Override
    public long longInitialValue()
    {
        return Long.MIN_VALUE;
    }

    @Override
    public double doubleInitialValue()
    {
        return -Double.MAX_VALUE;
    }

    @Override
    protected long longAccumulator(long currentAggregate, long newValue)
    {
        return Math.max(currentAggregate, newValue);
    }

    @Override
    protected double doubleAccumulator(double currentAggregate, double newValue)
    {
        return Math.max(currentAggregate, newValue);
    }
}
