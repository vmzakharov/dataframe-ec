package io.github.vmzakharov.ecdataframe.dataframe.aggregation;

import io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumn;

public class Max
        extends AggregateFunction
{
    public Max(String newColumnName)
    {
        super(newColumnName);
    }

    public Max(String newColumnName, String newTargetColumnName)
    {
        super(newColumnName, newTargetColumnName);
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
