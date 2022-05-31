package io.github.vmzakharov.ecdataframe.dataframe.aggregation;

import io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumn;

public class Min
        extends AggregateFunction
{
    public Min(String newColumnName)
    {
        super(newColumnName);
    }

    public Min(String newColumnName, String newTargetColumnName)
    {
        super(newColumnName, newTargetColumnName);
    }

    @Override
    public String getDescription()
    {
        return "Minimum numeric value";
    }

    @Override
    public Object applyToDoubleColumn(DfDoubleColumn doubleColumn)
    {
        return doubleColumn.toDoubleList().min();
    }

    @Override
    public Object applyToLongColumn(DfLongColumn longColumn)
    {
        return longColumn.toLongList().min();
    }

    @Override
    public long longInitialValue()
    {
        return Long.MAX_VALUE;
    }

    @Override
    public double doubleInitialValue()
    {
        return Double.MAX_VALUE;
    }

    @Override
    protected long longAccumulator(long currentAggregate, long newValue)
    {
        return Math.min(currentAggregate, newValue);
    }

    @Override
    protected double doubleAccumulator(double currentAggregate, double newValue)
    {
        return Math.min(currentAggregate, newValue);
    }
}
