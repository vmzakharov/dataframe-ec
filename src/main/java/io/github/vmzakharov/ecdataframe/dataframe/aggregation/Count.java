package io.github.vmzakharov.ecdataframe.dataframe.aggregation;

import io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfObjectColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;

public class Count
        extends AggregateFunction
{
    public Count(String newColumnName)
    {
        super(newColumnName);
    }

    public Count(String newColumnName, String newTargetColumnName)
    {
        super(newColumnName, newTargetColumnName);
    }

    @Override
    public ValueType targetColumnType(ValueType sourceColumnType)
    {
        return ValueType.LONG;
    }

    @Override
    public String getDescription()
    {
        return "Count the number of rows in a set or a group within a set";
    }

    @Override
    public Object applyToDoubleColumn(DfDoubleColumn doubleColumn)
    {
        return doubleColumn.getSize();
    }

    @Override
    public Object applyToLongColumn(DfLongColumn longColumn)
    {
        return longColumn.getSize();
    }

    @Override
    public Object applyToObjectColumn(DfObjectColumn<?> objectColumn)
    {
        return objectColumn.getSize();
    }

    @Override
    public long getLongValue(DfColumn sourceColumn, int sourceRowIndex)
    {
        return 0;
    }

    @Override
    public double getDoubleValue(DfColumn sourceColumn, int sourceRowIndex)
    {
        return 0.0;
    }

    @Override
    protected long longAccumulator(long currentAggregate, long newValue)
    {
        return currentAggregate + 1;
    }

    @Override
    protected double doubleAccumulator(double currentAggregate, double newValue)
    {
        return currentAggregate + 1;
    }

    @Override
    public long longInitialValue()
    {
        return 0;
    }

    @Override
    public double doubleInitialValue()
    {
        return 0;
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
