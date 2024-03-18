package io.github.vmzakharov.ecdataframe.dataframe.aggregation;

import io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction;
import io.github.vmzakharov.ecdataframe.dataframe.DfDecimalColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfIntColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfObjectColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

import java.math.BigDecimal;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.*;

public class Max
extends AggregateFunction
{
    private static final ListIterable<ValueType> SUPPORTED_TYPES = Lists.immutable.of(INT, LONG, DOUBLE, DECIMAL);

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
    public Object applyToIntColumn(DfIntColumn intColumn)
    {
        return intColumn.toIntList().max();
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
    public Object applyToObjectColumn(DfObjectColumn<?> objectColumn)
    {
        return ((DfDecimalColumn) objectColumn).injectIntoBreakOnNulls(
                this.objectInitialValue(),
                (result, current) -> result.compareTo(current) > 0 ? result : current);
    }

    @Override
    public BigDecimal objectInitialValue()
    {
        return BigDecimal.valueOf(-Double.MAX_VALUE);
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

    @Override
    protected Object objectAccumulator(Object currentAggregate, Object newValue)
    {
        return ((BigDecimal) currentAggregate).compareTo((BigDecimal) newValue) < 0 ? newValue : currentAggregate;
    }
}
