package io.github.vmzakharov.ecdataframe.dataframe.aggregation;

import io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
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

public class Sum
extends AggregateFunction
{
    private static final ListIterable<ValueType> SUPPORTED_TYPES = Lists.immutable.of(INT, LONG, DOUBLE, DECIMAL);

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
    public ValueType targetColumnType(ValueType sourceColumnType)
    {
        return sourceColumnType.isInt() ? LONG : sourceColumnType;
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
    public Object applyToIntColumn(DfIntColumn intColumn)
    {
        return intColumn.toIntList().sum();
    }

    @Override
    public Object applyToObjectColumn(DfObjectColumn<?> objectColumn)
    {
        return ((DfDecimalColumn) objectColumn).injectIntoBreakOnNulls(
                this.objectInitialValue(),
                BigDecimal::add);
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
    public BigDecimal objectInitialValue()
    {
        return BigDecimal.ZERO;
    }

    @Override
    public long getLongValue(DfColumn sourceColumn, int sourceRowIndex)
    {
        return sourceColumn.getType().isLong()
                ? ((DfLongColumn) sourceColumn).getLong(sourceRowIndex)
                : ((DfIntColumn) sourceColumn).getInt(sourceRowIndex);
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
    protected Object objectAccumulator(Object currentAggregate, Object newValue)
    {
        return ((BigDecimal) currentAggregate).add((BigDecimal) newValue);
    }

    @Override
    public long defaultLongIfEmpty()
    {
        return 0L;
    }

    @Override
    public int defaultIntIfEmpty()
    {
        return 0;
    }

    @Override
    public double defaultDoubleIfEmpty()
    {
        return 0.0;
    }
}
