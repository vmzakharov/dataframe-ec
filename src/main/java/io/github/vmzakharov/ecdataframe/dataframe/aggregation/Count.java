package io.github.vmzakharov.ecdataframe.dataframe.aggregation;

import io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DATE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DATE_TIME;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DECIMAL;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DOUBLE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.FLOAT;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.INT;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.LONG;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.STRING;

public class Count
extends AggregateFunction
{
    private static final ListIterable<ValueType> SUPPORTED_TYPES = Lists.immutable.of(INT, LONG, DOUBLE, FLOAT, STRING, DATE, DATE_TIME, DECIMAL);

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
    public ListIterable<ValueType> supportedSourceTypes()
    {
        return SUPPORTED_TYPES;
    }

    @Override
    public boolean nullsArePoisonous()
    {
        return false;
    }

    @Override
    public String getDescription()
    {
        return "Count the number of rows in a set or a group within a set";
    }

    @Override
    public Object applyToColumn(DfColumn column)
    {
        return column.getSize();
    }

    @Override
    public long getLongValue(DfColumn sourceColumn, int sourceRowIndex)
    {
        return 0;
    }

    @Override
    protected long longAccumulator(long currentAggregate, long newValue)
    {
        return currentAggregate + 1;
    }

    @Override
    public long longInitialValue()
    {
        return 0;
    }

    @Override
    public Object valueForEmptyColumn(DfColumn column)
    {
        return 0;
    }
}
