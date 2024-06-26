package io.github.vmzakharov.ecdataframe.dataframe.aggregation;

import io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.DfFloatColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfFloatColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.DfIntColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfIntColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.DfObjectColumn;
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

public class Same
extends AggregateFunction
{
    private static final ListIterable<ValueType> SUPPORTED_TYPES = Lists.fixedSize.of(INT, LONG, DOUBLE, FLOAT, STRING, DATE, DATE_TIME, DECIMAL);

    private static final long INITIAL_VALUE_LONG = System.nanoTime();
    private static final double INITIAL_VALUE_DOUBLE = INITIAL_VALUE_LONG;
    private static final int INITIAL_VALUE_INT = Integer.MIN_VALUE;
    private static final float INITIAL_VALUE_FLOAT =  INITIAL_VALUE_INT;
    private static final Object INITIAL_VALUE_OBJECT = new Object();

    public Same(String newColumnName)
    {
        super(newColumnName);
    }

    public Same(String newColumnName, String newTargetColumnName)
    {
        super(newColumnName, newTargetColumnName);
    }

    @Override
    public int intInitialValue()
    {
        return INITIAL_VALUE_INT;
    }

    @Override
    public long longInitialValue()
    {
        return INITIAL_VALUE_LONG;
    }

    @Override
    public double doubleInitialValue()
    {
        return INITIAL_VALUE_DOUBLE;
    }

    @Override
    public float floatInitialValue()
    {
        return INITIAL_VALUE_FLOAT;
    }

    @Override
    public Object objectInitialValue()
    {
        return INITIAL_VALUE_OBJECT;
    }

    @Override
    public void aggregateValueIntoInt(DfIntColumnStored targetColumn, int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
    {
        int currentAggregatedValue = targetColumn.getInt(targetRowIndex);
        int nextValue = this.getIntValue(sourceColumn, sourceRowIndex);

        if (currentAggregatedValue == INITIAL_VALUE_INT)
        {
            targetColumn.setInt(targetRowIndex, nextValue);
        }
        else if (currentAggregatedValue != nextValue)
        {
            targetColumn.setObject(targetRowIndex, null);
        }
    }

    @Override
    public void aggregateValueIntoLong(DfLongColumnStored targetColumn, int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
    {
        long currentAggregatedValue = targetColumn.getLong(targetRowIndex);
        long nextValue = this.getLongValue(sourceColumn, sourceRowIndex);

        if (currentAggregatedValue == INITIAL_VALUE_LONG)
        {
            targetColumn.setLong(targetRowIndex, nextValue);
        }
        else if (currentAggregatedValue != nextValue)
        {
            targetColumn.setObject(targetRowIndex, null);
        }
    }

    @Override
    public void aggregateValueIntoDouble(DfDoubleColumnStored targetColumn, int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
    {
        double currentAggregatedValue = targetColumn.getDouble(targetRowIndex);
        double nextValue = this.getDoubleValue(sourceColumn, sourceRowIndex);

        if (currentAggregatedValue == INITIAL_VALUE_DOUBLE)
        {
            targetColumn.setDouble(targetRowIndex, nextValue);
        }
        else if (currentAggregatedValue != nextValue)
        {
            targetColumn.setObject(targetRowIndex, null);
        }
    }

    @Override
    public void aggregateValueIntoFloat(DfFloatColumnStored targetColumn, int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
    {
        float currentAggregatedValue = targetColumn.getFloat(targetRowIndex);
        float nextValue = this.getFloatValue(sourceColumn, sourceRowIndex);

        if (currentAggregatedValue == INITIAL_VALUE_FLOAT)
        {
            targetColumn.setFloat(targetRowIndex, nextValue);
        }
        else if (currentAggregatedValue != nextValue)
        {
            targetColumn.setObject(targetRowIndex, null);
        }
    }

    @Override
    protected Object objectAccumulator(Object currentAggregate, Object newValue)
    {
        if (currentAggregate == INITIAL_VALUE_OBJECT)
        {
            return newValue;
        }

        if (currentAggregate != null && currentAggregate.equals(newValue))
        {
            return currentAggregate;
        }

        return null;
    }

    @Override
    public Object applyToDoubleColumn(DfDoubleColumn doubleColumn)
    {
        double first = doubleColumn.getDouble(0);
        if (doubleColumn.asDoubleIterable().allSatisfy(each -> each == first))
        {
            return first;
        }

        return null;
    }

    @Override
    public Object applyToFloatColumn(DfFloatColumn floatColumn)
    {
        float first = floatColumn.getFloat(0);
        if (floatColumn.asFloatIterable().allSatisfy(each -> each == first))
        {
            return first;
        }

        return null;
    }

    @Override
    public Object applyToLongColumn(DfLongColumn longColumn)
    {
        long first = longColumn.getLong(0);
        if (longColumn.asLongIterable().allSatisfy(each -> each == first))
        {
            return first;
        }

        return null;
    }

    @Override
    public Object applyToIntColumn(DfIntColumn intColumn)
    {
        long first = intColumn.getInt(0);
        if (intColumn.asIntIterable().allSatisfy(each -> each == first))
        {
            return first;
        }

        return null;
    }

    @Override
    public Object applyToObjectColumn(DfObjectColumn<?> objectColumn)
    {
        return objectColumn.injectIntoBreakOnNulls(
                objectColumn.getObject(0),
                (result, current) -> current == null ? null : (result.equals(current) ? result : null)
        );
    }

    @Override
    public String getDescription()
    {
        return "All values in the set are the same";
    }

    @Override
    public ListIterable<ValueType> supportedSourceTypes()
    {
        return SUPPORTED_TYPES;
    }
}
