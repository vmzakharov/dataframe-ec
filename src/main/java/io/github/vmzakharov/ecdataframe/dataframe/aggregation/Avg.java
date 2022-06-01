package io.github.vmzakharov.ecdataframe.dataframe.aggregation;

import io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumnStored;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DOUBLE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.LONG;

public class Avg
extends AggregateFunction
{
    private static final ListIterable<ValueType> SUPPORTED_TYPES = Lists.immutable.of(LONG, DOUBLE);

    public Avg(String newColumnName)
    {
        super(newColumnName);
    }

    public Avg(String newColumnName, String newTargetColumnName)
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
        return "Average, or mean of numeric values";
    }

    @Override
    public Object applyToDoubleColumn(DfDoubleColumn doubleColumn)
    {
        return doubleColumn.toDoubleList().average();
    }

    @Override
    public Object applyToLongColumn(DfLongColumn longColumn)
    {
        return Math.round(longColumn.toLongList().average());
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
    public void finishAggregating(DataFrame aggregatedDataFrame, int[] countsByRow)
    {
        DfColumn aggregatedColumn = aggregatedDataFrame.getColumnNamed(this.getTargetColumnName());

        if (aggregatedColumn.getType().isLong())
        {
            DfLongColumnStored longColumn = (DfLongColumnStored) aggregatedColumn;

            int columnSize = longColumn.getSize();
            for (int rowIndex = 0; rowIndex < columnSize; rowIndex++)
            {
                // todo: check for null
                longColumn.setLong(rowIndex, longColumn.getLong(rowIndex) / countsByRow[rowIndex]);
            }
        }
        else if (aggregatedColumn.getType().isDouble())
        {
            DfDoubleColumnStored doubleColumn = (DfDoubleColumnStored) aggregatedColumn;

            int columnSize = doubleColumn.getSize();
            for (int rowIndex = 0; rowIndex < columnSize; rowIndex++)
            {
                // todo: check for null
                doubleColumn.setDouble(rowIndex, doubleColumn.getDouble(rowIndex) / countsByRow[rowIndex]);
            }
        }
    }
}
