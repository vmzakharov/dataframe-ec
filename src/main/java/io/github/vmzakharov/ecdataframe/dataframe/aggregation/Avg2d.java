package io.github.vmzakharov.ecdataframe.dataframe.aggregation;

import io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DfColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDecimalColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDecimalColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfDoubleColumnStored;
import io.github.vmzakharov.ecdataframe.dataframe.DfFloatColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfIntColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfLongColumn;
import io.github.vmzakharov.ecdataframe.dataframe.DfObjectColumn;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.util.ExceptionFactory;
import org.eclipse.collections.api.block.procedure.primitive.IntProcedure;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.*;

/**
 * Average data frame aggregation function. For this aggregation the result type is <code>double</code> for columns
 * of primitive type.
 */
public class Avg2d
extends AggregateFunction
{
    private static final ListIterable<ValueType> SUPPORTED_TYPES = Lists.immutable.of(INT, LONG, DOUBLE, FLOAT, DECIMAL);

    public Avg2d(String newColumnName)
    {
        super(newColumnName);
    }

    public Avg2d(String newColumnName, String newTargetColumnName)
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
        return "Average, or mean of numeric values, the result retains the original value type";
    }

    @Override
    public ValueType targetColumnType(ValueType sourceColumnType)
    {
        return sourceColumnType.isDecimal() ? DECIMAL : DOUBLE;
    }

    @Override
    public Object applyToDoubleColumn(DfDoubleColumn doubleColumn)
    {
        return doubleColumn.toDoubleList().average();
    }

    @Override
    public Object applyToFloatColumn(DfFloatColumn floatColumn)
    {
        return floatColumn.toFloatList().average();
    }

    @Override
    public Object applyToLongColumn(DfLongColumn longColumn)
    {
        return longColumn.toLongList().average();
    }

    @Override
    public Object applyToIntColumn(DfIntColumn intColumn)
    {
        return intColumn.toIntList().average();
    }

    @Override
    public Object applyToObjectColumn(DfObjectColumn<?> objectColumn)
    {
        BigDecimal sum = ((DfDecimalColumn) objectColumn).injectIntoBreakOnNulls(
                this.objectInitialValue(),
                BigDecimal::add
        );

        return sum == null ? null : sum.divide(BigDecimal.valueOf(objectColumn.getSize()), RoundingMode.HALF_UP);
    }

    @Override
    public double getDoubleValue(DfColumn sourceColumn, int sourceRowIndex)
    {
        return switch (sourceColumn.getType())
        {
            case DOUBLE -> ((DfDoubleColumn) sourceColumn).getDouble(sourceRowIndex);
            case LONG -> ((DfLongColumn) sourceColumn).getLong(sourceRowIndex);
            case FLOAT -> ((DfFloatColumn) sourceColumn).getFloat(sourceRowIndex);
            case INT -> ((DfIntColumn) sourceColumn).getInt(sourceRowIndex);
            default -> throw this.notApplicable(sourceColumn);
        };
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
    public int intInitialValue()
    {
        return 0;
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
    public float floatInitialValue()
    {
        return 0.0f;
    }

    @Override
    public BigDecimal objectInitialValue()
    {
        return BigDecimal.ZERO;
    }

    @Override
    public void finishAggregating(DataFrame aggregatedDataFrame, int[] countsByRow)
    {
        DfColumn aggregatedColumn = aggregatedDataFrame.getColumnNamed(this.getTargetColumnName());
        int columnSize = aggregatedColumn.getSize();

        IntProcedure avgValueSetter = null;

        if (aggregatedColumn.getType().isDouble())
        {
            DfDoubleColumnStored doubleColumn = (DfDoubleColumnStored) aggregatedColumn;

            avgValueSetter = (int rowIndex) -> {
                double aggregateValue = doubleColumn.getDouble(rowIndex);

                if (this.zeroContributorCheck(countsByRow[rowIndex], aggregateValue != 0, rowIndex))
                {
                    doubleColumn.setDouble(rowIndex, aggregateValue / countsByRow[rowIndex]);
                }
            };
        }
        else if (aggregatedColumn.getType().isDecimal())
        {
            DfDecimalColumnStored decimalColumn = (DfDecimalColumnStored) aggregatedColumn;

            avgValueSetter = (int rowIndex) -> {
                BigDecimal aggregateValue = decimalColumn.getTypedObject(rowIndex);

                if (this.zeroContributorCheck(countsByRow[rowIndex], aggregateValue.signum() != 0, rowIndex))
                {
                    decimalColumn.setObject(rowIndex,
                            aggregateValue.divide(BigDecimal.valueOf(countsByRow[rowIndex]), MathContext.DECIMAL128));
                }
            };
        }

        for (int rowIndex = 0; rowIndex < columnSize; rowIndex++)
        {
            if (!aggregatedColumn.isNull(rowIndex))
            {
                avgValueSetter.value(rowIndex);
            }
        }
    }

    private boolean zeroContributorCheck(int contributorCount, boolean aggregateIsNotZero, int rowIndex)
    {
        if (contributorCount == 0)
        {
            if (aggregateIsNotZero) // zero contributors ended up with a non-zero average value - looks like a bug
            {
                throw ExceptionFactory.exceptionByKey("AGG_ZERO_COUNT")
                                      .with("columnName", this.getTargetColumnName())
                                      .with("rowIndex", rowIndex)
                                      .get();
            }

            return false; // zeros make sense, but don't divide by zero
        }

        return true; // no danger of zero division
    }
}
