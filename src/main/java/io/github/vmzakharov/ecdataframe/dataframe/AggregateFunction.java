package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataframe.aggregation.Avg;
import io.github.vmzakharov.ecdataframe.dataframe.aggregation.Count;
import io.github.vmzakharov.ecdataframe.dataframe.aggregation.Max;
import io.github.vmzakharov.ecdataframe.dataframe.aggregation.Min;
import io.github.vmzakharov.ecdataframe.dataframe.aggregation.Same;
import io.github.vmzakharov.ecdataframe.dataframe.aggregation.Sum;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public abstract class AggregateFunction
{
    private final String columnName;
    private final String targetColumnName;

    public AggregateFunction(String newSourceColumnName)
    {
        this(newSourceColumnName, newSourceColumnName);
    }

    public AggregateFunction(String newSourceColumnName, String newTargetColumnName)
    {
        this.columnName = newSourceColumnName;
        this.targetColumnName = newTargetColumnName;
    }

    public static AggregateFunction sum(String newSourceColumnName)
    {
        return new Sum(newSourceColumnName);
    }

    public static AggregateFunction sum(String newSourceColumnName, String newTargetColumnName)
    {
        return new Sum(newSourceColumnName, newTargetColumnName);
    }

    public static AggregateFunction min(String newSourceColumnName)
    {
        return new Min(newSourceColumnName);
    }

    public static AggregateFunction min(String newSourceColumnName, String newTargetColumnName)
    {
        return new Min(newSourceColumnName, newTargetColumnName);
    }

    public static AggregateFunction max(String newSourceColumnName)
    {
        return new Max(newSourceColumnName);
    }

    public static AggregateFunction max(String newSourceColumnName, String newTargetColumnName)
    {
        return new Max(newSourceColumnName, newTargetColumnName);
    }

    public static AggregateFunction avg(String newSourceColumnName)
    {
        return new Avg(newSourceColumnName);
    }

    public static AggregateFunction avg(String newSourceColumnName, String newTargetColumnName)
    {
        return new Avg(newSourceColumnName, newTargetColumnName);
    }

    public static AggregateFunction count(String newSourceColumnName)
    {
        return new Count(newSourceColumnName);
    }

    public static AggregateFunction count(String newSourceColumnName, String newTargetColumnName)
    {
        return new Count(newSourceColumnName, newTargetColumnName);
    }

    public static AggregateFunction same(String newSourceColumnName)
    {
        return new Same(newSourceColumnName);
    }

    public static AggregateFunction same(String newSourceColumnName, String newTargetColumnName)
    {
        return new Same(newSourceColumnName, newTargetColumnName);
    }

    public AggregateFunction cloneWith(String newSourceColumnName, String newTargetColumnName)
    {
        try
        {
            Constructor<?> ctor = this.getClass().getConstructor(String.class, String.class);
            return (AggregateFunction) ctor.newInstance(newSourceColumnName, newTargetColumnName);
        }
        catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e)
        {
            throw exceptionByKey("AGG_CANNOT_CLONE")
                    .with("operation", this.getName())
                    .get(e);
        }
    }

    public String getColumnName()
    {
        return this.columnName;
    }

    public String getTargetColumnName()
    {
        return this.targetColumnName;
    }

    public ValueType targetColumnType(ValueType sourceColumnType)
    {
        return sourceColumnType;
    }

    abstract public ListIterable<ValueType> supportedSourceTypes();

    public boolean supportsSourceType(ValueType type)
    {
        return this.supportedSourceTypes().contains(type);
    }

    public Object applyToDoubleColumn(DfDoubleColumn doubleColumn)
    {
        throw this.notApplicable("double values");
    }

    public Object applyToLongColumn(DfLongColumn longColumn)
    {
        throw this.notApplicable("long values");
    }

    public Object applyToObjectColumn(DfObjectColumn<?> objectColumn)
    {
        throw this.notApplicable("non-numeric values");
    }

    protected RuntimeException notApplicable(String scope)
    {
        return exceptionByKey("AGG_NOT_APPLICABLE")
                .with("operation", this.getName())
                .with("operationDescription", this.getDescription())
                .with("operationScope", scope)
                .getUnsupported();
    }

    public long longInitialValue()
    {
        throw exceptionByKey("AGG_NO_INITIAL_VALUE")
                .with("operation", this.getName())
                .with("type", "long")
                .getUnsupported();
    }

    public double doubleInitialValue()
    {
        throw exceptionByKey("AGG_NO_INITIAL_VALUE")
                .with("operation", this.getName())
                .with("type", "double")
                .getUnsupported();
    }

    public Object objectInitialValue()
    {
        throw exceptionByKey("AGG_NO_INITIAL_VALUE")
                .with("operation", this.getName())
                .with("type", "non-numeric")
                .getUnsupported();
    }

    protected long longAccumulator(long currentAggregate, long newValue)
    {
        throw exceptionByKey("AGG_NO_ACCUMULATOR")
                .with("operation", this.getName())
                .with("type", "long")
                .getUnsupported();
    }

    protected double doubleAccumulator(double currentAggregate, double newValue)
    {
        throw exceptionByKey("AGG_NO_ACCUMULATOR")
                .with("operation", this.getName())
                .with("type", "double")
                .getUnsupported();
    }

    protected Object objectAccumulator(Object currentAggregate, Object newValue)
    {
        throw exceptionByKey("AGG_NO_ACCUMULATOR")
                .with("operation", this.getName())
                .with("non-numeric", "long")
                .getUnsupported();
    }

    /**
     * A short name used to identify this aggregation function (such as 'Sum', 'Min', 'Avg', etc.). It is primarily
     * meant for internal used (as opposed to the value returned by the <code>getDescription()</code> method).
     * @return the name of this aggregation function
     */
    public String getName()
    {
        return this.getClass().getSimpleName();
    }

    /**
     * A brief description of the aggregation operation supported by this function (primarily for display purposes)
     * @return the description of this aggregation function
     */
    public String getDescription()
    {
        return this.getName();
    }

    public Object defaultObjectIfEmpty()
    {
        throw this.notApplicable("empty lists");
    }

    public long defaultLongIfEmpty()
    {
        throw this.notApplicable("empty lists");
    }

    public double defaultDoubleIfEmpty()
    {
        throw this.notApplicable("empty lists");
    }

    public long getLongValue(DfColumn sourceColumn, int sourceRowIndex)
    {
        return ((DfLongColumn) sourceColumn).getLong(sourceRowIndex);
    }

    public double getDoubleValue(DfColumn sourceColumn, int sourceRowIndex)
    {
        return ((DfDoubleColumn) sourceColumn).getDouble(sourceRowIndex);
    }

    public Object getObjectValue(DfColumn sourceColumn, int sourceRowIndex)
    {
        return sourceColumn.getObject(sourceRowIndex);
    }

    public void finishAggregating(DataFrame aggregatedDataFrame, int[] countsByRow)
    {
    }

    public void initializeValue(DfColumn accumulatorColumn, int accumulatorRowIndex)
    {
        if (accumulatorColumn.getType().isDouble())
        {
            ((DfDoubleColumnStored) accumulatorColumn).setDouble(accumulatorRowIndex, this.doubleInitialValue());
        }
        else if (accumulatorColumn.getType().isLong())
        {
            ((DfLongColumnStored) accumulatorColumn).setLong(accumulatorRowIndex, this.longInitialValue());
        }
        else
        {
            accumulatorColumn.setObject(accumulatorRowIndex, this.objectInitialValue());
        }
    }

    public void aggregateValueIntoLong(
            DfLongColumnStored targetColumn, int targetRowIndex,
            DfColumn sourceColumn, int sourceRowIndex)
    {
        long currentAggregatedValue = targetColumn.getLong(targetRowIndex);
        targetColumn.setLong(
                targetRowIndex,
                this.longAccumulator(currentAggregatedValue, this.getLongValue(sourceColumn, sourceRowIndex)));
    }

    public void aggregateValueIntoDouble(
            DfDoubleColumnStored targetColumn, int targetRowIndex,
            DfColumn sourceColumn, int sourceRowIndex)
    {
        double currentAggregatedValue = targetColumn.getDouble(targetRowIndex);
        targetColumn.setDouble(
                targetRowIndex,
                this.doubleAccumulator(currentAggregatedValue, this.getDoubleValue(sourceColumn, sourceRowIndex)));
    }

    /**
     * by default aggregators treat null values as "poisonous" - that is any null value passed in the aggregator will
     * cause the result of the entire aggregation to be null, which is a sensible behavior for most aggregation
     * functions.
     * Override this method to return <code>false</code> if this aggregation function can handle a null value.
     *
     * @return <code>true</code> if nulls are poisonous, <code>false</code> if nulls can be handled
     */
    public boolean nullsArePoisonous()
    {
        return true;
    }
}

