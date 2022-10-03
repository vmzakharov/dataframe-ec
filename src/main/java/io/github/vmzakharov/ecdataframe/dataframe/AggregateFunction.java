package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataframe.aggregation.Avg;
import io.github.vmzakharov.ecdataframe.dataframe.aggregation.Count;
import io.github.vmzakharov.ecdataframe.dataframe.aggregation.Max;
import io.github.vmzakharov.ecdataframe.dataframe.aggregation.Min;
import io.github.vmzakharov.ecdataframe.dataframe.aggregation.Same;
import io.github.vmzakharov.ecdataframe.dataframe.aggregation.Sum;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;

public abstract class AggregateFunction
{
    private final String columnName;
    private final String targetColumnName;

    public AggregateFunction(String newColumnName)
    {
        this(newColumnName, newColumnName);
    }

    public AggregateFunction(String newColumnName, String newTargetColumnName)
    {
        this.columnName = newColumnName;
        this.targetColumnName = newTargetColumnName;
    }

    public static AggregateFunction sum(String newColumnName)
    {
        return new Sum(newColumnName);
    }

    public static AggregateFunction sum(String newColumnName, String newTargetColumnName)
    {
        return new Sum(newColumnName, newTargetColumnName);
    }

    public static AggregateFunction min(String newColumnName)
    {
        return new Min(newColumnName);
    }

    public static AggregateFunction min(String newColumnName, String newTargetColumnName)
    {
        return new Min(newColumnName, newTargetColumnName);
    }

    public static AggregateFunction max(String newColumnName)
    {
        return new Max(newColumnName);
    }

    public static AggregateFunction max(String newColumnName, String newTargetColumnName)
    {
        return new Max(newColumnName, newTargetColumnName);
    }

    public static AggregateFunction avg(String newColumnName)
    {
        return new Avg(newColumnName);
    }

    public static AggregateFunction avg(String newColumnName, String newTargetColumnName)
    {
        return new Avg(newColumnName, newTargetColumnName);
    }

    public static AggregateFunction count(String newColumnName)
    {
        return new Count(newColumnName);
    }

    public static AggregateFunction count(String newColumnName, String newTargetColumnName)
    {
        return new Count(newColumnName, newTargetColumnName);
    }

    public static AggregateFunction same(String newColumnName)
    {
        return new Same(newColumnName);
    }

    public static AggregateFunction same(String newColumnName, String newTargetColumnName)
    {
        return new Same(newColumnName, newTargetColumnName);
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
        this.throwNotApplicable("double values");
        return 0.0;
    }

    public Object applyToLongColumn(DfLongColumn longColumn)
    {
        this.throwNotApplicable("long values");
        return 0;
    }

    public Object applyToObjectColumn(DfObjectColumn<?> objectColumn)
    {
        this.throwNotApplicable("non-numeric values");
        return null;
    }

    protected void throwNotApplicable(String scope)
    {
        ErrorReporter.unsupported(
                "Aggregation '" + this.getName() + "' (" + this.getDescription() + ") cannot be performed on " + scope);
    }

    public long longInitialValue()
    {
        ErrorReporter.unsupported("Operation " + this.getName() + " does not have a long initial value");
        return 0;
    }

    public double doubleInitialValue()
    {
        ErrorReporter.unsupported("Operation " + this.getName() + " does not have a double initial value");
        return 0.0;
    }

    public Object objectInitialValue()
    {
        ErrorReporter.unsupported("Operation " + this.getName() + " does not have a non-numeric initial value");
        return null;
    }

    protected long longAccumulator(long currentAggregate, long newValue)
    {
        ErrorReporter.unsupported("Operation " + this.getName() + " does not support a long accumulator");
        return 0;
    }

    protected double doubleAccumulator(double currentAggregate, double newValue)
    {
        ErrorReporter.unsupported("Operation " + this.getName() + " does not support a double accumulator");
        return 0.0;
    }

    protected Object objectAccumulator(Object currentAggregate, Object newValue)
    {
        ErrorReporter.unsupported("Operation " + this.getName() + " does not support an non-numeric accumulator");
        return null;
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
        this.throwNotApplicable("empty lists");
        return null;
    }

    public long defaultLongIfEmpty()
    {
        this.throwNotApplicable("empty lists");
        return 0;
    }

    public double defaultDoubleIfEmpty()
    {
        this.throwNotApplicable("empty lists");
        return 0.0;
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
     *
     * Override this method to return <code>false</code> if this aggregation function can handle null value.
     *
     * @return <code>true</code> if nulls are poisonous, <code>false</code> if nulls can be handled
     */
    public boolean nullsArePoisonous()
    {
        return true;
    }
}

