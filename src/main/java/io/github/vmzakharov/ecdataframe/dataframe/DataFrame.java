package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.DataFrameEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.DoubleIterable;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.block.function.primitive.IntIntToIntFunction;
import org.eclipse.collections.api.block.predicate.primitive.BooleanPredicate;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableBooleanList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.api.tuple.Triplet;
import org.eclipse.collections.api.tuple.Twin;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.eclipse.collections.impl.list.mutable.primitive.BooleanArrayList;
import org.eclipse.collections.impl.list.primitive.IntInterval;
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.utility.ArrayIterate;

import java.time.LocalDate;
import java.util.Arrays;

public class DataFrame
{
    private final String name;
    private final MutableMap<String, DfColumn> columnsByName = Maps.mutable.of();
    private final MutableList<DfColumn> columns = Lists.mutable.of();
    private int rowCount = 0;

    private final DataFrameEvalContext evalContext; // todo: thread safety?
    private IntList virtualRowMap = null;
    private boolean poolingEnabled = false;

    private MutableBooleanList bitmap = null;

    private MutableList<MutableIntList> aggregateIndex = null;

    public DataFrame(String newName)
    {
        this.name = newName;

        this.evalContext = new DataFrameEvalContext(this);

        this.resetBitmap();
    }

    public DataFrame addStringColumn(String newColumnName)
    {
        return this.addColumn(new DfStringColumnStored(this, newColumnName));
    }

    public DataFrame addStringColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumn(new DfStringColumnComputed(this, newColumnName, expressionAsString));
    }

    public DataFrame addStringColumn(String newColumnName, ListIterable<String> values)
    {
        return this.addColumn(new DfStringColumnStored(this, newColumnName, values));
    }

    public DataFrame addLongColumn(String newColumnName)
    {
        return this.addColumn(new DfLongColumnStored(this, newColumnName));
    }

    public DataFrame addLongColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumn(new DfLongColumnComputed(this, newColumnName, expressionAsString));
    }

    public DataFrame addLongColumn(String newColumnName, LongIterable values)
    {
        return this.addColumn(new DfLongColumnStored(this, newColumnName, values));
    }

    public DataFrame addDoubleColumn(String newColumnName)
    {
        return this.addColumn(new DfDoubleColumnStored(this, newColumnName));
    }

    public DataFrame addDoubleColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumn(new DfDoubleColumnComputed(this, newColumnName, expressionAsString));
    }

    public DataFrame addDoubleColumn(String newColumnName, DoubleIterable values)
    {
        return this.addColumn(new DfDoubleColumnStored(this, newColumnName, values));
    }

    public DataFrame addDateColumn(String newColumnName)
    {
        return this.addColumn(new DfDateColumnStored(this, newColumnName));
    }

    public DataFrame addDateColumn(String newColumnName, ListIterable<LocalDate> values)
    {
        return this.addColumn(new DfDateColumnStored(this, newColumnName, values));
    }

    public DataFrame addDateColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumn(new DfDateColumnComputed(this, newColumnName, expressionAsString));
    }

    public DataFrame addColumn(DfColumn newColumn)
    {
        // todo: would like to make it impossible in the first place
        if (newColumn.getDataFrame() != this)
        {
            throw new RuntimeException("Mixing columns from different data frames: attempting to add"
                    + " to '" + this.getName() + "' column '"
                    + newColumn.getName() + "' already bound to '" + newColumn.getDataFrame().getName() + "'");
        }

        ErrorReporter.reportAndThrow(this.hasColumn(newColumn.getName()),
                "Column named '" + newColumn.getName() + "' is already in data frame '" + this.getName() + "'");

        this.columnsByName.put(newColumn.getName(), newColumn);
        this.columns.add(newColumn);

        if (this.isPoolingEnabled())
        {
            newColumn.enablePooling();
        }
        return this;
    }

    public void enablePooling()
    {
        this.poolingEnabled = true;
        this.columns.forEach(DfColumn::enablePooling);
    }

    public boolean isPoolingEnabled()
    {
        return this.poolingEnabled;
    }

    public DfColumn getColumnNamed(String columnName)
    {
        DfColumn column = this.columnsByName.get(columnName);

        if (column == null)
        {
            ErrorReporter.reportAndThrow("Column '" + columnName + "' does not exist in data frame '" + this.getName() + "'");
        }

        return column;
    }

    public ImmutableList<DfColumn> getColumns()
    {
        return this.columns.toImmutable();
    }

    public DfColumn getColumnAt(int columnIndex)
    {
        return this.columns.get(columnIndex);
    }

    public void addRow(ListIterable<Value> rowValues)
    {
        rowValues.forEachWithIndex((v, i) -> this.columns.get(i).addValue(v));
        this.rowCount++;
    }

    /**
     * Convert the data frame into a multi-line CSV string. The output will include column headers.
     *
     * @return a string representation of the data frame.
     */
    public String asCsvString()
    {
        return this.asCsvString(-1);
    }

    /**
     * Convert the data frame into a multi-line CSV string. The output will include column headers.
     *
     * @param limit number of rows to convert, all rows if the value is negative. If the value is zero the result will
     *              only contain column names.
     * @return a string representation of the data frame.
     */
    public String asCsvString(int limit)
    {
        StringBuilder s = new StringBuilder();

        s.append(this.columns.collect(DfColumn::getName).makeString(","));
        s.append('\n');

        int columnCount = this.columnCount();
        String[] row = new String[columnCount];

        int last = limit < 0 ? this.rowCount() : Math.min(limit, this.rowCount());

        for (int rowIndex = 0; rowIndex < last; rowIndex++)
        {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {
                row[columnIndex] = this.getValueAsStringLiteral(rowIndex, columnIndex);
            }
            s.append(ArrayIterate.makeString(row, ","));
            s.append('\n');
        }

        return s.toString();
    }

    public int rowCount()
    {
        return this.rowCount;
    }

    public DataFrame addRow()
    {
        this.columns.forEach(DfColumn::addEmptyValue);
        this.rowCount++;
        return this;
    }

    public DataFrame addRow(Object... values)
    {
        if (values.length > this.columnCount())
        {
            ErrorReporter.reportAndThrow(
                    "Adding more row elements (" + values.length + ") than there are columns in the data frame (" + this.columnCount() + ")");
        }

        ArrayIterate.forEachWithIndex(values, (v, i) -> this.columns.get(i).addObject(v));
        this.rowCount++;
        return this;
    }

    public int columnCount()
    {
        return this.columns.size();
    }

    public String getName()
    {
        return this.name;
    }

    public DataFrame addColumn(String columnName, ValueType type)
    {
        switch (type)
        {
            case LONG:
                this.addLongColumn(columnName);
                break;
            case DOUBLE:
                this.addDoubleColumn(columnName);
                break;
            case STRING:
                this.addStringColumn(columnName);
                break;
            case DATE:
                this.addDateColumn(columnName);
                break;
            default:
                throw new RuntimeException("Cannot add a column for values of type " + type);
        }
        return this;
    }

    public DataFrame addColumn(String columnName, ValueType type, String expressionAsString)
    {
        switch (type)
        {
            case LONG:
                this.addLongColumn(columnName, expressionAsString);
                break;
            case DOUBLE:
                this.addDoubleColumn(columnName, expressionAsString);
                break;
            case STRING:
                this.addStringColumn(columnName, expressionAsString);
                break;
            case DATE:
                this.addDateColumn(columnName, expressionAsString);
                break;
            default:
                throw new RuntimeException("Cannot add a column for values of type " + type);
        }
        return this;
    }

    protected int rowIndexMap(int virtualRowIndex)
    {
        if (this.isIndexed())
        {
            return this.virtualRowMap.get(virtualRowIndex);
        }
        return virtualRowIndex;
    }

    private boolean isIndexed()
    {
        return this.virtualRowMap != null;
    }

    public IntList getAggregateIndex(int rowIndex)
    {
        if (this.isAggregateWithIndex())
        {
            return this.aggregateIndex.get(this.rowIndexMap(rowIndex));
        }

        return IntLists.immutable.empty();
    }

    private boolean isAggregateWithIndex()
    {
        return this.aggregateIndex != null;
    }

    public Object getObject(int rowIndex, int columnIndex)
    {
        return this.columns.get(columnIndex).getObject(this.rowIndexMap(rowIndex));
    }

    public Object getObject(String columnName, int rowIndex)
    {
        return this.getColumnNamed(columnName).getObject(this.rowIndexMap(rowIndex));
    }

    public boolean isNull(String columnName, int rowIndex)
    {
        return this.getColumnNamed(columnName).isNull(this.rowIndexMap(rowIndex));
    }

    public Value getValue(int rowIndex, int columnIndex)
    {
        return this.columns.get(columnIndex).getValue(this.rowIndexMap(rowIndex));
    }

    public Value getValue(String columnName, int rowIndex)
    {
        return this.columnsByName.get(columnName).getValue(this.rowIndexMap(rowIndex));
    }

    public Value getValueAtPhysicalRow(String columnName, int rowIndex)
    {
        return this.columnsByName.get(columnName).getValue(rowIndex);
    }

    public String getValueAsStringLiteral(int rowIndex, int columnIndex)
    {
        return this.columns.get(columnIndex).getValueAsStringLiteral(this.rowIndexMap(rowIndex));
    }

    public String getValueAsString(int rowIndex, int columnIndex)
    {
        return this.columns.get(columnIndex).getValueAsString(this.rowIndexMap(rowIndex));
    }

    public long getLong(String columnName, int rowIndex)
    {
        return this.getLongColumn(columnName).getLong(this.rowIndexMap(rowIndex));
    }

    public String getString(String columnName, int rowIndex)
    {
        return this.getStringColumn(columnName).getTypedObject(this.rowIndexMap(rowIndex));
    }

    public double getDouble(String columnName, int rowIndex)
    {
        return this.getDoubleColumn(columnName).getDouble(this.rowIndexMap(rowIndex));
    }

    public LocalDate getDate(String columnName, int rowIndex)
    {
        return this.getDateColumn(columnName).getTypedObject(this.rowIndexMap(rowIndex));
    }

    public DfLongColumn getLongColumn(String columnName)
    {
        return (DfLongColumn) this.getColumnNamed(columnName);
    }

    public DfDoubleColumn getDoubleColumn(String columnName)
    {
        return (DfDoubleColumn) this.getColumnNamed(columnName);
    }

    public DfDateColumn getDateColumn(String columnName)
    {
        return (DfDateColumn) this.getColumnNamed(columnName);
    }

    public DfStringColumn getStringColumn(String columnName)
    {
        return (DfStringColumn) this.getColumnNamed(columnName);
    }

    public boolean hasColumn(String columnName)
    {
        return this.columnsByName.containsKey(columnName);
    }

    public DataFrameEvalContext getEvalContext()
    {
        return this.evalContext;
    }

    public void setExternalEvalContext(EvalContext newEvalContext)
    {
        this.evalContext.setNestedContext(newEvalContext);
    }

    /**
     * Indicates that no further updates will be made to this data frame and ensures that the data frame is in a
     * consistent internal state.
     *
     * @return the data frame
     */
    public DataFrame seal()
    {
        MutableIntList storedColumnsSizes = this.columns.select(DfColumn::isStored).collectInt(DfColumn::getSize);
        if (storedColumnsSizes.size() == 0)
        {
            this.rowCount = 0;
        }
        else
        {
            this.rowCount = storedColumnsSizes.get(0);
            if (storedColumnsSizes.anySatisfy(e -> e != this.rowCount))
            {
                ErrorReporter.reportAndThrow(
                        "Stored column sizes are not the same when attempting to seal data frame '" + this.getName() + "'");
            }
        }

        this.resetBitmap();

        this.columns.forEach(DfColumn::seal);
        return this;
    }

    /**
     * Sums up the values in the specified columns
     *
     * @param columnsToAggregateNames - the columns to aggregate
     * @return a single row data frame containing the aggregated values in the respective columns
     */
    public DataFrame sum(ListIterable<String> columnsToAggregateNames)
    {
        return this.aggregate(columnsToAggregateNames.collect(AggregateFunction::sum));
    }

    /**
     * Aggregate the values in the specified columns
     *
     * @param aggregators - the aggregate functions to be applied to columns to aggregate
     * @return a single row data frame containing the aggregated values in the respective columns
     */
    public DataFrame aggregate(ListIterable<AggregateFunction> aggregators)
    {
        ListIterable<DfColumn> columnsToAggregate = this.getColumnsToAggregate(aggregators.collect(AggregateFunction::getColumnName));

        DataFrame summedDataFrame = new DataFrame("Aggregate Of " + this.getName());

        columnsToAggregate.forEachInBoth(aggregators,
                (col, agg) -> summedDataFrame.addColumn(agg.getTargetColumnName(), agg.targetColumnType(col.getType()))
        );

        ListIterable<Object> aggregatedValues = columnsToAggregate.collectWithIndex(
                (each, index) -> each.aggregate(aggregators.get(index))
        );

        summedDataFrame.addRow(aggregatedValues.toArray());
        return summedDataFrame;
    }

    private ListIterable<DfColumn> getColumnsToAggregate(ListIterable<String> columnNames)
    {
        return this.columnsNamed(columnNames);
    }

    /**
     * Aggregate the values in the specified columns grouped by values in the specified group by columns
     *
     * @param aggregators - the aggregate functions to be applied to columns to aggregate
     * @param columnsToGroupByNames - the columns to group by
     * @return a data frame with a summary row for each unique combination of the values in the columns to group by,
     * containing aggregated values in the columns to aggregate
     */
    public DataFrame aggregateBy(
            ListIterable<AggregateFunction> aggregators,
            ListIterable<String> columnsToGroupByNames)
    {
        return this.aggregateByWithIndex(aggregators, columnsToGroupByNames, false);
    }

    /**
     * Add up values in columns into summary rows group by values in the specified group by columns
     *
     * @param columnsToSumNames     - the columns to aggregate
     * @param columnsToGroupByNames - the columns to group by
     * @return a data frame with a row for each unique combination of the values in the columns to group by, containing
     * summed up values in the columns to aggregate
     */
    public DataFrame sumBy(ListIterable<String> columnsToSumNames, ListIterable<String> columnsToGroupByNames)
    {
        return this.aggregateBy(columnsToSumNames.collect(AggregateFunction::sum), columnsToGroupByNames);
    }

    public DataFrame aggregateByWithIndex(
            ListIterable<AggregateFunction> aggregators,
            ListIterable<String> columnsToGroupByNames)
    {
        return this.aggregateByWithIndex(aggregators, columnsToGroupByNames, true);
    }

    private DataFrame aggregateByWithIndex(
            ListIterable<AggregateFunction> aggregators,
            ListIterable<String> columnsToGroupByNames,
            boolean createSourceRowIdIndex)
    {
        MutableList<MutableIntList> sumIndex = null;
        if (createSourceRowIdIndex)
        {
            sumIndex = Lists.mutable.of();
        }

        int[] counts = new int[this.rowCount()]; // todo: a bit wasteful?

        ListIterable<String> columnsToAggregateNames = aggregators.collect(AggregateFunction::getColumnName);
        ListIterable<DfColumn> columnsToAggregate = this.getColumnsToAggregate(columnsToAggregateNames);

        DataFrame aggregatedDataFrame = new DataFrame("Aggregate Of " + this.getName());

        columnsToGroupByNames
                .collect(this::getColumnNamed)
                .forEach(col -> aggregatedDataFrame.addColumn(col.getName(), col.getType()));

        columnsToAggregate.forEachInBoth(aggregators,
                (col, agg) -> aggregatedDataFrame.addColumn(agg.getTargetColumnName(), agg.targetColumnType(col.getType()))
        );

        ListIterable<DfColumn> accumulatorColumns = aggregators
                .collect(AggregateFunction::getTargetColumnName)
                .collect(aggregatedDataFrame::getColumnNamed);

        DfIndexKeeper index = new DfIndexKeeper(aggregatedDataFrame, columnsToGroupByNames);

        for (int rowIndex = 0; rowIndex < this.rowCount; rowIndex++)
        {
            ListIterable<Object> keyValue = index.computeKeyFrom(this, rowIndex);

            int accumulatorRowIndex;
            if (index.doesNotContain(keyValue))
            {
                // new entry in the aggregated data frame - need to initialize accumulators
                accumulatorRowIndex = index.getRowIndexAtKeyIfAbsentAdd(keyValue);

                aggregators.forEachInBoth(accumulatorColumns,
                        (aggregateFunction, accumulatorColumn) -> aggregateFunction.initializeValue(accumulatorColumn, accumulatorRowIndex));
            }
            else
            {
                // an existing entry so just a lookup
                accumulatorRowIndex = index.getRowIndexAtKeyIfAbsentAdd(keyValue);
            }

            if (createSourceRowIdIndex)
            {
                while (sumIndex.size() <= accumulatorRowIndex)
                {
                    sumIndex.add(IntLists.mutable.of());
                }

                sumIndex.get(accumulatorRowIndex).add(rowIndex);
            }

            counts[accumulatorRowIndex]++;

            for (int colIndex = 0; colIndex < columnsToAggregate.size(); colIndex++)
            {
                accumulatorColumns.get(colIndex).applyAggregator(accumulatorRowIndex, columnsToAggregate.get(colIndex), rowIndex, aggregators.get(colIndex));
            }
        }

        if (createSourceRowIdIndex)
        {
            aggregatedDataFrame.aggregateIndex = sumIndex;
        }

        aggregators.forEach(agg -> agg.finishAggregating(aggregatedDataFrame, counts));

        return aggregatedDataFrame;
    }

    public DataFrame sumByWithIndex(ListIterable<String> columnsToSumNames, ListIterable<String> columnsToGroupByNames)
    {
        return this.aggregateByWithIndex(columnsToSumNames.collect(AggregateFunction::sum), columnsToGroupByNames);
    }

    public Twin<DataFrame> partition(String filterExpressionString)
    {
        DataFrame selected = this.cloneStructure(this.name + "-selected");
        DataFrame rejected = this.cloneStructure(this.name + "-rejected");

        DataFrameEvalContext context = new DataFrameEvalContext(this);
        Expression filterExpression = ExpressionParserHelper.DEFAULT.toExpression(filterExpressionString);
        InMemoryEvaluationVisitor evaluationVisitor = new InMemoryEvaluationVisitor(context);

        for (int i = 0; i < this.rowCount; i++)
        {
            context.setRowIndex(i);
            Value select = filterExpression.evaluate(evaluationVisitor);
            if (((BooleanValue) select).isTrue())
            {
                selected.copyRowFrom(this, i);
            }
            else
            {
                rejected.copyRowFrom(this, i);
            }
        }

        selected.seal();
        rejected.seal();

        return Tuples.twin(selected, rejected);
    }

    public DataFrame selectBy(String filterExpressionString)
    {
        DataFrame filtered = this.cloneStructure(this.getName() + "-selected");
        DataFrameEvalContext context = new DataFrameEvalContext(this);
        Expression filterExpression = ExpressionParserHelper.DEFAULT.toExpression(filterExpressionString);
        InMemoryEvaluationVisitor evaluationVisitor = new InMemoryEvaluationVisitor(context);
        for (int i = 0; i < this.rowCount; i++)
        {
            context.setRowIndex(i);
            Value select = filterExpression.evaluate(evaluationVisitor);
            if (((BooleanValue) select).isTrue())
            {
                filtered.copyRowFrom(this, i);
            }
        }
        filtered.seal();
        return filtered;
    }

    private DataFrame selectByMarkValue(BooleanPredicate markAtIndexPredicate)
    {
        DataFrame filtered = this.cloneStructure(this.getName() + "-selected");
        for (int i = 0; i < this.rowCount; i++)
        {
            if (markAtIndexPredicate.accept(this.isFlagged(i)))
            {
                filtered.copyRowFrom(this, this.rowIndexMap(i));
            }
        }
        filtered.seal();
        return filtered;
    }

    private void copyRowFrom(DataFrame source, int rowIndex)
    {
        for (int columnIndex = 0; columnIndex < this.columns.size(); columnIndex++)
        {
            DfColumn thisColumn = this.columns.get(columnIndex);

            if (thisColumn.isStored())
            {
                source.getColumnAt(columnIndex).addRowToColumn(rowIndex, thisColumn);
            }
        }
    }

    private void copyIndexedRowFrom(DataFrame source, int rowIndex)
    {
        this.copyRowFrom(source, source.rowIndexMap(rowIndex));
    }

    public DataFrame cloneStructure(String newName)
    {
        DataFrame cloned = new DataFrame(newName);

        this.columns.each(each -> each.cloneSchemaAndAttachTo(cloned));

        return cloned;
    }

    /**
     * creates an empty data frame with the same schema as this one except computed columns are converted to stored
     * columns of the same type
     *
     * @param newName the name for the new data frame
     * @return an empty data frame with the provided name and new schema
     */
    public DataFrame cloneStructureAsStored(String newName)
    {
        DataFrame cloned = new DataFrame(newName);

        this.columns.each(e -> cloned.addColumn(e.getName(), e.getType()));

        return cloned;
    }

    public DataFrame sortBy(ListIterable<String> columnsToSortByNames)
    {
        this.unsort();

        // doing this before check for rowCount to make sure that the sort columns exist even if the data frame is empty
        ListIterable<DfColumn> columnsToSortBy = this.columnsNamed(columnsToSortByNames);

        if (this.rowCount == 0)
        {
            this.virtualRowMap = IntLists.immutable.empty();
            return this;
        }

        DfTuple[] tuples = new DfTuple[this.rowCount];
        for (int i = 0; i < this.rowCount; i++)
        {
            tuples[i] = this.rowToTuple(i, columnsToSortBy);
        }

        MutableIntList indexes = IntInterval.zeroTo(this.rowCount - 1).toList();
        indexes.sortThisBy(i -> tuples[i]);

        this.virtualRowMap = indexes;

        return this;
    }

    public DataFrame sortByExpression(String expressionString)
    {
        this.unsort();
        if (this.rowCount == 0)
        {
            this.virtualRowMap = IntLists.immutable.empty();
            return this;
        }

        Expression expression = ExpressionParserHelper.DEFAULT.toExpression(expressionString);
        MutableIntList indexes = IntInterval.zeroTo(this.rowCount - 1).toList();
        indexes.sortThisBy(i -> this.evaluateExpression(expression, i));
        this.virtualRowMap = indexes;

        return this;
    }

    public Value evaluateExpression(Expression expression, int rowIndex)
    {
        this.getEvalContext().setRowIndex(rowIndex);
        return expression.evaluate(new InMemoryEvaluationVisitor(this.evalContext));
    }

    private DfTuple rowToTuple(int rowIndex, ListIterable<DfColumn> columnsToCollect)
    {
        int size = columnsToCollect.size();
        Object[] values = new Object[size];
        for (int i = 0; i < size; i++)
        {
            values[i] = columnsToCollect.get(i).getObject(rowIndex);
        }
        return new DfTuple(values);
    }

    public void unsort()
    {
        this.virtualRowMap = null;
    }

    private ListIterable<DfColumn> columnsNamed(ListIterable<String> columnNames)
    {
        return columnNames.collect(this::getColumnNamed);
    }

    /**
     * Creates a new data frame which is a union of this data frame and the data frame passed as the parameter. The data
     * frame schemas must match.
     *
     * @param other the data frame to union with
     * @return a data frame with the rows being the union of the rows this data frame and the parameter
     */
    public DataFrame union(DataFrame other)
    {
        ErrorReporter.reportAndThrow(this.columnCount() != other.columnCount(), "Attempting to union data frames with different numbers of columns");
        DataFrame dfUnion = new DataFrame("union");

        this.columns.forEach(
                col -> col.mergeWithInto(other.getColumnNamed(col.getName()), dfUnion)
        );

        dfUnion.seal();
        return dfUnion;
    }

    /**
     * enables flagging the rows as true or false - effectively creating a bitmap of the data frame
     */
    public void resetBitmap()
    {
        this.bitmap = BooleanArrayList.newWithNValues(this.rowCount, false);
    }

    public void setFlag(int rowIndex)
    {
        this.bitmap.set(rowIndex, true);
    }

    public boolean isFlagged(int rowIndex)
    {
        return this.bitmap.get(rowIndex);
    }

    /**
     * creates a new data frame, which contain a subset of rows of this data frame for the rows with the bitmap flags
     * set (i.e. equals to true). This is the behavior the opposite of {@code selectNotMarked}
     *
     * @return a data frame containing the filtered subset of rows
     */
    public DataFrame selectFlagged()
    {
        return this.selectByMarkValue(mark -> mark);
    }

    /**
     * creates a new data frame, which contain a subset of rows of this data frame for the rows with the bitmap flag not
     * set (i.e. equals to false). This is the behavior the opposite of {@code selectMarked}
     *
     * @return a data frame containing the filtered subset of rows
     */
    public DataFrame selectNotFlagged()
    {
        return this.selectByMarkValue(mark -> !mark);
    }

    /**
     * Tag rows based on whether the provided expression returns true of false
     *
     * @param filterExpressionString the expression to set the flags by
     */
    public void flagRowsBy(String filterExpressionString)
    {
        this.bitmap = BooleanArrayList.newWithNValues(this.rowCount, false);

        DataFrameEvalContext context = new DataFrameEvalContext(this);
        Expression filterExpression = ExpressionParserHelper.DEFAULT.toExpression(filterExpressionString);
        InMemoryEvaluationVisitor evaluationVisitor = new InMemoryEvaluationVisitor(context);

        for (int i = 0; i < this.rowCount; i++)
        {
            context.setRowIndex(i);
            BooleanValue evalResult = (BooleanValue) filterExpression.evaluate(evaluationVisitor);
            if (evalResult.isTrue())
            {
                this.bitmap.set(i, true);
            }
        }
    }

    /**
     * Removes the column from this data frame. Throws a {@code RuntimeException} if the specified column doesn't
     * exist.
     *
     * @param columnName the name of the column to drop.
     * @return the data frame
     */
    public DataFrame dropColumn(String columnName)
    {
        DfColumn dropped = this.getColumnNamed(columnName);

        this.columns.remove(dropped);
        this.columnsByName.remove(columnName);

        return this;
    }

    /**
     * A very basic join - creates a data frame that is a join of this data frame and another one, based on the key
     * column values. Rows with the same values of the key column will be combined in the resulting data frame into one
     * wide row. This is an inner join so rows for which there is no match in the other data frame will not be present
     * in the join.
     *
     * @param other               the data frame to join to
     * @param thisJoinColumnName  the name of the column in this data frame to use as the join key
     * @param otherJoinColumnName the name of the column in the other data frame to use as the join key
     * @return a data frame that is a join of this data frame and the data frame passed as a parameter
     */
    public DataFrame join(DataFrame other, String thisJoinColumnName, String otherJoinColumnName)
    {
        return this.join(other, Lists.immutable.of(thisJoinColumnName), Lists.immutable.of(otherJoinColumnName));
    }

    /**
     * A basic inner join - creates a data frame that is a join of this data frame and another one, based on the key
     * column values. Rows with the same values of the key column will be combined in the resulting data frame into one
     * wide row. This is an inner join so rows for which there is no match in the other data frame will not be present
     * in the join.
     *
     * @param other                the data frame to join to
     * @param thisJoinColumnNames  the name of the columns in this data frame to use as the join keys
     * @param otherJoinColumnNames the name of the columns in the other data frame to use as the join keys, they will be
     *                             compared to the columns in this data frame in the order they are specified
     * @return a data frame that is a join of this data frame and the data frame passed as a parameter
     */
    public DataFrame join(DataFrame other, ListIterable<String> thisJoinColumnNames, ListIterable<String> otherJoinColumnNames)
    {
        return this.join(
                other, JoinType.INNER_JOIN,
                thisJoinColumnNames, Lists.immutable.empty(),
                otherJoinColumnNames, Lists.immutable.empty(),
                Maps.mutable.of()
        ).getTwo();
    }

    /**
     * A basic inner join - creates a data frame that is a join of this data frame and another one, based on the key
     * column values. Rows with the same values of the key column will be combined in the resulting data frame into one
     * wide row. This is an inner join so rows for which there is no match in the other data frame will not be present
     * in the join.
     *
     * @param other                the data frame to join to
     * @param thisJoinColumnNames  the name of the columns in this data frame to use as the join keys
     * @param otherJoinColumnNames the name of the columns in the other data frame to use as the join keys, they will be
     *                             compared to the columns in this data frame in the order they are specified
     * @param renamedOtherColumns  the map, which will be populated with the column names on the other data frame
     *                             renamed in the join to avoid column name collisions
     * @return a data frame that is a join of this data frame and the data frame passed as a parameter
     */
    public DataFrame join(
            DataFrame other,
            ListIterable<String> thisJoinColumnNames,
            ListIterable<String> otherJoinColumnNames,
            MutableMap<String, String> renamedOtherColumns)
    {
        return this.join(
                other, JoinType.INNER_JOIN,
                thisJoinColumnNames, Lists.immutable.empty(),
                otherJoinColumnNames, Lists.immutable.empty(),
                renamedOtherColumns
        ).getTwo();
    }

    /**
     * A basic outer join - creates a data frame that is a join of this data frame and another one, based on the key
     * column values. Rows with the same values of the key column will be combined in the resulting data frame into one
     * wide row. The rows for which there is no match in the other data frame will have the missing values filled with
     * nulls for object column types or zeros for numeric column types.
     *
     * @param other               the data frame to join to
     * @param thisJoinColumnName  the name of the column in this data frame to use as the join key
     * @param otherJoinColumnName the name of the column in the other data frame to use as the join key
     * @return a data frame that is a join of this data frame and the data frame passed as a parameter
     */
    public DataFrame outerJoin(DataFrame other, String thisJoinColumnName, String otherJoinColumnName)
    {
        return this.outerJoin(other, Lists.immutable.of(thisJoinColumnName), Lists.immutable.of(otherJoinColumnName));
    }

    /**
     * A basic outer join - creates a data frame that is a join of this data frame and another one, based on the key
     * column values. Rows with the same values of the key columns will be combined in the resulting data frame into one
     * wide row. The rows for which there is no match in the other data frame will have the missing values filled with
     * nulls for object column types or zeros for numeric column types.
     *
     * @param other                the data frame to join to
     * @param thisJoinColumnNames  the name of the columns in this data frame to use as the join keys
     * @param otherJoinColumnNames the name of the columns in the other data frame to use as the join keys
     * @return a data frame that is a join of this data frame and the data frame passed as a parameter
     */
    public DataFrame outerJoin(DataFrame other, ListIterable<String> thisJoinColumnNames, ListIterable<String> otherJoinColumnNames)
    {
        return this.join(
                other, JoinType.OUTER_JOIN,
                thisJoinColumnNames, Lists.immutable.empty(),
                otherJoinColumnNames, Lists.immutable.empty(),
                Maps.mutable.of()
        ).getTwo();
    }

    /**
     * A basic outer join - creates a data frame that is a join of this data frame and another one, based on the key
     * column values. Rows with the same values of the key columns will be combined in the resulting data frame into one
     * wide row. The rows for which there is no match in the other data frame will have the missing values filled with
     * nulls for object column types or zeros for numeric column types.
     *
     * @param other                the data frame to join to
     * @param thisJoinColumnNames  the name of the columns in this data frame to use as the join keys
     * @param otherJoinColumnNames the name of the columns in the other data frame to use as the join keys
     * @param renamedOtherColumns  the map, which will be populated with the column names on the other data frame
     *                             renamed in the join to avoid column name collisions
     * @return a data frame that is a join of this data frame and the data frame passed as a parameter
     */
    public DataFrame outerJoin(
            DataFrame other,
            ListIterable<String> thisJoinColumnNames,
            ListIterable<String> otherJoinColumnNames,
            MutableMap<String, String> renamedOtherColumns)
    {
        return this.join(
                other, JoinType.OUTER_JOIN,
                thisJoinColumnNames, Lists.immutable.empty(),
                otherJoinColumnNames, Lists.immutable.empty(),
                renamedOtherColumns
        ).getTwo();
    }

    /**
     * Performs intersection and complement set operations between two data frames based on the provided keys and
     * returns their results as a triplet of data frames. The result of the intersection is equivalent to inner join of
     * two data frames, and the result of each complement is the subset of the rows of each data frame that does not
     * have corresponding keys in the other data frame
     *
     * @param other                the data frame to join to
     * @param thisJoinColumnNames  the name of the columns in this data frame to use as the join keys
     * @param otherJoinColumnNames the name of the columns in the other data frame to use as the join keys
     * @return a triplet containing the complement of the other dataframe in this one, the joined dataframe, and the
     * complement of this data frame in the other one
     */
    public Triplet<DataFrame> joinWithComplements(
            DataFrame other,
            ListIterable<String> thisJoinColumnNames,
            ListIterable<String> otherJoinColumnNames)
    {
        return this.join(
                other, JoinType.JOIN_WITH_COMPLEMENTS,
                thisJoinColumnNames, Lists.immutable.empty(),
                otherJoinColumnNames, Lists.immutable.empty(),
                Maps.mutable.of());
    }

    /**
     * Performs intersection and complement set operations between two data frames based on the provided keys and
     * returns their results as a triplet of data frames. The result of the intersection is equivalent to inner join of
     * two data frames, and the result of each complement is the subset of the rows of each data frame that does not
     * have corresponding keys in the other data frame
     *
     * @param other                the data frame to join to
     * @param thisJoinColumnNames  the name of the columns in this data frame to use as the join keys
     * @param otherJoinColumnNames the name of the columns in the other data frame to use as the join keys
     * @param renamedOtherColumns  the map which will be populated with the column names on the other data frame renamed
     *                             in the join to avoid column name collisions
     * @return a triplet containing the complement of the other dataframe in this one, the joined dataframe, and the
     * complement of this data frame in the other one
     */
    public Triplet<DataFrame> joinWithComplements(
            DataFrame other,
            ListIterable<String> thisJoinColumnNames,
            ListIterable<String> otherJoinColumnNames,
            MutableMap<String, String> renamedOtherColumns)
    {
        return this.join(
                other, JoinType.JOIN_WITH_COMPLEMENTS,
                thisJoinColumnNames, Lists.immutable.empty(),
                otherJoinColumnNames, Lists.immutable.empty(),
                renamedOtherColumns);
    }

    /**
     * Performs intersection and complement set operations between two data frames based on the provided keys and
     * returns their results as a triplet of data frames. The result of the intersection is equivalent to inner join of
     * two data frames, and the result of each complement is the subset of the rows of each data frame that does not
     * have corresponding keys in the other data frame
     *
     * @param other                          the data frame to join to
     * @param thisJoinColumnNames            the name of the columns in this data frame to use as the join keys
     * @param thisAdditionalSortColumnNames  specifies columns for sort order on this data frame's side in addition to
     *                                       the order of the values of its key columns
     * @param otherJoinColumnNames           the name of the columns in the other data frame to use as the join keys
     * @param otherAdditionalSortColumnNames specifies columns for sort order on the other data frame's side in addition
     *                                       to the order of the values of its key columns
     * @return a triplet containing the complement of the other dataframe in this one, the joined dataframe, and the
     * complement of this data frame in the other one
     */
    public Triplet<DataFrame> joinWithComplements(
            DataFrame other,
            ListIterable<String> thisJoinColumnNames,
            ListIterable<String> thisAdditionalSortColumnNames,
            ListIterable<String> otherJoinColumnNames,
            ListIterable<String> otherAdditionalSortColumnNames)
    {
        return this.join(
                other, JoinType.JOIN_WITH_COMPLEMENTS,
                thisJoinColumnNames, thisAdditionalSortColumnNames,
                otherJoinColumnNames, otherAdditionalSortColumnNames,
                Maps.mutable.of());
    }

    /**
     * Performs intersection and complement set operations between two data frames based on the provided keys and
     * returns their results as a triplet of data frames. The result of the intersection is equivalent to inner join of
     * two data frames, and the result of each complement is the subset of the rows of each data frame that does not
     * have corresponding keys in the other data frame
     *
     * @param other                          the data frame to join to
     * @param thisJoinColumnNames            the name of the columns in this data frame to use as the join keys
     * @param thisAdditionalSortColumnNames  specifies columns for sort order on this data frame's side in addition to
     *                                       the order of the values of its key columns
     * @param otherJoinColumnNames           the name of the columns in the other data frame to use as the join keys
     * @param otherAdditionalSortColumnNames specifies columns for sort order on the other data frame's side in addition
     *                                       to the order of the values of its key columns
     * @param renamedOtherColumns            the map which will be populated with the column names on the other data
     *                                       frame renamed in the join to avoid column name collisions
     * @return a triplet containing the complement of the other dataframe in this one, the joined dataframe, and the
     * complement of this data frame in the other one
     */
    public Triplet<DataFrame> joinWithComplements(
            DataFrame other,
            ListIterable<String> thisJoinColumnNames,
            ListIterable<String> thisAdditionalSortColumnNames,
            ListIterable<String> otherJoinColumnNames,
            ListIterable<String> otherAdditionalSortColumnNames,
            MutableMap<String, String> renamedOtherColumns)
    {
        return this.join(
                other, JoinType.JOIN_WITH_COMPLEMENTS,
                thisJoinColumnNames, thisAdditionalSortColumnNames,
                otherJoinColumnNames, otherAdditionalSortColumnNames,
                renamedOtherColumns);
    }

    // todo: do not override sort order (use an external sort)
    private Triplet<DataFrame> join(
            DataFrame other,
            JoinType joinType,
            ListIterable<String> thisJoinColumnNames,
            ListIterable<String> thisAdditionalSortColumnNames,
            ListIterable<String> otherJoinColumnNames,
            ListIterable<String> otherAdditionalSortColumnNames,
            MutableMap<String, String> renamedOtherColumns
    )
    {
        if (thisJoinColumnNames.size() != otherJoinColumnNames.size())
        {
            ErrorReporter.reportAndThrow("Attempting to join dataframes by different number of keys on each side: "
                    + thisJoinColumnNames.makeString() + " to " + otherJoinColumnNames.makeString());
        }

        DataFrame joined = this.cloneStructureAsStored(this.getName() + "_" + other.getName());

        DataFrame thisComplementOther = this.cloneStructureAsStored(this.getName() + "-" + other.getName());
        DataFrame otherComplementThis = other.cloneStructureAsStored(other.getName() + "-" + this.getName());

        MapIterable<String, String> otherColumnNameMap = this.resolveDuplicateNames(
                this.columns.collect(DfColumn::getName),
                other.columns.collect(DfColumn::getName));

        otherColumnNameMap.forEachKeyValue(renamedOtherColumns::put);
        otherJoinColumnNames.forEachInBoth(thisJoinColumnNames, renamedOtherColumns::put);

        other.columns
                .reject(col -> otherJoinColumnNames.contains(col.getName()))
                .forEach(col -> joined.addColumn(otherColumnNameMap.get(col.getName()), col.getType()));

        this.sortBy(thisJoinColumnNames.toList().withAll(thisAdditionalSortColumnNames));
        other.sortBy(otherJoinColumnNames.toList().withAll(otherAdditionalSortColumnNames));

        int thisRowIndex = 0;
        int otherRowIndex = 0;

        int thisRowCount = this.rowCount();
        int otherRowCount = other.rowCount();

        Object[] rowData = new Object[joined.columnCount()];

        ListIterable<String> theseColumnNames = this.columns.collect(DfColumn::getName);
        int theseColumnCount = theseColumnNames.size();

        IntList joinColumnIndices = thisJoinColumnNames.collectInt(theseColumnNames::indexOf);

        ListIterable<String> otherColumnNames = other.columns
                .collect(DfColumn::getName)
                .reject(otherJoinColumnNames::contains);

        IntIntToIntFunction keyComparator = (thisIndex, otherIndex) -> {
            for (int i = 0; i < thisJoinColumnNames.size(); i++)
            {
                int result = DfTuple.compareMindingNulls(
                        this.getObject(thisJoinColumnNames.get(i), thisIndex),
                        other.getObject(otherJoinColumnNames.get(i), otherIndex)
                );
                if (result != 0)
                {
                    return result;
                }
            }
            return 0;
        };

        int comparison;
        while (thisRowIndex < thisRowCount && otherRowIndex < otherRowCount)
        {
            comparison = keyComparator.valueOf(thisRowIndex, otherRowIndex);

            final int thisMakeFinal = thisRowIndex;
            final int otherMakeFinal = otherRowIndex;

            if (comparison == 0)
            {
                theseColumnNames.forEachWithIndex((colName, i) -> rowData[i] = this.getObject(colName, thisMakeFinal));
                otherColumnNames.forEachWithIndex((colName, i) -> rowData[theseColumnCount + i] = other.getObject(colName, otherMakeFinal));

                joined.addRow(rowData);
                thisRowIndex++;
                otherRowIndex++;
            }
            else
            {
                if (comparison < 0)
                {
                    // this side is behind
                    if (joinType.isOuterJoin())
                    {
                        Arrays.fill(rowData, null);
                        theseColumnNames.forEachWithIndex((colName, i) -> rowData[i] = this.getObject(colName, thisMakeFinal));

                        joined.addRow(rowData);
                    }
                    else if (joinType.isJoinWithComplements())
                    {
                        thisComplementOther.copyIndexedRowFrom(this, thisMakeFinal);
                    }
                    thisRowIndex++;
                }
                else
                {
                    // the other side is behind
                    if (joinType.isOuterJoin())
                    {
                        Arrays.fill(rowData, null);

                        joinColumnIndices.forEachWithIndex(
                                (joinColumnIndex, sourceKeyIndex)
                                        -> rowData[joinColumnIndex] = other.getObject(otherJoinColumnNames.get(sourceKeyIndex), otherMakeFinal)
                        );

                        otherColumnNames.forEachWithIndex((colName, i) -> rowData[theseColumnCount + i] = other.getObject(colName, otherMakeFinal));

                        joined.addRow(rowData);
                    }
                    else if (joinType.isJoinWithComplements())
                    {
                        otherComplementThis.copyIndexedRowFrom(other, otherMakeFinal);
                    }
                    otherRowIndex++;
                }
            }
        }

        //   leftovers go here
        if (joinType.isOuterJoin())
        {
            while (thisRowIndex < thisRowCount)
            {
                int thisMakeFinal = thisRowIndex;

                Arrays.fill(rowData, null);
                theseColumnNames.forEachWithIndex((colName, i) -> rowData[i] = this.getObject(colName, thisMakeFinal));

                joined.addRow(rowData);

                thisRowIndex++;
            }

            while (otherRowIndex < otherRowCount)
            {
                int otherMakeFinal = otherRowIndex;

                Arrays.fill(rowData, null);

                joinColumnIndices.forEachWithIndex(
                        (joinColumnIndex, sourceKeyIndex)
                                -> rowData[joinColumnIndex] = other.getObject(otherJoinColumnNames.get(sourceKeyIndex), otherMakeFinal)
                );

                otherColumnNames.forEachWithIndex((colName, i) -> rowData[theseColumnCount + i] = other.getObject(colName, otherMakeFinal));

                joined.addRow(rowData);

                otherRowIndex++;
            }
        }
        else if (joinType.isJoinWithComplements())
        {
            while (thisRowIndex < thisRowCount)
            {
                thisComplementOther.copyIndexedRowFrom(this, thisRowIndex);
                thisRowIndex++;
            }

            while (otherRowIndex < otherRowCount)
            {
                otherComplementThis.copyIndexedRowFrom(other, otherRowIndex);
                otherRowIndex++;
            }
        }

        thisComplementOther.seal();
        otherComplementThis.seal();
        return Tuples.triplet(thisComplementOther, joined, otherComplementThis);
    }

    private MapIterable<String, String> resolveDuplicateNames(
            ListIterable<String> theseNames,
            ListIterable<String> otherNames
    )
    {
        MutableSet<String> uniqueColumnNames = theseNames.toSet();

        MutableMap<String, String> otherColumnNameMap = Maps.mutable.of();

        otherNames.forEach(e ->
                {
                    String newName = e;
                    while (uniqueColumnNames.contains(newName))
                    {
                        newName += "_B";
                    }
                    uniqueColumnNames.add(newName);
                    otherColumnNameMap.put(e, newName);
                }
        );

        return otherColumnNameMap;
    }

    /**
     * Drops the data frame columns with the names specified as the method parameter
     *
     * @param columnNamesToDrop the names of the columns to remove from this data frame
     * @return the data frame
     */
    public DataFrame dropColumns(ListIterable<String> columnNamesToDrop)
    {
        ListIterable<DfColumn> columnsToDrop = columnNamesToDrop.collect(this::getColumnNamed);

        this.columns.removeAllIterable(columnsToDrop);
        this.columnsByName.removeAllKeys(columnNamesToDrop.toSet());

        return this;
    }

    /**
     * Drops all the data frame columns except those with the names specified as the method parameter
     *
     * @param columnNamesToKeep the list of column names to retain
     * @return the data frame
     */
    public DataFrame keepColumns(ListIterable<String> columnNamesToKeep)
    {
        ListIterable<DfColumn> columnsToKeep = columnNamesToKeep.collect(this::getColumnNamed); // will throw if a column doesn't exist

        MutableList<String> columnNamesToDrop = this.columns.collect(DfColumn::getName).reject(columnNamesToKeep::contains);

        return this.dropColumns(columnNamesToDrop);
    }

    public DfCellComparator columnComparator(DataFrame other, String thisColumnName, String otherColumnName)
    {
        final DfColumn thisColumn = this.getColumnNamed(thisColumnName);
        final DfColumn otherColumn = other.getColumnNamed(otherColumnName);

        return thisColumn.columnComparator(otherColumn);
    }

    /**
     * Appends one or more columns to this dataframe based on value lookup in another data frame. If more than one value
     * matches a lookup key, the first matching value is used.
     *
     * @param joinDescriptor - a descriptor, which specifies join keys, values to select, default values, and other join
     *                       parameters
     * @return this dataframe
     */
    public DataFrame lookup(DfJoin joinDescriptor)
    {
        DataFrame target = joinDescriptor.joinTo();
        DfIndex index = new DfIndex(target, joinDescriptor.joinToColumnNames());

        ListIterable<DfColumn> columnsToSelectFrom = joinDescriptor
                .selectFromJoined()
                .collect(target::getColumnNamed);

        ListIterable<DfColumn> columnsToLookup = joinDescriptor.columnsToLookup().collect(this::getColumnNamed);

        columnsToSelectFrom.forEachInBoth(joinDescriptor.columnNameAliases(),
                (col, alias) -> this.addColumn(alias, col.getType()));

        ListIterable<DfColumn> addedColumns = joinDescriptor.columnNameAliases().collect(this::getColumnNamed);

        for (int rowIndex = 0; rowIndex < this.rowCount(); rowIndex++)
        {
            int finalRowIndex = rowIndex; // to pass as a lambda parameter

            ListIterable<Object> lookupKey = columnsToLookup.collect(col -> col.getObject(finalRowIndex));

            IntList found = index.getRowIndicesAtKey(lookupKey);

            if (found.isEmpty())
            {
                // default if specified, otherwise null
                if (joinDescriptor.valuesIfAbsent().notEmpty())
                {
                    addedColumns.forEachInBoth(joinDescriptor.valuesIfAbsent(), DfColumn::addObject);
                }
                else
                {
                    addedColumns.forEach(DfColumn::addEmptyValue);
                }
            }
            else
            {
                // use the first row
                int targetRowIndex = found.get(0);

                columnsToSelectFrom.forEachInBoth(addedColumns, (selectFrom, addTo) -> addTo.addObject(selectFrom.getObject(targetRowIndex)));
            }
        }

        return this.seal();
    }

    private enum JoinType
    {
        INNER_JOIN, OUTER_JOIN, JOIN_WITH_COMPLEMENTS;

        public boolean isOuterJoin()
        {
            return this == OUTER_JOIN;
        }

        public boolean isJoinWithComplements()
        {
            return this == JOIN_WITH_COMPLEMENTS;
        }
    }
}
