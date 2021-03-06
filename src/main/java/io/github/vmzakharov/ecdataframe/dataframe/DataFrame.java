package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.DataFrameEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.block.predicate.primitive.BooleanPredicate;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.BooleanList;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableBooleanList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.MutableMap;
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

    private final DataFrameEvalContext evalContext; // todo: make threadlocal?
    private IntList virtualRowMap = null;
    private boolean poolingEnabled = false;

    private MutableBooleanList bitmap = null;

    private MutableList<MutableIntList> aggregateIndex = null;

    public DataFrame(String newName)
    {
        this.name = newName;
        this.evalContext = new DataFrameEvalContext(this);
    }

    public DataFrame addStringColumn(String newColumnName)
    {
        return this.addColumn(new DfStringColumnStored(this, newColumnName));
    }

    public DataFrame addStringColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumn(new DfStringColumnComputed(this, newColumnName, expressionAsString));
    }

    public DataFrame addLongColumn(String newColumnName)
    {
        return this.addColumn(new DfLongColumnStored(this, newColumnName));
    }

    public DataFrame addLongColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumn(new DfLongColumnComputed(this, newColumnName, expressionAsString));
    }

    public DataFrame addDoubleColumn(String newColumnName)
    {
        return this.addColumn(new DfDoubleColumnStored(this, newColumnName));
    }

    public DataFrame addDoubleColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumn(new DfDoubleColumnComputed(this, newColumnName, expressionAsString));
    }

    public DataFrame addDateColumn(String newColumnName)
    {
        return this.addColumn(new DfDateColumnStored(this, newColumnName));
    }

    public DataFrame addDateColumn(String newColumnName, String expressionAsString)
    {
        throw new UnsupportedOperationException("Cannot add calculated date columns... yet");
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

        ErrorReporter.reportAndThrow(column == null,
                "Column '" + columnName + "' does not exist in data frame '" + this.getName() + "'");

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
     * @return a string representation of the data frame.
     */
    public String asCsvString()
    {
        return this.asCsvString(-1);
    }

    /**
     * Convert the data frame into a multi-line CSV string. The output will include column headers.
     * @param limit number of rows to convert, all rows if the value if  negative. If the value is zero the result will
     *              only contain column names.
     * @return a string representation of the data frame.
     */
    public String asCsvString(int limit)
    {
        StringBuilder s = new StringBuilder();

        s.append(this.columns.collect(DfColumn::getName).makeString());
        s.append('\n');

        int columnCount = this.columnCount();
        String[] row = new String[columnCount];

        int last = limit < 0 ? this.rowCount() : Math.max(limit, this.rowCount);

        for (int rowIndex = 0; rowIndex < last; rowIndex++)
        {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {
                row[columnIndex] = this.getValueAsStringLiteral(rowIndex, columnIndex);
            }
            s.append(ArrayIterate.makeString(row));
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

    private int rowIndexMap(int virtualRowIndex)
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
        return this.columnsByName.get(columnName).getObject(this.rowIndexMap(rowIndex));
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
        return this.getStringColumn(columnName).getString(this.rowIndexMap(rowIndex));
    }

    public double getDouble(String columnName, int rowIndex)
    {
        return this.getDoubleColumn(columnName).getDouble(this.rowIndexMap(rowIndex));
    }

    public LocalDate getDate(String columnName, int rowIndex)
    {
        return this.getDateColumn(columnName).getDate(this.rowIndexMap(rowIndex));
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
     * indicates that no further updates can be made to this data frame.
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
            if (storedColumnsSizes.detectIfNone(e -> e != this.rowCount, -1) != -1)
            {
                ErrorReporter.reportAndThrow(
                        "Stored column sizes are not the same when attempting to seal data frame '" + this.getName() + "'");
            }
        }

        this.columns.forEach(DfColumn::seal);
        return this;
    }

    public DataFrame sum(ListIterable<String> columnsToAggregateNames)
    {
        return this.aggregate(columnsToAggregateNames.collect(AggregateFunction::sum));
    }

    public DataFrame aggregate(ListIterable<AggregateFunction> aggregators)
    {
        ListIterable<DfColumn> columnsToAggregate = this.getColumnsToAggregate(aggregators.collect(AggregateFunction::getColumnName));

        DataFrame summedDataFrame = new DataFrame("Aggregate Of " + this.getName());

        columnsToAggregate.forEachInBoth(aggregators,
                (col, agg) -> summedDataFrame.addColumn(agg.getTargetColumnName(), agg.targetColumnType(col.getType()))
        );

        ListIterable<Number> aggregatedValues = columnsToAggregate.collectWithIndex(
                (each, index) -> each.aggregate(aggregators.get(index))
        );

        summedDataFrame.addRow(aggregatedValues.toArray());
        return summedDataFrame;
    }

    private ListIterable<DfColumn> getColumnsToAggregate(ListIterable<String> columnNames)
    {
        ListIterable<DfColumn> columnsToAggregate = this.columnsNamed(columnNames);

//        ListIterable<DfColumn> nonNumericColumns = columnsToAggregate.reject(each -> each.getType().isNumber());
//
//        ErrorReporter.reportAndThrow(nonNumericColumns.notEmpty(),
//                "Attempting to aggregate non-numeric columns: "
//                        + nonNumericColumns.collect(DfColumn::getName).makeString() + " in data frame '" + this.getName() + "'");

        return columnsToAggregate;
    }

    public DataFrame aggregateBy(
            ListIterable<AggregateFunction> aggregators,
            ListIterable<String> columnsToGroupByNames)
    {
        return this.aggregateByWithIndex(aggregators, columnsToGroupByNames, false);
    }

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
                .collect(aggregatedDataFrame::getColumnNamed)
                .tap(DfColumn::enableNulls);

        DfUniqueIndex index = new DfUniqueIndex(aggregatedDataFrame, columnsToGroupByNames);

        for (int rowIndex = 0; rowIndex < this.rowCount; rowIndex++)
        {
            ListIterable<Object> keyValue = index.computeKeyFrom(this, rowIndex);
            int aggregateRowIndex = index.getRowIndexAtKeyIfAbsentAdd(keyValue);

            if (createSourceRowIdIndex)
            {
                while (sumIndex.size() <= aggregateRowIndex)
                {
                    sumIndex.add(IntLists.mutable.of());
                }

                sumIndex.get(aggregateRowIndex).add(rowIndex);
            }

            counts[aggregateRowIndex]++;

            for (int colIndex = 0; colIndex < columnsToAggregate.size(); colIndex++)
            {
                accumulatorColumns.get(colIndex).applyAggregator(aggregateRowIndex, columnsToAggregate.get(colIndex), rowIndex, aggregators.get(colIndex));
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

    private DataFrame selectByMarkValue(BooleanList marked, BooleanPredicate markAtIndexPredicate)
    {

        DataFrame filtered = this.cloneStructure(this.getName() + "-selected");
        for (int i = 0; i < this.rowCount; i++)
        {
            if (markAtIndexPredicate.accept(marked.get(i)))
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

    public DataFrame cloneStructure(String newName)
    {
        DataFrame cloned = new DataFrame(newName);

        this.columns.each(each -> each.cloneSchemaAndAttachTo(cloned));

        return cloned;
    }

    /**
     * creates an empty data frame with the same schema as this one except computed columns are converted to stored
     * columns of the same type
     * @param newName the name for the new data frame
     * @return an empty data frame with the provided name and new schema
     */
    public DataFrame cloneStructureAsStored(String newName)
    {
        DataFrame cloned = new DataFrame(newName);

        this.columns.each(e -> cloned.addColumn(e.getName(), e.getType()));

        return cloned;
    }

    public void sortBy(ListIterable<String> columnsToSortByNames)
    {
        this.unsort();

        // doing this before check for rowCount to make sure that the sort columns exist even if the data frame is empty
        ListIterable<DfColumn> columnsToSortBy = this.columnsNamed(columnsToSortByNames);

        if (this.rowCount == 0)
        {
            this.virtualRowMap = IntLists.immutable.empty();
            return;
        }

        DfTuple[] tuples = new DfTuple[this.rowCount];
        for (int i = 0; i < this.rowCount; i++)
        {
            tuples[i] = this.rowToTuple(i, columnsToSortBy);
        }

        MutableIntList indexes = IntInterval.zeroTo(this.rowCount - 1).toList();
        indexes.sortThisBy(i -> tuples[i]);

        this.virtualRowMap = indexes;
    }

    public void sortByExpression(String expressionString)
    {
        this.unsort();
        if (this.rowCount == 0)
        {
            this.virtualRowMap = IntLists.immutable.empty();
            return;
        }

        Expression expression = ExpressionParserHelper.DEFAULT.toExpression(expressionString);
        MutableIntList indexes = IntInterval.zeroTo(this.rowCount - 1).toList();
        indexes.sortThisBy(i -> this.evaluateExpression(expression, i));
        this.virtualRowMap = indexes;
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
     * Creates a new data frame which is a union of this data frame and the data frame passed as the parameter.
     * The data frame schemas must match.
     *
     * @param other the data frame to union with
     * @return a data frame with the rows being the union of the rows this data frame and the parameter
     */
    public DataFrame union(DataFrame other)
    {
        ErrorReporter.reportAndThrow(this.columnCount() != other.columnCount(), "Attempting to union data frames with different number of columns");
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
    public void enableBitmap()
    {
        this.bitmap = BooleanArrayList.newWithNValues(this.rowCount, false);
    }

    public void disableBitmap()
    {
        this.bitmap = null;
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
     * creates a new data frame, which contain a subset of rows of this data frame for the rows with the bitmap flags set
     * (i.e. equals to true). This is the behavior the opposite of {@code selectNotMarked}
     *
     * @return a data frame containing the filtered subset of rows
     */
    public DataFrame selectFlagged()
    {
        return this.selectByMarkValue(this.bitmap, mark -> mark);
    }

    /**
     * creates a new data frame, which contain a subset of rows of this data frame for the rows with the bitmap flag not
     * set (i.e. equals to false). This is the behavior the opposite of {@code selectMarked}
     *
     * @return a data frame containing the filtered subset of rows
     */
    public DataFrame selectNotFlagged()
    {
        return this.selectByMarkValue(this.bitmap, mark -> !mark);
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
     * A very basic join - creates a data frame that is this an join of this data frame and another one, based on
     * the key column values. Rows with the same values of the key column will be combined in the resulting data frame
     * into one wide row. This is an inner join so rows for which there is no match in the other data frame will not be
     * present in the join.
     *
     * @param other               the data frame to join to
     * @param thisJoinColumnName  the name of the column in the data frame to use a join key
     * @param otherJoinColumnName the name of the column in the data frame to use a join
     * @return a data frame that is a join of this data frame and the data frame passed as a parameter
     */
    public DataFrame join(DataFrame other, String thisJoinColumnName, String otherJoinColumnName)
    {
        return this.join(other, false, thisJoinColumnName, otherJoinColumnName);
    }

    /**
     * A basic outer join - creates a data frame that is this an join of this data frame and another one, based on
     * the key column values. Rows with the same values of the key column will be combined in the resulting data frame
     * into one wide row. The rows for which there is no match in the other data frame will have the missing values
     * filled with nulls for object column types or zeros for numeric column types.
     *
     * @param other               the data frame to join to
     * @param thisJoinColumnName  the name of the column in the data frame to use a join key
     * @param otherJoinColumnName the name of the column in the data frame to use a join
     * @return a data frame that is a join of this data frame and the data frame passed as a parameter
     */
    public DataFrame outerJoin(DataFrame other, String thisJoinColumnName, String otherJoinColumnName)
    {
        return this.join(other, true, thisJoinColumnName, otherJoinColumnName);
    }

    // todo: multi column join
    // todo: data frame difference
    // todo: do not override sort order (use external sort)
    private DataFrame join(DataFrame other, boolean outerJoin, String thisJoinColumnName, String otherJoinColumnName)
    {
        DataFrame joined = this.cloneStructureAsStored(this.getName() + "_" + other.getName());

        MutableList<String> uniqueColumnNames = this.columns.collect(DfColumn::getName);

        MutableMap<String, String> otherColumnNameMap = Maps.mutable.of();

        other.columns.collect(DfColumn::getName).forEach(e ->
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

        other.columns
                .reject(c -> c.getName().equals(otherJoinColumnName))
                .forEach(c -> joined.addColumn(otherColumnNameMap.get(c.getName()), c.getType()));

        this.sortBy(Lists.immutable.of(thisJoinColumnName));
        other.sortBy(Lists.immutable.of(otherJoinColumnName));

        int thisRowIndex = 0;
        int otherRowIndex = 0;

        int thisRowCount = this.rowCount();
        int otherRowCount = other.rowCount();

        Object[] rowData = new Object[joined.columnCount()];

        ListIterable<String> theseColumnNames = this.columns.collect(DfColumn::getName);
        int theseColumnCount = theseColumnNames.size();

        int joinColumnIndex = theseColumnNames.detectIndex(e -> e.equals(thisJoinColumnName));

        ListIterable<String> otherColumnNames = other.columns.collect(DfColumn::getName).rejectWith(String::equals, otherJoinColumnName);

        int comparison;
        while (thisRowIndex < thisRowCount && otherRowIndex < otherRowCount)
        {
            comparison = ((Comparable<Object>) this.getObject(thisJoinColumnName, thisRowIndex)).compareTo(
                    other.getObject(otherJoinColumnName, otherRowIndex));

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
                    if (outerJoin)
                    {
                        Arrays.fill(rowData, null);
                        theseColumnNames.forEachWithIndex((colName, i) -> rowData[i] = this.getObject(colName, thisMakeFinal));

                        joined.addRow(rowData);
                    }
                    thisRowIndex++;
                }
                else
                {
                    // the other side is behind
                    if (outerJoin)
                    {
                        Arrays.fill(rowData, null);
                        rowData[joinColumnIndex] = other.getObject(otherJoinColumnName, otherMakeFinal);
                        otherColumnNames.forEachWithIndex((colName, i) -> rowData[theseColumnCount + i] = other.getObject(colName, otherMakeFinal));

                        joined.addRow(rowData);
                    }
                    otherRowIndex++;
                }
            }
        }

        //   leftovers go here
        if (outerJoin)
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
                rowData[joinColumnIndex] = other.getObject(otherJoinColumnName, otherMakeFinal);
                otherColumnNames.forEachWithIndex((colName, i) -> rowData[theseColumnCount + i] = other.getObject(colName, otherMakeFinal));

                joined.addRow(rowData);

                otherRowIndex++;
            }
        }

        return joined;
    }

    /**
     * Drops the data frame columns with the names specified as the method parameter
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
     * @param columnNamesToKeep the list of column names to retain
     * @return the data frame
     */
    public DataFrame keepColumns(ListIterable<String> columnNamesToKeep)
    {
        ListIterable<DfColumn> columnsToKeep = columnNamesToKeep.collect(this::getColumnNamed); // will throw if a column doesn't exist

        MutableList<String> columnNamesToDrop = this.columns.collect(DfColumn::getName).reject(columnNamesToKeep::contains);

        return this.dropColumns(columnNamesToDrop);
    }
}
