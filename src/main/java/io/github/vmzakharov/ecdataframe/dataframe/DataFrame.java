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

public class DataFrame
{
    private final String name;
    private final MutableMap<String, DfColumn> columnsByName = Maps.mutable.of();
    private final MutableList<DfColumn> columns = Lists.mutable.of();
    private int rowCount = 0;

    private final DataFrameEvalContext evalContext; // todo: make threadlocal
    private IntList rowIndex = null;
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

    public DfColumn getColumnAt(int columnIndex)
    {
        return this.columns.get(columnIndex);
    }

    public void addRow(ListIterable<Value> rowValues)
    {
        rowValues.forEachWithIndex((v, i) -> this.columns.get(i).addValue(v));
        this.rowCount++;
    }

    public String asCsvString()
    {
        StringBuilder s = new StringBuilder();

        s.append(this.columns.collect(DfColumn::getName).makeString());
        s.append('\n');

        int columnCount = this.columnCount();
        String[] row = new String[columnCount];
        for (int rowIndex = 0; rowIndex < this.rowCount(); rowIndex++)
        {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {
                row[columnIndex] = this.getValueAsStringLiteral(rowIndex,columnIndex);
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
        columns.forEach(DfColumn::addEmptyValue);
        rowCount++;
        return this;
    }

    public DataFrame addRow(Object... values)
    {
        ArrayIterate.forEachWithIndex(values, (v, i) -> this.columns.get(i).addObject(v));
        rowCount++;
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

    public DataFrame addColumn(String name, ValueType type)
    {
        switch (type)
        {
            case LONG:
                this.addLongColumn(name);
                break;
            case DOUBLE:
                this.addDoubleColumn(name);
                break;
            case STRING:
                this.addStringColumn(name);
                break;
            case DATE:
                this.addDateColumn(name);
                break;
            default:
                throw new RuntimeException("Cannot add a column for values of type " + type);
        }
        return this;
    }

    public DataFrame addColumn(String name, ValueType type, String expressionAsString)
    {
        switch (type)
        {
            case LONG:
                this.addLongColumn(name, expressionAsString);
                break;
            case DOUBLE:
                this.addDoubleColumn(name, expressionAsString);
                break;
            case STRING:
                this.addStringColumn(name, expressionAsString);
                break;
            case DATE:
                this.addDateColumn(name, expressionAsString);
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
            return this.rowIndex.get(virtualRowIndex);
        }
        return virtualRowIndex;
    }

    private boolean isIndexed()
    {
        return this.rowIndex != null;
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

    public DataFrame sum(ListIterable<String> columnsToSumNames)
    {
        ListIterable<DfColumn> columnsToSum = this.getColumnsToAggregate(columnsToSumNames);

        DataFrame summedDataFrame = new DataFrame("Sum Of " + this.getName());

        columnsToSum.forEachWith(DfColumn::cloneSchemaAndAttachTo, summedDataFrame);

        ListIterable<Number> sums = columnsToSum.collect(
                each -> (each instanceof DfDoubleColumn) ? ((DfDoubleColumn) each).sum() : ((DfLongColumn) each).sum()
        );

        summedDataFrame.addRow(sums.toArray());
        return summedDataFrame;
    }

    private ListIterable<DfColumn> getColumnsToAggregate(ListIterable<String> columnNames)
    {
        ListIterable<DfColumn> columnsToAggregate = this.columnsNamed(columnNames);

        ListIterable<DfColumn> nonNumericColumns = columnsToAggregate.reject(each -> each.getType().isNumber());

        ErrorReporter.reportAndThrow(nonNumericColumns.notEmpty(),
                "Attempting to aggregate non-numeric columns: "
                + nonNumericColumns.collect(DfColumn::getName).makeString() + " in data frame '" + this.getName() + "'");

        return columnsToAggregate;
    }

    public DataFrame sumBy(ListIterable<String> columnsToSumNames, ListIterable<String> columnsToGroupByNames)
    {
        ListIterable<DfColumn> columnsToSum = this.getColumnsToAggregate(columnsToSumNames);

        DataFrame summedDataFrame = new DataFrame("Sum Of " + this.getName());

        columnsToGroupByNames
            .collect(this::getColumnNamed)
            .forEachWith(DfColumn::cloneSchemaAndAttachTo, summedDataFrame);

        columnsToSum
            .forEachWith(DfColumn::cloneSchemaAndAttachTo, summedDataFrame);

        ListIterable<DfColumn> accumulatorColumns = summedDataFrame.columnsNamed(columnsToSumNames);

        // todo: consider implementing index as a structure in DataFrame
        DfUniqueIndex index = new DfUniqueIndex(summedDataFrame, columnsToGroupByNames);

        for (int i = 0; i < this.rowCount; i++)
        {
            ListIterable<Object> keyValue = index.computeKeyFrom(this, i);
            int accIndex = index.getRowIndexAtKeyIfAbsentAdd(keyValue);
            for (int colIndex = 0; colIndex < columnsToSum.size(); colIndex++)
            {
                accumulatorColumns.get(colIndex).incrementFrom(accIndex, columnsToSum.get(colIndex), i);
            }
        }

        return summedDataFrame;
    }

    public DataFrame sumByWithIndex(ListIterable<String> columnsToSumNames, ListIterable<String> columnsToGroupByNames)
    {
        MutableList<MutableIntList> sumIndex = Lists.mutable.of();

        ListIterable<DfColumn> columnsToSum = this.getColumnsToAggregate(columnsToSumNames);

        DataFrame summedDataFrame = new DataFrame("Sum Of " + this.getName());

        columnsToGroupByNames
                .collect(this::getColumnNamed)
                .forEachWith(DfColumn::cloneSchemaAndAttachTo, summedDataFrame);

        columnsToSum
                .forEachWith(DfColumn::cloneSchemaAndAttachTo, summedDataFrame);

        ListIterable<DfColumn> accumulatorColumns = summedDataFrame.columnsNamed(columnsToSumNames);

        DfUniqueIndex index = new DfUniqueIndex(summedDataFrame, columnsToGroupByNames);

        for (int i = 0; i < this.rowCount; i++)
        {
            ListIterable<Object> keyValue = index.computeKeyFrom(this, i);
            int accIndex = index.getRowIndexAtKeyIfAbsentAdd(keyValue);

            while (sumIndex.size() <= accIndex)
            {
                 sumIndex.add(IntLists.mutable.of());
            }

            sumIndex.get(accIndex).add(i);

            for (int colIndex = 0; colIndex < columnsToSum.size(); colIndex++)
            {
                accumulatorColumns.get(colIndex).incrementFrom(accIndex, columnsToSum.get(colIndex), i);
            }
        }

        summedDataFrame.aggregateIndex = sumIndex;

        return summedDataFrame;
    }

    public Twin<DataFrame> selectAndRejectBy(String filterExpressionString)
    {
        DataFrame selected = this.cloneStructure(this.name+"-selected");
        DataFrame rejected = this.cloneStructure(this.name+"-rejected");

        DataFrameEvalContext context = new DataFrameEvalContext(this);
        Expression filterExpression = ExpressionParserHelper.toExpression(filterExpressionString);
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
        DataFrame filtered = this.cloneStructure();
        DataFrameEvalContext context = new DataFrameEvalContext(this);
        Expression filterExpression = ExpressionParserHelper.toExpression(filterExpressionString);
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

        DataFrame filtered = this.cloneStructure();
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

    public DataFrame cloneStructure()
    {
        return this.cloneStructure(this.getName() + "-copy");
    }

    public DataFrame cloneStructure(String newName)
    {
        DataFrame cloned = new DataFrame(newName);

        this.columns.each(each -> each.cloneSchemaAndAttachTo(cloned));

        return cloned;
    }

    public void sortBy(ListIterable<String> columnsToSortByNames)
    {
        this.unsort();

        // doing this before check for rowCount to make sure that the sort columns exist even if the data frame is empty
        ListIterable<DfColumn> columnsToSortBy = this.columnsNamed(columnsToSortByNames);

        if (this.rowCount == 0)
        {
            this.rowIndex = IntLists.immutable.empty();
            return;
        }

        DfTuple[] tuples = new DfTuple[this.rowCount];
        for (int i = 0; i < this.rowCount; i++)
        {
            tuples[i] = this.rowToTuple(i, columnsToSortBy);
        }

        MutableIntList indexes = IntInterval.zeroTo(this.rowCount - 1).toList();
        indexes.sortThisBy(i -> tuples[i]);

        this.rowIndex = indexes;
    }

    public void sortByExpression(String expressionString)
    {
        this.unsort();
        if (this.rowCount == 0)
        {
            this.rowIndex = IntLists.immutable.empty();
            return;
        }

        Expression expression = ExpressionParserHelper.toExpression(expressionString);
        MutableIntList indexes = IntInterval.zeroTo(this.rowCount - 1).toList();
        indexes.sortThisBy(i -> this.evaluateExpression(expression, i));
        this.rowIndex = indexes;
    }

    public Value evaluateExpression(Expression expression, int rowIndex)
    {
        this.getEvalContext().setRowIndex(rowIndex);
        return expression.evaluate(new InMemoryEvaluationVisitor(evalContext));
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
        this.rowIndex = null;
    }

    private ListIterable<DfColumn> columnsNamed(ListIterable<String> columnNames)
    {
        return columnNames.collect(this::getColumnNamed);
    }

    /**
     * Creates a new data frame which is a union of this data frame and the data frame passed as the parameter.
     * The data frame schemas must match.
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
     * creates a new data frame, which contain a subset of rows of this data frame for the rows with the btimap flag not
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
     * @param filterExpressionString the expression to set the flags by
     */
    public void flagRowsBy(String filterExpressionString)
    {
        this.bitmap = BooleanArrayList.newWithNValues(this.rowCount, false);

        DataFrameEvalContext context = new DataFrameEvalContext(this);
        Expression filterExpression = ExpressionParserHelper.toExpression(filterExpressionString);
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
     * exists.
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
     * @param other the data frame to join to
     * @param thisJoinColumnName the name of the column in the data frame to use a join key
     * @param otherJoinColumnName the name of the column in the data frame to use a join
     * @return a data frame that is a join of this data frame and the data frame passed as a parameter
     */
    // todo: multi column join
    // todo: outer join
    // todo: data frame difference
    // todo: deal with column name collision
    // todo: do not override sort order (use external sort)
    public DataFrame join(DataFrame other, String thisJoinColumnName, String otherJoinColumnName)
    {
        DataFrame joined = this.cloneStructure(this.getName() + "_" + other.getName());

        other.columns
                .reject(c -> c.getName().equals(otherJoinColumnName))
                .forEach(c -> c.cloneSchemaAndAttachTo(joined));

        this.sortBy(Lists.immutable.of(thisJoinColumnName));
        other.sortBy(Lists.immutable.of(otherJoinColumnName));

        int thisRowIndex = 0;
        int otherRowIndex = 0;

        int thisRowCount = this.rowCount();
        int otherRowCount = other.rowCount();

        Object[] rowData = new Object[joined.columnCount()];

        ListIterable<String> theseColumnNames = this.columns.collect(DfColumn::getName);
        int theseColumnCount = theseColumnNames.size();
        ListIterable<String> otherColumnNames = other.columns.collect(DfColumn::getName).rejectWith(String::equals, otherJoinColumnName);

        while (thisRowIndex < thisRowCount && otherRowIndex  < otherRowCount)
        {
            int comparison = ((Comparable<Object>) this.getObject(thisJoinColumnName, thisRowIndex)).compareTo(
                    other.getObject(otherJoinColumnName, otherRowIndex));

            if (comparison == 0)
            {
                final int thisFinalRequired = thisRowIndex;
                final int otherFinalRequired = otherRowIndex;

                theseColumnNames.forEachWithIndex((colName, i) -> rowData[i] = this.getObject(colName, thisFinalRequired));
                otherColumnNames.forEachWithIndex((colName, i) -> rowData[theseColumnCount + i] = other.getObject(colName, otherFinalRequired));

                joined.addRow(rowData);
                thisRowIndex++;
                otherRowIndex++;
            }
            else if (comparison < 0)
            {
                thisRowIndex++;
            }
            else
            {
                otherRowIndex++;
            }
        }

        return joined;
    }
}
