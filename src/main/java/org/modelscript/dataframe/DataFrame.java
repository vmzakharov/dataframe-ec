package org.modelscript.dataframe;

import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.eclipse.collections.impl.list.Interval;
import org.eclipse.collections.impl.list.primitive.IntInterval;
import org.eclipse.collections.impl.utility.ArrayIterate;
import org.modelscript.expr.DataFrameEvalContext;
import org.modelscript.expr.EvalContext;
import org.modelscript.expr.Expression;
import org.modelscript.expr.value.BooleanValue;
import org.modelscript.expr.value.LongValue;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.value.ValueType;
import org.modelscript.expr.visitor.InMemoryEvaluationVisitor;
import org.modelscript.util.ExpressionParserHelper;

import java.util.Arrays;
import java.util.Comparator;

public class DataFrame
{
    private final String name;
    private final MutableMap<String, DfColumn> columnsByName = Maps.mutable.of();
    private final MutableList<DfColumn> columnsInOrder = Lists.mutable.of();
    private int rowCount = 0;

    private final DataFrameEvalContext evalContext; // todo: make threadlocal
    private IntList rowIndex = null;

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

    public DataFrame addColumn(DfColumn newColumn)
    {
        // todo: would like to make it impossible in the first place
        if (newColumn.getDataFrame() != this)
        {
            throw new RuntimeException("Mixing columns from different data frames: attempting to add"
                    + " to '" + this.getName() + "' column '"
                    + newColumn.getName() + "' already bound to '" + newColumn.getDataFrame().getName() + "'");
        }
        this.columnsByName.put(newColumn.getName(), newColumn);
        this.columnsInOrder.add(newColumn);
        return this;
    }

    public DfColumn getColumnNamedOrNull(String columnName)
    {
        return this.columnsByName.get(columnName);
    }

    public DfColumn getColumnNamed(String columnName)
    {
        DfColumn column = this.columnsByName.get(columnName);
        if (column == null)
        {
            ErrorReporter.reportError("Column '" + columnName + "' does not exist in DataFrame '" + this.getName() + "'");
        }
        return column;
    }

    public DfColumn getColumnAt(int columnIndex)
    {
        return this.columnsInOrder.get(columnIndex);
    }

    public void addRow(ListIterable<Value> rowValues)
    {
        rowValues.forEachWithIndex((v, i) -> this.columnsInOrder.get(i).addValue(v));
        this.rowCount++;
    }

    public String asCsvString()
    {
        StringBuilder s = new StringBuilder();

        s.append(this.columnsInOrder.collect(DfColumn::getName).makeString());
        s.append('\n');

        int columnCount = this.columnCount();
        String[] row = new String[columnCount];
        for (int rowIndex = 0; rowIndex < this.rowCount(); rowIndex++)
        {
            for (int columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {
                row[columnIndex] = this.getStringLiteral(rowIndex,columnIndex);
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
        columnsInOrder.forEach(dfColumn -> dfColumn.addEmptyValue());
        rowCount++;
        return this;
    }

    public DataFrame addRow(Object... values)
    {
        ArrayIterate.forEachWithIndex(values, (v, i) -> this.columnsInOrder.get(i).addObject(v));
        rowCount++;
        return this;
    }

    public int columnCount()
    {
        return this.columnsInOrder.size();
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
    
    public Object getObject(int rowIndex, int columnIndex)
    {
        return this.columnsInOrder.get(columnIndex).getObject(this.rowIndexMap(rowIndex));
    }

    public Value getValue(int rowIndex, int columnIndex)
    {
        return this.columnsInOrder.get(columnIndex).getValue(this.rowIndexMap(rowIndex));
    }

    public Value getValue(String columnName, int rowIndex)
    {
        return this.columnsByName.get(columnName).getValue(this.rowIndexMap(rowIndex));
    }

    public Value getValueAtPhysicalRow(String columnName, int rowIndex)
    {
        return this.columnsByName.get(columnName).getValue(rowIndex);
    }

    public String getStringLiteral(int rowIndex, int columnIndex)
    {
        return this.columnsInOrder.get(columnIndex).getValueAsStringLiteral(this.rowIndexMap(rowIndex));
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

    public DfLongColumn getLongColumn(String columnName)
    {
        return (DfLongColumn) this.getColumnNamed(columnName);
    }

    public DfDoubleColumn getDoubleColumn(String columnName)
    {
        return (DfDoubleColumn) this.getColumnNamed(columnName);
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

    public void seal()
    {
        MutableIntList storedColumnsSizes = this.columnsInOrder.select(DfColumn::isStored).collectInt(DfColumn::getSize);
        if (storedColumnsSizes.size() == 0)
        {
            this.rowCount = 0;
        }
        else
        {
            this.rowCount = storedColumnsSizes.get(0);
            if (storedColumnsSizes.detectIfNone(e -> e != this.rowCount, -1) != -1)
            {
                throw new RuntimeException(
                        "Stored column sizes are not the same when attempting to seal data frame '" + this.getName() + "'");
            }
        }
    }

    public DataFrame sum(ListIterable<String> columnNames)
    {
        ListIterable<DfColumn> columnsToSum = this.getColumnsToAggregate(columnNames);

        DataFrame summedDataFrame = new DataFrame("Sum Of " + this.getName());

        columnsToSum.forEach(each -> summedDataFrame.addColumn(each.getName(), each.getType()));

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

        ErrorReporter.reportError(nonNumericColumns.notEmpty(),
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

        // todo: implement index as a method on DataFrame;
        DfUniqueIndex index = new DfUniqueIndex(summedDataFrame, columnsToGroupByNames);

        for (int i = 0; i < this.rowCount; i++)
        {
            int accIndex = index.getRowIndexFor(this, i);
            for (int colIndex = 0; colIndex < columnsToSum.size(); colIndex++)
            {
                accumulatorColumns.get(colIndex).incrementFrom(accIndex, columnsToSum.get(colIndex), i);
            }
        }

        return summedDataFrame;
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

    private void copyRowFrom(DataFrame source, int rowIndex)
    {
        this.columnsInOrder.each(
                each -> {
                    if (each.isStored())
                    {
                        source.getColumnNamed(each.getName()).addRowToColumn(rowIndex, each);
                    }
                }
        );
    }

    public DataFrame cloneStructure()
    {
        DataFrame cloned = new DataFrame(this.getName() + "-copy");

        this.columnsInOrder.each(each -> each.cloneSchemaAndAttachTo(cloned));

        return cloned;
    }

    public void sortBy(ListIterable<String> columnsToSortByNames)
    {
        ListIterable<DfColumn> columnsToSortBy = this.columnsNamed(columnsToSortByNames);

        DfTuple[] tuples = new DfTuple[this.rowCount];
        for (int i = 0; i < this.rowCount; i++)
        {
            tuples[i] = this.rowToTuple(i, columnsToSortBy);
        }

        Arrays.sort(tuples);
        this.rowIndex = ArrayIterate.collectInt(tuples, DfTuple::getRowIndex);
    }

    public void sortByExpression(String expressionString)
    {
        this.unsort();
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
        Comparable<Object>[] values = new Comparable[columnsToCollect.size()];
        for (int i = 0; i < size; i++)
        {
            values[i] = (Comparable<Object>) columnsToCollect.get(i).getObject(rowIndex);
        }
        return new DfTuple(rowIndex, values);
    }

    public void unsort()
    {
        this.rowIndex = null;
    }

    private ListIterable<DfColumn> columnsNamed(ListIterable<String> columnNames)
    {
        return columnNames.collect(this::getColumnNamed);
    }
}
