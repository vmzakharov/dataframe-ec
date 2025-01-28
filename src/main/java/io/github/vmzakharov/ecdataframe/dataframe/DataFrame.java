package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dataset.HierarchicalDataSet;
import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.EvalContextAbstract;
import io.github.vmzakharov.ecdataframe.dsl.Expression;
import io.github.vmzakharov.ecdataframe.dsl.FunctionScript;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.dsl.visitor.ExpressionEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.TypeInferenceVisitor;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.BooleanIterable;
import org.eclipse.collections.api.DoubleIterable;
import org.eclipse.collections.api.FloatIterable;
import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.block.function.primitive.IntIntToIntFunction;
import org.eclipse.collections.api.block.predicate.primitive.IntPredicate;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.list.ImmutableList;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableBooleanList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.map.MapIterable;
import org.eclipse.collections.api.map.MutableMap;
import org.eclipse.collections.api.multimap.list.MutableListMultimap;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.api.tuple.Triplet;
import org.eclipse.collections.api.tuple.Twin;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.Maps;
import org.eclipse.collections.impl.factory.Multimaps;
import org.eclipse.collections.impl.factory.primitive.IntLists;
import org.eclipse.collections.impl.list.mutable.primitive.BooleanArrayList;
import org.eclipse.collections.impl.set.sorted.mutable.TreeSortedSet;
import org.eclipse.collections.impl.tuple.Tuples;
import org.eclipse.collections.impl.utility.ArrayIterate;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Supplier;

import static io.github.vmzakharov.ecdataframe.dataframe.DfColumnSortOrder.ASC;
import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

/**
 * Data Frame - a tabular data structure
 */
public class DataFrame
implements DfIterate
{
    private String name;

    private final MutableMap<String, DfColumn> columnsByName = Maps.mutable.of();
    private final MutableList<DfColumn> columns = Lists.mutable.of();

    private int rowCount = 0;

    private final ThreadLocal<DataFrameEvalContext> localEvalContext;
    private final ThreadLocal<ExpressionEvaluationVisitor> localEvalVisitor;

    private IntList virtualRowMap = null;
    private boolean poolingEnabled = false;

    private MutableBooleanList bitmap = null;

    private MutableList<MutableIntList> aggregateIndex = null;

    private final MutableMap<String, DfIndex> indices = Maps.mutable.of();

    public DataFrame(String newName)
    {
        this.name = newName;

        this.localEvalContext = ThreadLocal.withInitial(
                () -> new DataFrameEvalContext(DataFrame.this)
        );

        this.localEvalVisitor = ThreadLocal.withInitial(
                () -> new InMemoryEvaluationVisitor(DataFrame.this.localEvalContext.get())
        );

        this.resetBitmap();
    }

    public DataFrame addStringColumn(String newColumnName)
    {
        return this.addColumn(newColumnName, ValueType.STRING);
    }

    /**
     * Add a computed column of string type to this data frame
     * @deprecated use {@link #addColumn(String, String)} instead. The type of the column to add will be inferred from
     * the expression
     * @param newColumnName the name of the column to be added
     * @param expressionAsString the expression used to compute the column values
     * @return this data frame
     */
    public DataFrame addStringColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumnWithTypeValidation(newColumnName, ValueType.STRING, expressionAsString);
    }

    public DataFrame addStringColumn(String newColumnName, ListIterable<String> values)
    {
        this.attachColumn(new DfStringColumnStored(this, newColumnName, values));
        return this;
    }

    public DataFrame addLongColumn(String newColumnName)
    {
        return this.addColumn(newColumnName, ValueType.LONG);
    }

    /**
     * Add a computed column of long type to this data frame
     * @deprecated use {@link #addColumn(String, String)} instead. The type of the column to add will be inferred from
     * the expression
     * @param newColumnName the name of the column to be added
     * @param expressionAsString the expression used to compute the column values
     * @return this data frame
     */
    public DataFrame addLongColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumnWithTypeValidation(newColumnName, ValueType.LONG, expressionAsString);
    }

    public DataFrame addLongColumn(String newColumnName, LongIterable values)
    {
        this.attachColumn(new DfLongColumnStored(this, newColumnName, values));
        return this;
    }

    /**
     * Add a stored column of int type to this data frame
     * @param newColumnName the name of the column to be added
     * @return this data frame
     */
    public DataFrame addIntColumn(String newColumnName)
    {
        return this.addColumn(newColumnName, ValueType.INT);
    }

    /**
     * Add a computed column of int type to this data frame
     * @deprecated use {@link #addColumn(String, String)} instead. The type of the column to add will be inferred from
     * the expression
     * @param newColumnName the name of the column to be added
     * @param expressionAsString the expression used to compute the column values
     * @return this data frame
     */
    public DataFrame addIntColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumnWithTypeValidation(newColumnName, ValueType.INT, expressionAsString);
    }

    public DataFrame addIntColumn(String newColumnName, IntIterable values)
    {
        this.attachColumn(new DfIntColumnStored(this, newColumnName, values));
        return this;
    }

    /**
     * Add a stored column of boolean type to this data frame
     * @param newColumnName the name of the column to be added
     * @return this data frame
     */
    public DataFrame addBooleanColumn(String newColumnName)
    {
        return this.addColumn(newColumnName, ValueType.BOOLEAN);
    }

    public DataFrame addBooleanColumn(String newColumnName, BooleanIterable values)
    {
        this.attachColumn(new DfBooleanColumnStored(this, newColumnName, values));
        return this;
    }

    public DataFrame addFloatColumn(String newColumnName)
    {
        return this.addColumn(newColumnName, ValueType.FLOAT);
    }

    /**
     * Add a computed column of float type to this data frame
     * @deprecated use {@link #addColumn(String, String)} instead. The type of the column to add will be inferred from
     * the expression
     * @param newColumnName the name of the column to be added
     * @param expressionAsString the expression used to compute the column values
     * @return this data frame
     */
    public DataFrame addFloatColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumnWithTypeValidation(newColumnName, ValueType.FLOAT, expressionAsString);
    }

    public DataFrame addFloatColumn(String newColumnName, FloatIterable values)
    {
        this.attachColumn(new DfFloatColumnStored(this, newColumnName, values));
        return this;
    }

    public DataFrame addDoubleColumn(String newColumnName)
    {
        return this.addColumn(newColumnName, ValueType.DOUBLE);
    }

    /**
     * Add a computed column of double type to this data frame
     * @deprecated use {@link #addColumn(String, String)} instead. The type of the column to add will be inferred from
     * the expression
     * @param newColumnName the name of the column to be added
     * @param expressionAsString the expression used to compute the column values
     * @return this data frame
     */
    public DataFrame addDoubleColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumnWithTypeValidation(newColumnName, ValueType.DOUBLE, expressionAsString);
    }

    public DataFrame addDoubleColumn(String newColumnName, DoubleIterable values)
    {
        this.attachColumn(new DfDoubleColumnStored(this, newColumnName, values));
        return this;
    }

    public DataFrame addDateColumn(String newColumnName)
    {
        return this.addColumn(newColumnName, ValueType.DATE);
    }

    public DataFrame addDateColumn(String newColumnName, ListIterable<LocalDate> values)
    {
        this.attachColumn(new DfDateColumnStored(this, newColumnName, values));
        return this;
    }

    /**
     * Add a computed column of date type to this data frame
     * @deprecated use {@link #addColumn(String, String)} instead. The type of the column to add will be inferred from
     * the expression
     * @param newColumnName the name of the column to be added
     * @param expressionAsString the expression used to compute the column values
     * @return this data frame
     */
    public DataFrame addDateColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumnWithTypeValidation(newColumnName, ValueType.DATE, expressionAsString);
    }

    public DataFrame addDateTimeColumn(String newColumnName)
    {
        return this.addColumn(newColumnName, ValueType.DATE_TIME);
    }

    public DataFrame addDateTimeColumn(String newColumnName, ListIterable<LocalDateTime> values)
    {
        this.attachColumn(new DfDateTimeColumnStored(this, newColumnName, values));
        return this;
    }

    /**
     * Add a computed column of date/time type to this data frame
     * @deprecated use {@link #addColumn(String, String)} instead. The type of the column to add will be inferred from
     * the expression
     * @param newColumnName the name of the column to be added
     * @param expressionAsString the expression used to compute the column values
     * @return this data frame
     */
    public DataFrame addDateTimeColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumnWithTypeValidation(newColumnName, ValueType.DATE_TIME, expressionAsString);
    }

    public DataFrame addDecimalColumn(String newColumnName)
    {
        return this.addColumn(newColumnName, ValueType.DECIMAL);
    }

    /**
     * Add a computed column of decimal type to this data frame
     * @deprecated use {@link #addColumn(String, String)} instead. The type of the column to add will be inferred from
     * the expression
     * @param newColumnName the name of the column to be added
     * @param expressionAsString the expression used to compute the column values
     * @return this data frame
     */
    public DataFrame addDecimalColumn(String newColumnName, String expressionAsString)
    {
        return this.addColumnWithTypeValidation(newColumnName, ValueType.DECIMAL, expressionAsString);
    }

    public DataFrame addDecimalColumn(String newColumnName, ListIterable<BigDecimal> values)
    {
        this.attachColumn(new DfDecimalColumnStored(this, newColumnName, values));
        return this;
    }

    /**
     * Returns a string representation of the data frame, which consists of the data frame's name, the row count, and up
     * to the first 10 rows of its data. If the data frame contains more than 10 rows, the first 10 rows are followed by
     * the ellipsis punctuation mark ("...").
     *
     * @return a string representation of the data frame
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder()
                .append(this.getName())
                .append(" [")
                .append(this.rowCount)
                .append(" rows]")
                .append('\n')
                .append(this.asCsvString(10));

        if (this.rowCount() > 10)
        {
            sb.append("...\n");
        }

        return sb.toString();
    }

    private void attachColumn(DfColumn newColumn)
    {
        if (this.hasColumn(newColumn.getName()))
        {
            exceptionByKey("DF_DUPLICATE_COLUMN")
                    .with("columnName", newColumn.getName())
                    .with("dataFrameName", this.getName())
                    .fire();
        }

        this.columnsByName.put(newColumn.getName(), newColumn);
        this.columns.add(newColumn);

        if (this.isPoolingEnabled())
        {
            newColumn.enablePooling();
        }

        if (newColumn.isStored() && newColumn.getSize() > 0)
        {
            this.determineRowCount();
        }
    }

    public DfColumn getColumnNamed(String columnName)
    {
        DfColumn column = this.columnsByName.get(columnName);

        if (column == null)
        {
            exceptionByKey("DF_COLUMN_DOES_NOT_EXIST")
                    .with("columnName", columnName)
                    .with("dataFrameName", this.getName())
                    .fire();
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
     * @param limit number of rows to return, all rows if the value is negative. If the value is zero the result will
     *              only contain column names.
     * @return a CSV string representation of the data frame rows.
     */
    public String asCsvString(int limit)
    {
        StringBuilder s = new StringBuilder();

        s.append(this.columns.makeString(DfColumn::getName, "", ",", ""));
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
            exceptionByKey("DF_ADDING_ROW_TOO_WIDE")
                    .with("elementCount", values.length)
                    .with("columnCount", this.columnCount())
                    .fire();
        }

        ArrayIterate.forEachWithIndex(values, (v, i) -> this.columns.get(i).addObject(v));
        this.rowCount++;
        return this;
    }

    public int columnCount()
    {
        return this.columns.size();
    }

    /**
     * Sets the name of the data frame
     */
    public void setName(String newName)
    {
        this.name = newName;
    }

    /**
     * @return the name of this data frame
     */
    public String getName()
    {
        return this.name;
    }

    /**
     * Add a computed column of int type to this data frame. The column type will be inferred from the expression
     * provided
     *
     * @param columnName the name of the column to be added
     * @param expressionAsString the expression used to compute the column values
     * @return this data frame
     */
    public DataFrame addColumn(String columnName, String expressionAsString)
    {
        return this.addColumn(
                columnName,
                this.inferExpressionType(columnName, expressionAsString),
                expressionAsString);
    }

    /**
     * creates a stored column with the specified name of the specified type and attaches it to this dataframe.
     *
     * @param columnName the name of the column to be created
     * @param type       the type of the new column
     * @return this data frame
     */
    public DataFrame addColumn(String columnName, ValueType type)
    {
        this.newColumn(columnName, type);
        return this;
    }

    /**
     * creates a stored column with the specified name of the specified type and attaches it to this dataframe.
     *
     * @param columnName the name of the column to be created
     * @param type       the type of the new column
     * @return the newly created columns
     */
    public DfColumnStored newColumn(String columnName, ValueType type)
    {
        DfColumnStored created = this.createStoredColumn(columnName, type);
        this.attachColumn(created);
        return created;
    }

    private DfColumnStored createStoredColumn(String columnName, ValueType type)
    {
        return switch (type)
        {
            case LONG -> new DfLongColumnStored(this, columnName);
            case DOUBLE -> new DfDoubleColumnStored(this, columnName);
            case STRING -> new DfStringColumnStored(this, columnName);
            case DATE -> new DfDateColumnStored(this, columnName);
            case DATE_TIME -> new DfDateTimeColumnStored(this, columnName);
            case DECIMAL -> new DfDecimalColumnStored(this, columnName);
            case INT -> new DfIntColumnStored(this, columnName);
            case FLOAT -> new DfFloatColumnStored(this, columnName);
            case BOOLEAN -> new DfBooleanColumnStored(this, columnName);
            default -> throw exceptionByKey("DF_ADD_COL_UNKNOWN_TYPE")
                    .with("columnName", columnName)
                    .with("type", type)
                    .get();
        };
    }

    /**
     * creates a calculated column with the specified name of the specified type and attaches it to this dataframe.
     *
     * @param columnName         the name of the column to be created
     * @param type               the type of the new column
     * @param expressionAsString the expression
     * @return this data frame
     */
    public DataFrame addColumn(String columnName, ValueType type, String expressionAsString)
    {
        this.newColumn(columnName, type, expressionAsString);
        return this;
    }

    private DataFrame addColumnWithTypeValidation(String columnName, ValueType columnType, String expressionAsString)
    {
        ValueType expressionType = this.inferExpressionType(columnName, expressionAsString);
        if (expressionType != columnType)
        {
            throw exceptionByKey("DF_CALC_COL_TYPE_MISMATCH")
                    .with("columnName", columnName)
                    .with("dataFrameName", this.getName())
                    .with("inferredType", expressionType.toString())
                    .with("expression", expressionAsString)
                    .with("specifiedType", columnType.toString())
                    .get();
        }
        this.newColumn(columnName, columnType, expressionAsString);
        return this;
    }

    /**
     * creates a calculated column with the specified name of the specified type and attaches it to this dataframe.
     *
     * @param columnName         the name of the column to be created
     * @param type               the type of the new column
     * @param expressionAsString the expression used to calculate column values
     * @return the newly created columns
     */
    public DfColumnComputed newColumn(String columnName, ValueType type, String expressionAsString)
    {
        DfColumnComputed created = this.createComputedColumn(columnName, type, expressionAsString);
        this.attachColumn(created);
        return created;
    }

    private ValueType inferExpressionType(String columnName, String expressionAsString)
    {
        TypeInferenceVisitor visitor = new TypeInferenceVisitor(this.getEvalContext());

        this.getColumns().each(col -> visitor.storeVariableType(col.getName(), col.getType()));

        Expression expression = ExpressionParserHelper.DEFAULT.toExpressionOrScript(expressionAsString);

        ValueType expressionType = visitor.inferExpressionType(expression);
        if (visitor.hasErrors())
        {
            exceptionByKey("DF_CALC_COL_INFER_TYPE")
                    .with("columnName", columnName)
                    .with("dataFrameName", this.getName())
                    .with("expression", expressionAsString)
                    .with("errorList", visitor.getErrors()
                                              .collect(err -> err.getOne() + ": " + err.getTwo())
                                              .makeString("\n"))
                    .fire();
        }

        return expressionType;
    }

    private DfColumnComputed createComputedColumn(String columnName, ValueType type, String expressionAsString)
    {
        return switch (type)
        {
            case LONG -> new DfLongColumnComputed(this, columnName, expressionAsString);
            case DOUBLE -> new DfDoubleColumnComputed(this, columnName, expressionAsString);
            case STRING -> new DfStringColumnComputed(this, columnName, expressionAsString);
            case DATE -> new DfDateColumnComputed(this, columnName, expressionAsString);
            case DATE_TIME -> new DfDateTimeColumnComputed(this, columnName, expressionAsString);
            case DECIMAL -> new DfDecimalColumnComputed(this, columnName, expressionAsString);
            case INT -> new DfIntColumnComputed(this, columnName, expressionAsString);
            case FLOAT -> new DfFloatColumnComputed(this, columnName, expressionAsString);
            case BOOLEAN -> new DfBooleanColumnComputed(this, columnName, expressionAsString);
            default -> throw exceptionByKey("DF_ADD_COL_UNKNOWN_TYPE").with("columnName", columnName)
                                                                      .with("type", type)
                                                                      .get();
        };
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

    public long getInt(String columnName, int rowIndex)
    {
        return this.getIntColumn(columnName).getInt(this.rowIndexMap(rowIndex));
    }

    public boolean getBoolean(String columnName, int rowIndex)
    {
        return this.getBooleanColumn(columnName).getBoolean(this.rowIndexMap(rowIndex));
    }

    public String getString(String columnName, int rowIndex)
    {
        return this.getStringColumn(columnName).getTypedObject(this.rowIndexMap(rowIndex));
    }

    public double getDouble(String columnName, int rowIndex)
    {
        return this.getDoubleColumn(columnName).getDouble(this.rowIndexMap(rowIndex));
    }

    public float getFloat(String columnName, int rowIndex)
    {
        return this.getFloatColumn(columnName).getFloat(this.rowIndexMap(rowIndex));
    }

    public LocalDate getDate(String columnName, int rowIndex)
    {
        return this.getDateColumn(columnName).getTypedObject(this.rowIndexMap(rowIndex));
    }

    public LocalDateTime getDateTime(String columnName, int rowIndex)
    {
        return this.getDateTimeColumn(columnName).getTypedObject(this.rowIndexMap(rowIndex));
    }

    public BigDecimal getDecimal(String columnName, int rowIndex)
    {
        return this.getDecimalColumn(columnName).getTypedObject(this.rowIndexMap(rowIndex));
    }

    public DfLongColumn getLongColumn(String columnName)
    {
        return (DfLongColumn) this.getColumnNamed(columnName);
    }

    public DfIntColumn getIntColumn(String columnName)
    {
        return (DfIntColumn) this.getColumnNamed(columnName);
    }

    public DfBooleanColumn getBooleanColumn(String columnName)
    {
        return (DfBooleanColumn) this.getColumnNamed(columnName);
    }

    public DfDoubleColumn getDoubleColumn(String columnName)
    {
        return (DfDoubleColumn) this.getColumnNamed(columnName);
    }

    public DfFloatColumn getFloatColumn(String columnName)
    {
        return (DfFloatColumn) this.getColumnNamed(columnName);
    }

    public DfDateColumn getDateColumn(String columnName)
    {
        return (DfDateColumn) this.getColumnNamed(columnName);
    }

    public DfDateTimeColumn getDateTimeColumn(String columnName)
    {
        return (DfDateTimeColumn) this.getColumnNamed(columnName);
    }

    public DfDecimalColumn getDecimalColumn(String columnName)
    {
        return (DfDecimalColumn) this.getColumnNamed(columnName);
    }

    public DfStringColumn getStringColumn(String columnName)
    {
        return (DfStringColumn) this.getColumnNamed(columnName);
    }

    public boolean hasColumn(String columnName)
    {
        return this.columnsByName.containsKey(columnName);
    }

    private DataFrameEvalContext getEvalContext()
    {
        return this.localEvalContext.get();
    }

    public void setEvalContextRowIndex(int rowIndex)
    {
        this.getEvalContext().setRowIndex(rowIndex);
    }

    public ExpressionEvaluationVisitor getEvalVisitor()
    {
        return this.localEvalVisitor.get();
    }

    public void setExternalEvalContext(EvalContext newEvalContext)
    {
        this.getEvalContext().setNestedContext(newEvalContext);
    }

    /**
     * Indicates that no further updates will be made to this data frame and ensures that the data frame is in a
     * consistent internal state. This method should be invoked when done populating a data frame with data. Failure to
     * do so may result in degraded performance or delayed problem detection. It is usually OK to skip it in the context
     * of unit tests.
     *
     * @return the data frame
     */
    public DataFrame seal()
    {
        this.determineRowCount();
        this.resetBitmap();
        this.disablePooling();
        return this;
    }

    /**
     * Turns off pooling of object values and drops object pools.
     * See also {@link #enablePooling()}
     */
    public void disablePooling()
    {
        this.poolingEnabled = false;
        this.columns.forEach(DfColumn::disablePooling);
    }

    /**
     * Turns on pooling of object values. This can result in memory savings when data frame is populated with data.
     * If pooling is enabled on a data frame that already has data, the existing data will be pooled.
     * Note, if pooling is enabled, data frames produced from this one via filtering or union operations will also have
     * pooling enabled. Retaining pooling  may be useful if rows will be added to the resulting data frame, otherwise
     * call  on this data frame before performing these operations, alternatively call {@link #disablePooling()} on the
     * derived data frame
     */
    public void enablePooling()
    {
        this.poolingEnabled = true;
        this.columns.forEach(DfColumn::enablePooling);
    }

    public boolean isPoolingEnabled()
    {
        return this.poolingEnabled;
    }

    private void determineRowCount()
    {
        MutableIntList storedColumnsSizes = this.columns.select(DfColumn::isStored).collectInt(DfColumn::getSize);
        if (storedColumnsSizes.isEmpty())
        {
            this.rowCount = 0;
        }
        else
        {
            this.rowCount = storedColumnsSizes.getFirst();
            if (storedColumnsSizes.anySatisfy(e -> e != this.rowCount))
            {
                exceptionByKey("DF_DIFFERENT_COL_SIZES").with("dataFrameName", this.getName()).fire();
            }
        }
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
     * Pivot the data frame. This operation produces another data frame, with the columns that correspond to the values
     * of the key column, populated with the values from the values columns. THe values are aggregated by one or more
     * aggregation function.
     * <br>
     * NOTE: If more than one aggregator is provided, the column names for the aggregate values will be made up of
     * pairs of all the values of the pivot column and the column names specified in aggregators.
     * So if a pivot values are for example "2001" and "2002" and the only aggregator provided is {@code sum("X")} the
     * columns with aggregated values will have names "2001" and "2002". If there are two aggregator functions,
     * sum("X") and avg("Y"), there will be four columns for aggregated values in the resulting table with the names
     * "2001:X", "2001:Y", "2002:X", "2002:Y". It will also respect the column name overrides in the aggregator
     * function, that is in the example above we have sum("X", "Foo") and avg("Y", "Bar") instead, the resulting column
     * names will be "2001:Foo", "2001:Bar", "2002:Foo", "2002:Bar".
     *
     * @param columnsToGroupByNames the columns to group by the resulting pivot table
     * @param pivotColumnName       the column the values of which will become columns for the pivoted data frame.
     * @param aggregators           the aggregate functions to aggregate values in the value columns specified in
     *                              their parameters
     * @return a new data frame representing a pivot table view of this data frame.
     */
    public DataFrame pivot(
            ListIterable<String> columnsToGroupByNames,
            String pivotColumnName,
            ListIterable<AggregateFunction> aggregators
    )
    {
        return this.pivot(columnsToGroupByNames, pivotColumnName, null, aggregators);
    }

    /**
     * Pivot the data frame. This operation produces another data frame, with the columns that correspond to the values
     * of the key column, populated with the values from the values columns. THe values are aggregated by one or more
     * aggregation function.
     * <br>
     * NOTE: If more than one aggregator is provided, the column names for the aggregate values will be made up of
     * pairs of all the values of the pivot column and the column names specified in aggregators.
     * So if a pivot values are for example "2001" and "2002" and the only aggregator provided is {@code sum("X")} the
     * columns with aggregated values will have names "2001" and "2002". If there are two aggregator functions,
     * sum("X") and avg("Y"), there will be four columns for aggregated values in the resulting table with the names
     * "2001:X", "2001:Y", "2002:X", "2002:Y". It will also respect the column name overrides in the aggregator
     * function, that is in the example above we have sum("X", "Foo") and avg("Y", "Bar") instead, the resulting column
     * names will be "2001:Foo", "2001:Bar", "2002:Foo", "2002:Bar".
     *
     * @param columnsToGroupByNames the columns to group by the resulting pivot table
     * @param pivotColumnName       the column the values of which will become columns for the pivoted data frame.
     * @param pivotColumnOrder      the order in which the pivot columns will appear in the returned data frame (based
     *                              on the ordering of the values of column headers)
     * @param aggregators           the aggregate functions to aggregate values in the value columns specified in
     *                              their parameters
     * @return a new data frame representing a pivot table view of this data frame.
     */
    public DataFrame pivot(
            ListIterable<String> columnsToGroupByNames,
            String pivotColumnName,
            DfColumnSortOrder pivotColumnOrder,
            ListIterable<AggregateFunction> aggregators
    )
    {
        DataFrame pivoted = new DataFrame(this.getName() + "-pivoted");

        // index columns first
        ListIterable<DfColumn> columnsToGroupBy = this.columnsNamed(columnsToGroupByNames);

        columnsToGroupBy.forEach(col -> pivoted.addColumn(col.getName(), col.getType()));

        // then columns derived from pivot dimension values
        // first, find distinct pivot dimension values
        DfColumn columnToPivot = this.getColumnNamed(pivotColumnName);

        Set<Comparable<Object>> pivotColumnValues;

        if (pivotColumnOrder == null)
        {
            pivotColumnValues = new LinkedHashSet<>(); // to maintain insertion order
        }
        else
        {
            pivotColumnValues = new TreeSortedSet<Comparable<Object>>(
                    (pivotColumnOrder == ASC) ? Comparator.naturalOrder() : Comparator.reverseOrder()
            );
        }

        for (int i = 0; i < columnToPivot.getSize(); i++)
        {
            pivotColumnValues.add((Comparable<Object>) columnToPivot.getObject(i));
        }

        ListIterable<String> pivotColumnValuesToStrings =
                Lists.mutable.fromStream(pivotColumnValues.stream()).collect(String::valueOf);

        MutableList<String> pivotColumnNames = Lists.mutable.of();
        MutableListMultimap<String, AggregateFunction> aggregatorsByPivotValue = Multimaps.mutable.list.of();

        // aggregators passed into the method multiplied for each pivot value
        MutableList<AggregateFunction> aggregatorsForPivot = Lists.mutable.of();

        boolean singleAggregator = aggregators.size() == 1;

        // add aggregation columns for each pivot value for each aggregation function
        pivotColumnValuesToStrings.forEach(
            pivotColumnValue -> {
                aggregators.forEach(
                    aggregator -> {
                        DfColumn valueColum = this.getColumnNamed(aggregator.getSourceColumnName());
                        ValueType targetType = aggregator.targetColumnType(valueColum.getType());
                        String targetColumnName = singleAggregator ? pivotColumnValue : pivotColumnValue + ":" + aggregator.getTargetColumnName();

                        pivoted.addColumn(targetColumnName, targetType);

                        AggregateFunction aggregatorForPivotValue = aggregator.cloneWith(aggregator.getSourceColumnName(), targetColumnName);

                        pivotColumnNames.add(targetColumnName);
                        aggregatorsForPivot.add(aggregatorForPivotValue);
                        aggregatorsByPivotValue.put(pivotColumnValue, aggregatorForPivotValue);
                    }
                );
            }
        );

        ListIterable<DfColumn> pivotColumns = pivoted.columnsNamed(Lists.immutable.withAll(pivotColumnNames));

        DfIndexKeeper index = new DfIndexKeeper(pivoted, columnsToGroupByNames, this);

        MapIterable<String, int[]> inputRowCountPerAggregateRow = pivotColumnNames.toMap(colName -> colName, colName -> new int[this.rowCount]);
        // distinct row counts for each pivot column
        // sizing for the worst case scenario (if there is no aggregation)

        for (int rowIndex = 0; rowIndex < this.rowCount; rowIndex++)
        {
            final int finalRowIndex = rowIndex;

            String pivotValue = columnToPivot.getValueAsString(rowIndex);

            ListIterable<Object> keyValueForGroupBy = index.computeKeyFrom(rowIndex);

            int accumulatorRowIndex = index.getRowIndexAtKeyIfAbsentAddAndEvaluate(
                    keyValueForGroupBy,
                    newRowIndex -> pivotColumns.forEachWithIndex(
                            (accumulatorColumn, i) -> aggregatorsForPivot.get(i).initializeValue(accumulatorColumn, newRowIndex)
                    )
            );

            aggregatorsByPivotValue
                    .get(pivotValue)
                    .forEach(agg -> {
                            DfColumn valueColumn = this.getColumnNamed(agg.getSourceColumnName());
                            inputRowCountPerAggregateRow.get(agg.getTargetColumnName())[accumulatorRowIndex]++;
                            agg.getTargetColumn(pivoted)
                                   .applyAggregator(accumulatorRowIndex, valueColumn, finalRowIndex, agg);
                        });
        }

        aggregatorsForPivot.forEach(agg -> agg.finishAggregating(pivoted, inputRowCountPerAggregateRow.get(agg.getTargetColumnName())));

        return pivoted;
    }

    /**
     * Aggregate the values in the specified columns
     *
     * @param aggregators - the aggregate functions to be applied to columns to aggregate
     * @return a single row data frame containing the aggregated values in the respective columns
     */
    public DataFrame aggregate(ListIterable<AggregateFunction> aggregators)
    {
        ListIterable<DfColumn> columnsToAggregate = this.getColumnsToAggregate(aggregators.collect(AggregateFunction::getSourceColumnName));

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
     * @param aggregators           the aggregate functions to be applied to columns to aggregate
     * @param columnsToGroupByNames the columns to group by
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
     * @param columnsToSumNames     the columns to aggregate
     * @param columnsToGroupByNames the columns to group by
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
        MutableList<MutableIntList> sourceRowIds = null;
        if (createSourceRowIdIndex)
        {
            sourceRowIds = Lists.mutable.of();
        }

        int[] inputRowCountPerAggregateRow = new int[this.rowCount()]; // sizing for the worst case scenario: no aggregation

        ListIterable<String> columnsToAggregateNames = aggregators.collect(AggregateFunction::getSourceColumnName);
        ListIterable<DfColumn> columnsToAggregate = this.getColumnsToAggregate(columnsToAggregateNames);

        DataFrame aggregatedDataFrame = new DataFrame("Aggregate Of " + this.getName());

        columnsToGroupByNames
                .asLazy()
                .collect(this::getColumnNamed)
                .forEach(col -> aggregatedDataFrame.addColumn(col.getName(), col.getType()));

        columnsToAggregate.forEachInBoth(aggregators,
                (col, agg) -> aggregatedDataFrame.addColumn(agg.getTargetColumnName(), agg.targetColumnType(col.getType()))
        );

        ListIterable<DfColumn> accumulatorColumns = aggregators
                .collectWith(AggregateFunction::getTargetColumn, aggregatedDataFrame);

        DfIndexKeeper index = new DfIndexKeeper(aggregatedDataFrame, columnsToGroupByNames, this);

        for (int rowIndex = 0; rowIndex < this.rowCount; rowIndex++)
        {
            ListIterable<Object> keyValue = index.computeKeyFrom(rowIndex);

            int accumulatorRowIndex = index.getRowIndexAtKeyIfAbsentAddAndEvaluate(
                    keyValue,
                    newRowIndex -> aggregators.forEachInBoth(accumulatorColumns,
                            (aggregateFunction, accumulatorColumn) -> aggregateFunction.initializeValue(accumulatorColumn, newRowIndex))
            );

            if (createSourceRowIdIndex)
            {
                while (sourceRowIds.size() <= accumulatorRowIndex)
                {
                    sourceRowIds.add(IntLists.mutable.of());
                }

                sourceRowIds.get(accumulatorRowIndex).add(rowIndex);
            }

            inputRowCountPerAggregateRow[accumulatorRowIndex]++;

            for (int colIndex = 0; colIndex < columnsToAggregate.size(); colIndex++)
            {
                accumulatorColumns.get(colIndex).applyAggregator(accumulatorRowIndex, columnsToAggregate.get(colIndex), rowIndex, aggregators.get(colIndex));
            }
        }

        if (createSourceRowIdIndex)
        {
            aggregatedDataFrame.aggregateIndex = sourceRowIds;
        }

        aggregators.forEach(agg -> agg.finishAggregating(aggregatedDataFrame, inputRowCountPerAggregateRow));

        return aggregatedDataFrame;
    }

    public DataFrame sumByWithIndex(ListIterable<String> columnsToSumNames, ListIterable<String> columnsToGroupByNames)
    {
        return this.aggregateByWithIndex(columnsToSumNames.collect(AggregateFunction::sum), columnsToGroupByNames);
    }

    /**
     * Extracts distinct rows from the data frame. Returns a new data frame with the same schema as this data frame and
     * only containing distinct row values. This is the same as calling the {@link #distinct(ListIterable)} method with
     * the list of all the data frame columns as its parameter.
     *
     * @return a new data frame with a row for each distinct (unique) combination of row values for the specified
     * columns in this dataframe
     */
    public DataFrame distinct()
    {
        return this.uniqueRowsForColumns(this.columns);
    }

    /**
     * Extracts distinct valued in rows, in multiple columns, for the specified columns. Returns a new data frame
     * containing the specified columns with distinct row values from this dataframe.
     *
     * @param columnNames the list of columns from which to select unique row values
     * @return a new data frame with a row for each distinct (unique) combination of row values for the specified
     * columns in this dataframe
     */
    public DataFrame distinct(ListIterable<String> columnNames)
    {
        return this.uniqueRowsForColumns(columnNames.collect(this::getColumnNamed));
    }

    private DataFrame uniqueRowsForColumns(ListIterable<DfColumn> uniqueColumns)
    {
        ListIterable<String> columnNames = uniqueColumns.collect(DfColumn::getName);

        DataFrame result = new DataFrame("Distinct " + this.getName());

        uniqueColumns.forEach(col -> result.addColumn(col.getName(), col.getType()));

        DfIndexKeeper index = new DfIndexKeeper(result, columnNames, this);

        for (int rowIndex = 0; rowIndex < this.rowCount; rowIndex++)
        {
            ListIterable<Object> keyValue = index.computeKeyFrom(rowIndex);
            index.getRowIndexAtKeyIfAbsentAdd(keyValue);
        }

        return result;
    }

    /**
     * Returns two data frames, splitting the rows of this data frame based on the evaluation of a criteria expression
     * passes as the parameter into this method. The two data frames will contain, respectively: the rows for which the
     * expression evaluates to {@code true} (the selected rows) and the rows for which the expression evaluates to
     * {@code false} (the rejected rows).
     *
     * @param filterExpressionString the expression based on which the rows in this data frame will be partitioned
     * @return a {@link org.eclipse.collections.api.tuple.Twin} containing a data frame with the selected rows (matching the filter criteria) as its
     * first element and a data frame containing the rejected rows (failing the filter criteria) as its second element
     * @see DataFrame#selectBy(String)
     * @see DataFrame#rejectBy(String)
     */
    public Twin<DataFrame> partition(String filterExpressionString)
    {
        DataFrame selected = this.cloneStructure(this.name + "-selected");
        this.inheritPoolingSettings(selected);

        DataFrame rejected = this.cloneStructure(this.name + "-rejected");
        this.inheritPoolingSettings(rejected);

        Expression filterExpression = ExpressionParserHelper.DEFAULT.toExpression(filterExpressionString);

        for (int i = 0; i < this.rowCount; i++)
        {
            this.getEvalContext().setRowIndex(i);
            Value select = filterExpression.evaluate(this.getEvalVisitor());
            if (((BooleanValue) select).isTrue())
            {
                selected.copyRowFrom(this, i);
            }
            else
            {
                rejected.copyRowFrom(this, i);
            }
        }

        this.sealMindingPooling(selected);
        this.sealMindingPooling(rejected);

        return Tuples.twin(selected, rejected);
    }

    /**
     * Creates a new data frame from this one by copying the rows of this data frame for which the specified filter
     * expression returns {@code true}.
     *
     * @param filterExpressionString the expression based on which rows will be selected into the result data frame
     * @return a new data frame, which is a copy of this data frame with only the rows that satisfy the filter criteria
     * @see DataFrame#rejectBy(String)
     * @see DataFrame#partition(String)
     */
    public DataFrame selectBy(String filterExpressionString)
    {
        return this.filterBy(filterExpressionString, "selected", true);
    }

    /**
     * Creates a new data frame from this one by copying the rows of this data frame for which the specified filter
     * expression returns {@code false}. In other words the result is a copy of this data frame, *except* the rows for
     * which the expression is {@code true}.
     *
     * @param filterExpressionString the expression based on which rows will be excluded from the result data frame
     * @return a new data frame, which is a copy of this data frame excluding the rows satisfying the filter criteria
     * @see DataFrame#selectBy(String)
     * @see DataFrame#partition(String)
     */
    public DataFrame rejectBy(String filterExpressionString)
    {
        return this.filterBy(filterExpressionString, "rejected", false);
    }

    private void inheritPoolingSettings(DataFrame dataFrame)
    {
        if (this.isPoolingEnabled())
        {
            dataFrame.enablePooling();
        }
    }

    private void sealMindingPooling(DataFrame dataFrame)
    {
        if (this.isPoolingEnabled())
        {
            dataFrame.determineRowCount();
            dataFrame.resetBitmap();
        }
        else
        {
            dataFrame.seal();
        }
    }

    private DataFrame filterBy(String filterExpressionString, String operation, boolean select)
    {
        DataFrame filtered = this.cloneStructure(this.getName() + "-" + operation);
        this.inheritPoolingSettings(filtered);

        Expression filterExpression = ExpressionParserHelper.DEFAULT.toExpression(filterExpressionString);
        for (int i = 0; i < this.rowCount; i++)
        {
            this.getEvalContext()
                .setRowIndex(i);
            Value filterValue = filterExpression.evaluate(this.getEvalVisitor());
            if (((BooleanValue) filterValue).is(select))
            {
                filtered.copyRowFrom(this, i);
            }
        }

        this.sealMindingPooling(filtered);
        return filtered;
    }

    private DataFrame selectByMarkValue(IntPredicate flaggedAtIndex, String description)
    {
        DataFrame filtered = this.cloneStructure(this.getName() + "-" + description);
        this.inheritPoolingSettings(filtered);

        for (int i = 0; i < this.rowCount; i++)
        {
            if (flaggedAtIndex.accept(i))
            {
                filtered.copyRowFrom(this, this.rowIndexMap(i));
            }
        }

        this.sealMindingPooling(filtered);

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

    /**
     * creates a copy of the whole data frame with the same schema as the original, including computed columns that
     * are converted to stored columns of the same type
     *
     * @param newName the name for the new data frame
     * @return a copy of the original data frame with the provided name and new schema
     */
    public DataFrame copy(String newName)
    {
        return this.copy(newName, null);
    }

    /**
     * creates a copy of the parts of data frame with the same schema as the original, including computed columns that
     * are converted to stored columns of the same type. Only the provided columns are copied to the new data frame.
     * If the list is set to null, all columns are copied (behaving in the same way as copy(String newName)).
     *
     * @param newName           the name for the new data frame
     * @param columnNamesToCopy the names of the columns to be copied
     * @return a copy of the original data frame with the provided name and new schema
     */
    public DataFrame copy(String newName, ListIterable<String> columnNamesToCopy)
    {
        DataFrame copied = new DataFrame(newName);

        ((columnNamesToCopy == null)
                ? this.columns
                : columnNamesToCopy.collect(this::getColumnNamed)
        ).forEach(col -> col.copyTo(copied));

        copied.seal();
        return copied;
    }

    public DataFrame sortBy(ListIterable<String> columnsToSortByNames)
    {
        return this.sortBy(columnsToSortByNames, null);
    }

    public DataFrame sortBy(ListIterable<String> columnsToSortByNames, ListIterable<DfColumnSortOrder> sortOrders)
    {
        this.unsort();

        this.virtualRowMap = this.buildSortedRowIndexMap(columnsToSortByNames, sortOrders);

        return this;
    }

    private IntList buildSortedRowIndexMap(ListIterable<String> columnsToSortByNames, ListIterable<DfColumnSortOrder> sortOrders)
    {
        // doing this before check for rowCount to make sure that the sort columns exist even if the data frame is empty
        ListIterable<DfColumn> columnsToSortBy = this.columnsNamed(columnsToSortByNames);

        if (this.rowCount == 0)
        {
            return IntLists.immutable.empty();
        }

        DfTuple[] tuples = new DfTuple[this.rowCount];
        for (int i = 0; i < this.rowCount; i++)
        {
            tuples[i] = this.rowToTuple(i, columnsToSortBy);
        }

        if (sortOrders == null)
        {
            Arrays.sort(tuples);
        }
        else
        {
            Arrays.sort(tuples, (t1, t2) -> t1.compareTo(t2, sortOrders));
        }

        return ArrayIterate.collectInt(tuples, DfTuple::order);
    }

    public DataFrame sortByExpression(String expressionString)
    {
        return this.sortByExpression(expressionString, ASC);
    }

    public DataFrame sortByExpression(String expressionString, DfColumnSortOrder sortOrder)
    {
        this.unsort();
        if (this.rowCount == 0)
        {
            this.virtualRowMap = IntLists.immutable.empty();
            return this;
        }

        Expression expression = ExpressionParserHelper.DEFAULT.toExpression(expressionString);

        DfTuple[] tuples = new DfTuple[this.rowCount];
        for (int i = 0; i < this.rowCount; i++)
        {
            this.getEvalContext().setRowIndex(i);
            tuples[i] = new DfTuple(i, expression.evaluate(this.getEvalVisitor()));
        }

        Arrays.sort(tuples, (t1, t2) -> t1.compareTo(t2, Lists.immutable.of(sortOrder)));

        this.virtualRowMap = ArrayIterate.collectInt(tuples, DfTuple::order);

        return this;
    }

    private DfTuple rowToTuple(int rowIndex, ListIterable<DfColumn> columnsToCollect)
    {
        int size = columnsToCollect.size();
        Object[] values = new Object[size];
        for (int i = 0; i < size; i++)
        {
            values[i] = columnsToCollect.get(i).getObject(rowIndex);
        }

        return new DfTuple(rowIndex, values);
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
        if (this.columnCount() != other.columnCount())
        {
            exceptionByKey("DF_UNION_DIFF_COL_COUNT").fire();
        }

        DataFrame dfUnion = new DataFrame("union");
        this.inheritPoolingSettings(dfUnion);

        this.columns.forEach(
                col -> col.mergeWithInto(other.getColumnNamed(col.getName()), dfUnion)
        );

        this.sealMindingPooling(dfUnion);
        return dfUnion;
    }

    /**
     * Enables flagging the rows as true or false - effectively creating a bitmap of the data frame rows.
     * This method initializes the bitmap so that no flags are set.
     */
    public void resetBitmap()
    {
        this.bitmap = BooleanArrayList.newWithNValues(this.rowCount, false);
    }

    /**
     * Flags a row in the data frame. The data frame can be filtered based on the flagged rows.
     * @param rowIndex the index of the row to flag
     */
    public void setFlag(int rowIndex)
    {
        this.ensureBitmapCapacity();
        this.bitmap.set(rowIndex, true);
    }

    private void ensureBitmapCapacity()
    {
        if (this.bitmap.size() < this.rowCount)
        {
            for (int i = this.bitmap.size(); i <= this.rowCount; i++)
            {
                this.bitmap.add(false);
            }
        }
    }

    /**
     * checks if a row is flagged
     * @param rowIndex the index of the row to check
     * @return {@code true} if the row a {@code rowIndex} is flagged, {@code false}
     */
    public boolean isFlagged(int rowIndex)
    {
        this.ensureBitmapCapacity();
        return this.bitmap.get(rowIndex);
    }

    public boolean notFlagged(int rowIndex)
    {
        return !this.bitmap.get(rowIndex);
    }

    /**
     * creates a new data frame, which contain a subset of rows of this data frame for the rows with the bitmap flags
     * set (i.e. equals to true). This is the behavior the opposite of {@code selectNotMarked}
     *
     * @return a data frame containing the filtered subset of rows
     */
    public DataFrame selectFlagged()
    {
        return this.selectByMarkValue(this::isFlagged, "flagged");
    }

    /**
     * creates a new data frame, which contain a subset of rows of this data frame for the rows with the bitmap flag not
     * set (i.e. equals to false). This is the behavior the opposite of {@code selectMarked}
     *
     * @return a data frame containing the filtered subset of rows
     */
    public DataFrame selectNotFlagged()
    {
        return this.selectByMarkValue(this::notFlagged, "not flagged");
    }

    /**
     * Tag rows based on whether the provided expression returns true of false
     *
     * @param filterExpressionString the expression to set the flags by
     */
    public void flagRowsBy(String filterExpressionString)
    {
        this.resetBitmap();

        Expression filterExpression = ExpressionParserHelper.DEFAULT.toExpression(filterExpressionString);

        for (int i = 0; i < this.rowCount; i++)
        {
            this.getEvalContext().setRowIndex(i);
            BooleanValue evalResult = (BooleanValue) filterExpression.evaluate(this.getEvalVisitor());
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
                   )
                   .getTwo();
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
                   )
                   .getTwo();
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
                   )
                   .getTwo();
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
                   )
                   .getTwo();
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

    private Triplet<DataFrame> join(
            DataFrame other,
            JoinType joinType,
            ListIterable<String> theseJoinColumnNames,
            ListIterable<String> thisAdditionalSortColumnNames,
            ListIterable<String> otherJoinColumnNames,
            ListIterable<String> otherAdditionalSortColumnNames,
            MutableMap<String, String> renamedOtherColumns
    )
    {
        if (theseJoinColumnNames.size() != otherJoinColumnNames.size())
        {
            exceptionByKey("DF_JOIN_DIFF_KEY_COUNT")
                    .with("side1KeyList", theseJoinColumnNames.makeString())
                    .with("side2KeyList", otherJoinColumnNames.makeString())
                    .fire();
        }

        DataFrame joined = this.cloneStructureAsStored(this.getName() + "_" + other.getName());

        DataFrame thisComplementOther = this.cloneStructureAsStored(this.getName() + "-" + other.getName());
        DataFrame otherComplementThis = other.cloneStructureAsStored(other.getName() + "-" + this.getName());

        MapIterable<String, String> otherColumnNameMap = this.resolveDuplicateNames(
                this.columns.collect(DfColumn::getName),
                other.columns.collect(DfColumn::getName));

        otherColumnNameMap.forEachKeyValue(renamedOtherColumns::put);
        otherJoinColumnNames.forEachInBoth(theseJoinColumnNames, renamedOtherColumns::put);

        other.columns
                .reject(col -> otherJoinColumnNames.contains(col.getName()))
                .forEach(col -> joined.addColumn(otherColumnNameMap.get(col.getName()), col.getType()));

        // building separate indexes for sorting rather than sorting the data frames directly to preserve their existing
        // sort order (if any)
        IntList thisSortedIndexMap = this.buildSortedRowIndexMap(theseJoinColumnNames.toList().withAll(thisAdditionalSortColumnNames), null);
        IntList otherSortedIndexMap = other.buildSortedRowIndexMap(otherJoinColumnNames.toList().withAll(otherAdditionalSortColumnNames), null);

        int thisRowIndex = 0;
        int otherRowIndex = 0;

        int thisRowCount = this.rowCount();
        int otherRowCount = other.rowCount();

        Object[] rowData = new Object[joined.columnCount()];

        ListIterable<String> theseColumnNames = this.columns.collect(DfColumn::getName);
        int theseColumnCount = theseColumnNames.size();

        IntList joinColumnIndices = theseJoinColumnNames.collectInt(theseColumnNames::indexOf);

        ListIterable<String> otherColumnNames = other.columns
                .collect(DfColumn::getName)
                .reject(otherJoinColumnNames::contains);

        ListIterable<DfColumn> theseColumns = this.columnsNamed(theseColumnNames);
        ListIterable<DfColumn> otherColumns = other.columnsNamed(otherColumnNames);

        ListIterable<DfColumn> theseJoinColumns = this.columnsNamed(theseJoinColumnNames);
        ListIterable<DfColumn> otherJoinColumns = other.columnsNamed(otherJoinColumnNames);

        IntIntToIntFunction keyComparator = (thisIndex, otherIndex) -> {
            for (int i = 0; i < theseJoinColumns.size(); i++)
            {
                int result = DfTuple.compareMindingNulls(
                        theseJoinColumns.get(i).getObject(thisSortedIndexMap.get(thisIndex)),
                        otherJoinColumns.get(i).getObject(otherSortedIndexMap.get(otherIndex))
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

            final int thisSortedIndex = thisSortedIndexMap.get(thisRowIndex);
            final int otherSortedIndex = otherSortedIndexMap.get(otherRowIndex);

            if (comparison == 0)
            {
                theseColumns.forEachWithIndex((col, i) -> rowData[i] = col.getObject(thisSortedIndex));
                otherColumns.forEachWithIndex((col, i) -> rowData[theseColumnCount + i] = col.getObject(otherSortedIndex));

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
                        theseColumns.forEachWithIndex((col, i) -> rowData[i] = col.getObject(thisSortedIndex));

                        joined.addRow(rowData);
                    }
                    else if (joinType.isJoinWithComplements())
                    {
                        thisComplementOther.copyRowFrom(this, thisSortedIndex);
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
                            (joinColumnIndex, sourceKeyIndex) ->
                                rowData[joinColumnIndex] = otherJoinColumns.get(sourceKeyIndex)
                                                                           .getObject(otherSortedIndex)
                        );

                        otherColumns.forEachWithIndex(
                                (col, i) -> rowData[theseColumnCount + i] = col.getObject(otherSortedIndex));

                        joined.addRow(rowData);
                    }
                    else if (joinType.isJoinWithComplements())
                    {
                        otherComplementThis.copyRowFrom(other, otherSortedIndex);
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
                final int thisSortedIndex = thisSortedIndexMap.get(thisRowIndex);

                Arrays.fill(rowData, null);
                theseColumns.forEachWithIndex((col, i) -> rowData[i] = col.getObject(thisSortedIndex));

                joined.addRow(rowData);

                thisRowIndex++;
            }

            while (otherRowIndex < otherRowCount)
            {
                final int otherSortedIndex = otherSortedIndexMap.get(otherRowIndex);

                Arrays.fill(rowData, null);

                joinColumnIndices.forEachWithIndex(
                        (joinColumnIndex, sourceKeyIndex)
                                -> rowData[joinColumnIndex] = otherJoinColumns.get(sourceKeyIndex)
                                                                              .getObject(otherSortedIndex)
                );

                otherColumns.forEachWithIndex(
                        (col, i) -> rowData[theseColumnCount + i] = col.getObject(otherSortedIndex)
                );

                joined.addRow(rowData);

                otherRowIndex++;
            }
        }
        else if (joinType.isJoinWithComplements())
        {
            while (thisRowIndex < thisRowCount)
            {
                thisComplementOther.copyRowFrom(this, thisSortedIndexMap.get(thisRowIndex));
                thisRowIndex++;
            }

            while (otherRowIndex < otherRowCount)
            {
                otherComplementThis.copyRowFrom(other, otherSortedIndexMap.get(otherRowIndex));
                otherRowIndex++;
            }
        }

        joined.seal();
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

        otherNames.forEach(e -> {
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
        columnNamesToKeep.forEach(this::getColumnNamed); // will throw if a column doesn't exist

        MutableList<String> columnNamesToDrop = this.columns.collect(DfColumn::getName)
                                                            .reject(columnNamesToKeep::contains);

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

        ListIterable<DfColumn> columnsToLookup = joinDescriptor.columnsToLookup()
                                                               .collect(this::getColumnNamed);

        columnsToSelectFrom.forEachInBoth(joinDescriptor.columnNameAliases(),
                (col, alias) -> this.addColumn(alias, col.getType()));

        ListIterable<DfColumn> addedColumns = joinDescriptor.columnNameAliases()
                                                            .collect(this::getColumnNamed);

        for (int rowIndex = 0; rowIndex < this.rowCount(); rowIndex++)
        {
            int finalRowIndex = rowIndex; // to pass as a lambda parameter

            ListIterable<Object> lookupKey = columnsToLookup.collect(col -> col.getObject(finalRowIndex));

            IntList found = index.getRowIndicesAtKey(lookupKey);

            if (found.isEmpty())
            {
                // default if specified, otherwise null
                if (joinDescriptor.valuesIfAbsent()
                                  .notEmpty())
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
                // use the first row in case there are multiple rows matching this lookup key
                int targetRowIndex = found.getFirst();

                columnsToSelectFrom.forEachInBoth(addedColumns, (selectFrom, addTo) -> addTo.addObject(selectFrom.getObject(targetRowIndex)));
            }
        }

        return this.seal();
    }

    public DfJoin lookupIn(DataFrame lookupTarget)
    {
        return DfJoin.from(this).joinTo(lookupTarget);
    }

    public boolean isEmpty()
    {
        return this.rowCount() == 0;
    }

    public boolean isNotEmpty()
    {
        return !this.isEmpty();
    }

    /**
     * creates a named index based on the values of one or more columns. A data frame can have multiple indexes defined
     * for it
     * @param indexName the name of the index to be created
     * @param columnNames the names of the columns to index data frame by
     * @see io.github.vmzakharov.ecdataframe.dataframe.DataFrame#index(String)
     * @see io.github.vmzakharov.ecdataframe.dataframe.DataFrame#dropIndex(String)
     */
    public void createIndex(String indexName, ListIterable<String> columnNames)
    {
        this.indices.put(indexName, new DfIndex(this, columnNames));
    }

    /**
     * drops a named index from the list of indexes for this data frame
     * @param indexName the name of the index to be dropped
     * @see io.github.vmzakharov.ecdataframe.dataframe.DataFrame#createIndex(String, ListIterable)
     * @see io.github.vmzakharov.ecdataframe.dataframe.DataFrame#index(String)
     */
    public void dropIndex(String indexName)
    {
        this.indices.remove(indexName);
    }

    /**
     * returns an existing index with the specified name. The index is an instance of the {@link DfIndex} class that
     * allows indexed access to the data frame including iterating over subsets of rows with the same index values
     * @param indexName the name of the index to retrieve
     * @see io.github.vmzakharov.ecdataframe.dataframe.DataFrame#createIndex(String, ListIterable)
     * @see io.github.vmzakharov.ecdataframe.dataframe.DataFrame#dropIndex(String)
     */
    public DfIndex index(String indexName)
    {
        DfIndex found = this.indices.get(indexName);
        if (found == null)
        {
            exceptionByKey("DF_INDEX_DOES_NOT_EXIST").with("indexName", indexName)
                                                     .with("dataFrameName", this.getName())
                                                     .fire();
        }

        return found;
    }

    @Override
    public void forEach(Procedure<DfCursor> action)
    {
        DfCursor cursor = new DfCursor(this);

        for (int i = 0; i < this.rowCount; i++)
        {
            action.value(cursor.rowIndex(i));
        }
    }

    public DataFrame schema()
    {
        DataFrame columnDescriptors = new DataFrame("Columns of " + this.getName())
                .addStringColumn("Name")
                .addStringColumn("Type")
                .addStringColumn("Stored")
                .addStringColumn("Expression");

        this.getColumns()
            .forEach(
                    col -> columnDescriptors.addRow(
                            col.getName(),
                            col.getType().toString(),
                            col.isStored() ? "Y" : "N",
                            col.isComputed() ? ((DfColumnComputed) col).getExpressionAsString() : ""
                    )
            );

        columnDescriptors.seal();
        return columnDescriptors;
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

    private static class DataFrameEvalContext
    extends EvalContextAbstract
    {
        final private DataFrame dataFrame;
        private EvalContext nestedContext;
        private int rowIndex;

        private final MutableMap<String, Supplier<Value>> resolvedVariables = Maps.mutable.of();

        public DataFrameEvalContext(DataFrame newDataFrame)
        {
            this(newDataFrame, new SimpleEvalContext());
        }

        public DataFrameEvalContext(DataFrame newDataFrame, EvalContext newNestedContext)
        {
            this.dataFrame = newDataFrame;
            this.nestedContext = newNestedContext;
        }

        public int getRowIndex()
        {
            return this.rowIndex;
        }

        public void setRowIndex(int newRowIndex)
        {
            this.rowIndex = newRowIndex;
        }

        @Override
        public Value getVariable(String variableName)
        {
            Supplier<Value> valueGetter = this.resolvedVariables.get(variableName);

            if (valueGetter == null)
            {
                if (this.getDataFrame().hasColumn(variableName))
                {
                    DfColumn column = this.dataFrame.getColumnNamed(variableName);
                    valueGetter = () -> column.getValue(this.getRowIndex());
                }
                else if (this.getContextVariables().containsKey(variableName))
                {
                    valueGetter = () -> this.getContextVariables().get(variableName);
                }
                else
                {
                    valueGetter = () -> this.getNestedContext().getVariable(variableName);
                }

                this.resolvedVariables.put(variableName, valueGetter);
            }

            return valueGetter.get();
        }

        @Override
        public Value getVariableOrDefault(String variableName, Value defaultValue)
        {
            Value value = this.getVariable(variableName);

            return value == Value.VOID ? defaultValue : value;
        }

        @Override
        public boolean hasVariable(String variableName)
        {
            return this.getDataFrame().hasColumn(variableName) || this.getNestedContext().hasVariable(variableName);
        }

        @Override
        public void removeVariable(String variableName)
        {
            exceptionByKey("DSL_ATTEMPT_TO_REMOVE_DF_VAR").fire();
        }

        @Override
        public MapIterable<String, FunctionScript> getDeclaredFunctions()
        {
            return this.getNestedContext().getDeclaredFunctions();
        }

        @Override
        public FunctionScript getDeclaredFunction(String functionName)
        {
            FunctionScript functionScript = super.getDeclaredFunction(functionName);
            if (functionScript == null)
            {
                functionScript = this.getNestedContext().getDeclaredFunction(functionName);
            }

            return functionScript;
        }

        @Override
        public void addDataSet(HierarchicalDataSet dataSet)
        {
            exceptionByKey("DSL_DF_EVAL_NO_DATASET").fire();
        }

        @Override
        public HierarchicalDataSet getDataSet(String dataSetName)
        {
            return this.getNestedContext().getDataSet(dataSetName);
        }

        @Override
        public RichIterable<String> getVariableNames()
        {
            return this.getNestedContext().getVariableNames();
        }

        @Override
        public void removeAllVariables()
        {
            throw exceptionByKey("DSL_ATTEMPT_TO_REMOVE_DF_VAR").getUnsupported();
        }

        public DataFrame getDataFrame()
        {
            return this.dataFrame;
        }

        public EvalContext getNestedContext()
        {
            return this.nestedContext;
        }

        public void setNestedContext(EvalContext newEvalContext)
        {
            this.nestedContext = newEvalContext;
        }
    }
}
