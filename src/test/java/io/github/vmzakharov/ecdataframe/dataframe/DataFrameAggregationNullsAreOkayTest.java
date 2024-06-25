package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.*;

public class DataFrameAggregationNullsAreOkayTest
{
    @Test
    public void countNulls()
    {
          DataFrame dataFrame = new DataFrame("FrameOfData")
            .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar").addDoubleColumn("Baz")
            .addIntColumn("Waldo").addDateColumn("Date")
            .addRow("Alice",    "Blue",  123L, null, null, LocalDate.of(2021, 11, 21))
            .addRow(null,     "Orange",  456L, 12.0,   10, null)
            .addRow("Bob",    "Violet",  457L, 16.0, null, LocalDate.of(2021, 11, 23))
            .addRow("Carol",   "Green",  null, 17.0,   11, null)
            .addRow(null,     "Purple",  null, 17.0, null, LocalDate.of(2021, 12, 22));

        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addLongColumn("Null Color Count").addLongColumn("Null Name Count")
                        .addLongColumn("Bar").addLongColumn("Baz").addLongColumn("Waldo").addLongColumn("Date")
                        .addRow(0, 2, 2, 1, 3, 2),
                dataFrame.aggregate(
                        Lists.immutable.of(
                                new CountNulls("Color", "Null Color Count"),
                                new CountNulls("Name", "Null Name Count"),
                                new CountNulls("Bar"),
                                new CountNulls("Baz"),
                                new CountNulls("Waldo"),
                                new CountNulls("Date")
                        ))
        );
    }

    @Test
    public void countNullsWithGrouping()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
            .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar").addDoubleColumn("Baz")
            .addIntColumn("Waldo").addDateColumn("Date")
            .addRow("Alice",   "Blue",  123L, null, null, LocalDate.of(2021, 11, 21))
            .addRow("Alice", "Orange",  456L, 12.0,   10, null)
            .addRow("Bob",   "Violet",  457L, 16.0, null, LocalDate.of(2021, 11, 23))
            .addRow("Carol",  "Green",  null, 17.0,   11, null)
            .addRow("Carol", "Purple",  null, 17.0,   12, LocalDate.of(2021, 12, 22));

        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addStringColumn("Name").addLongColumn("Null Color Count").addLongColumn("Null Name Count")
                        .addLongColumn("Bar").addLongColumn("Baz").addLongColumn("Waldo").addLongColumn("Date")
                        .addRow("Alice", 0, 0, 0, 1, 1, 1)
                        .addRow("Bob",   0, 0, 0, 0, 1, 0)
                        .addRow("Carol", 0, 0, 2, 0, 0, 1),
                dataFrame.aggregateBy(
                        Lists.immutable.of(
                                new CountNulls("Color", "Null Color Count"),
                                new CountNulls("Name", "Null Name Count"),
                                new CountNulls("Bar"),
                                new CountNulls("Baz"),
                                new CountNulls("Waldo"),
                                new CountNulls("Date")
                        ),
                        Lists.immutable.of("Name")));
    }

    @Test
    public void sumIgnoringNulls()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz")
                .addIntColumn("Waldo").addDoubleColumn("Qux")
                .addRow("Alice", 123L, null, null, null)
                .addRow("Alice", 456L, 12.0,    1, null)
                .addRow("Bob",   457L, null, null, null)
                .addRow("Bob",   111L, null,    2, null)
                .addRow("Carol", null, 17.0, null, null)
                .addRow("Carol", null, 18.0,    3, null);

        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addLongColumn("Bar").addDoubleColumn("Baz")
                        .addLongColumn("Total Waldos").addDoubleColumn("Qux")
                        .addRow(1147L, 47.0, 6, null),
                dataFrame.aggregate(Lists.immutable.of(
                        new SumIgnoreNulls("Bar"),
                        new SumIgnoreNulls("Baz"),
                        new SumIgnoreNulls("Waldo", "Total Waldos"),
                        new SumIgnoreNulls("Qux")
                    ))
        );
    }

    @Test
    public void sumIgnoringNullsWithGrouping()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addIntColumn("Waldo")
                .addRow("Alice", 123L, null,   10)
                .addRow("Alice", 456L, 12.0, null)
                .addRow("Bob",   457L, null, null)
                .addRow("Bob",   111L, null, null)
                .addRow("Carol", null, 17.0,   -5)
                .addRow("Carol", null, 18.0,    6);

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz").addLongColumn("Waldo")
                        .addRow("Alice", 579L, 12.0,   10)
                        .addRow("Bob",   568L, null, null)
                        .addRow("Carol", null, 35.0,    1),
                dataFrame.aggregateBy(
                        Lists.immutable.of(
                                new SumIgnoreNulls("Bar"),
                                new SumIgnoreNulls("Baz"),
                                new SumIgnoreNulls("Waldo")
                        ),
                        Lists.immutable.of("Name")));
    }

    private static class CountNulls
    extends AggregateFunction
    {
        public CountNulls(String newColumnName)
        {
            super(newColumnName);
        }

        public CountNulls(String newColumnName, String newTargetColumnName)
        {
            super(newColumnName, newTargetColumnName);
        }

        @Override
        public ListIterable<ValueType> supportedSourceTypes()
        {
            return Lists.immutable.of(INT, LONG, DOUBLE, STRING, DATE, DATE_TIME);
        }

        @Override
        public long longInitialValue()
        {
            return 0;
        }

        @Override
        public long getLongValue(DfColumn sourceColumn, int sourceRowIndex)
        {
            return sourceColumn.isNull(sourceRowIndex) ? 1 : 0;
        }

        @Override
        protected long longAccumulator(long currentAggregate, long newValue)
        {
            return currentAggregate + newValue;
        }

        @Override
        public Object applyToObjectColumn(DfObjectColumn<?> objectColumn)
        {
            int count = 0;
            for (int rowIndex = 0; rowIndex < objectColumn.getSize(); rowIndex++)
            {
                if (objectColumn.isNull(rowIndex))
                {
                    count++;
                }
            }
            return count;
        }

        @Override
        public Object applyToDoubleColumn(DfDoubleColumn doubleColumn)
        {
            int count = 0;
            for (int rowIndex = 0; rowIndex < doubleColumn.getSize(); rowIndex++)
            {
                if (doubleColumn.isNull(rowIndex))
                {
                    count++;
                }
            }
            return count;
        }

        @Override
        public Object applyToLongColumn(DfLongColumn longColumn)
        {
            int count = 0;
            for (int rowIndex = 0; rowIndex < longColumn.getSize(); rowIndex++)
            {
                if (longColumn.isNull(rowIndex))
                {
                    count++;
                }
            }
            return count;
        }

        @Override
        public Object applyToIntColumn(DfIntColumn intColumn)
        {
            int count = 0;
            for (int rowIndex = 0; rowIndex < intColumn.getSize(); rowIndex++)
            {
                if (intColumn.isNull(rowIndex))
                {
                    count++;
                }
            }
            return count;
        }

        @Override
        public String getDescription()
        {
            return "Count of null values";
        }

        @Override
        public ValueType targetColumnType(ValueType sourceColumnType)
        {
            return LONG;
        }

        @Override
        public boolean nullsArePoisonous()
        {
            return false;
        }
    }

    private static class SumIgnoreNulls
    extends AggregateFunction
    {
        public SumIgnoreNulls(String newColumnName)
        {
            super(newColumnName);
        }

        public SumIgnoreNulls(String newColumnName, String newTargetColumnName)
        {
            super(newColumnName, newTargetColumnName);
        }

        @Override
        public String getDescription()
        {
            return "Sum ignoring null values";
        }

        @Override
        public ListIterable<ValueType> supportedSourceTypes()
        {
            return Lists.immutable.of(INT, LONG, DOUBLE);
        }

        @Override
        public ValueType targetColumnType(ValueType sourceColumnType)
        {
            return sourceColumnType.isInt() ? LONG : sourceColumnType;
        }

        @Override
        public Object applyToDoubleColumn(DfDoubleColumn doubleColumn)
        {
            boolean allNulls = true;
            double sum = 0;
            for (int rowIndex = 0; rowIndex < doubleColumn.getSize(); rowIndex++)
            {
                if (!doubleColumn.isNull(rowIndex))
                {
                    allNulls = false;
                    sum += doubleColumn.getDouble(rowIndex);
                }
            }

            return allNulls ? null : sum;
        }

        @Override
        public Object applyToLongColumn(DfLongColumn longColumn)
        {
            boolean allNulls = true;
            long sum = 0;
            for (int rowIndex = 0; rowIndex < longColumn.getSize(); rowIndex++)
            {
                if (!longColumn.isNull(rowIndex))
                {
                    allNulls = false;
                    sum += longColumn.getLong(rowIndex);
                }
            }

            return allNulls ? null : sum;
        }

        @Override
        public Object applyToIntColumn(DfIntColumn intColumn)
        {
            boolean allNulls = true;
            long sum = 0;
            for (int rowIndex = 0; rowIndex < intColumn.getSize(); rowIndex++)
            {
                if (!intColumn.isNull(rowIndex))
                {
                    allNulls = false;
                    sum += intColumn.getInt(rowIndex);
                }
            }

            return allNulls ? null : sum;
        }

        @Override
        public void initializeValue(DfColumn accumulatorColumn, int accumulatorRowIndex)
        {
            accumulatorColumn.setObject(accumulatorRowIndex, null);
        }

        @Override
        public void aggregateValueIntoLong(DfLongColumnStored targetColumn, int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
        {
            if (sourceColumn.isNull(sourceRowIndex))
            {
                return;
            }

            long nextValue = sourceColumn.getType().isInt()
                    ? this.getIntValue(sourceColumn, sourceRowIndex)
                    : this.getLongValue(sourceColumn, sourceRowIndex);

            if (!targetColumn.isNull(targetRowIndex)) // otherwise we have the first not-null value
            {
                nextValue += targetColumn.getLong(targetRowIndex);
            }

            targetColumn.setLong(targetRowIndex, nextValue);
        }

        @Override
        public void aggregateValueIntoDouble(DfDoubleColumnStored targetColumn, int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
        {
            if (sourceColumn.isNull(sourceRowIndex))
            {
                return;
            }

            double nextValue = this.getDoubleValue(sourceColumn, sourceRowIndex);

            if (!targetColumn.isNull(targetRowIndex)) // otherwise we have the first not-null value
            {
                nextValue += targetColumn.getDouble(targetRowIndex);
            }

            targetColumn.setDouble(targetRowIndex, nextValue);
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
        public boolean nullsArePoisonous()
        {
            return false;
        }
    }
}
