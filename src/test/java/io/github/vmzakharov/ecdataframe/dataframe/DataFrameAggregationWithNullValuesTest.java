package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.impl.factory.Lists;
import org.junit.Test;

import java.time.LocalDate;

public class DataFrameAggregationWithNullValuesTest
{
    @Test
    public void countNulls()
    {
          DataFrame dataFrame = new DataFrame("FrameOfData")
            .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar").addDoubleColumn("Baz")
            .addDateColumn("Date")
            .addRow("Alice",    "Blue",  123L, null, LocalDate.of(2021, 11, 21))
            .addRow(null,     "Orange",  456L, 12.0, null)
            .addRow("Bob",    "Violet",  457L, 16.0, LocalDate.of(2021, 11, 23))
            .addRow("Carol",   "Green",  null, 17.0, null)
            .addRow(null,     "Purple",  null, 17.0, LocalDate.of(2021, 12, 22));

        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addLongColumn("Null Color Count").addLongColumn("Null Name Count")
                        .addLongColumn("Bar").addLongColumn("Baz").addLongColumn("Date")
                        .addRow(0, 2, 2, 1, 2),
                dataFrame.aggregate(
                        Lists.immutable.of(
                                new CountNulls("Color", "Null Color Count"),
                                new CountNulls("Name", "Null Name Count"),
                                new CountNulls("Bar"),
                                new CountNulls("Baz"),
                                new CountNulls("Date")
                        ))
        );
    }

    @Test
    public void countNullsWithGrouping()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
            .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar").addDoubleColumn("Baz")
            .addDateColumn("Date")
            .addRow("Alice",   "Blue",  123L, null, LocalDate.of(2021, 11, 21))
            .addRow("Alice", "Orange",  456L, 12.0, null)
            .addRow("Bob",   "Violet",  457L, 16.0, LocalDate.of(2021, 11, 23))
            .addRow("Carol",  "Green",  null, 17.0, null)
            .addRow("Carol", "Purple",  null, 17.0, LocalDate.of(2021, 12, 22));

        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addStringColumn("Name").addLongColumn("Null Color Count").addLongColumn("Null Name Count")
                        .addLongColumn("Bar").addLongColumn("Baz").addLongColumn("Date")
                        .addRow("Alice", 0, 0, 0, 1, 1)
                        .addRow("Bob",   0, 0, 0, 0, 0)
                        .addRow("Carol", 0, 0, 2, 0, 1),
                dataFrame.aggregateBy(
                        Lists.immutable.of(
                                new CountNulls("Color", "Null Color Count"),
                                new CountNulls("Name", "Null Name Count"),
                                new CountNulls("Bar"),
                                new CountNulls("Baz"),
                                new CountNulls("Date")
                        ),
                        Lists.immutable.of("Name")));
    }

    @Test
    public void sumIgnoringNulls()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz")
                .addLongColumn("Waldo").addDoubleColumn("Qux")
                .addRow("Alice", 123L, null, null, null)
                .addRow("Alice", 456L, 12.0, null, null)
                .addRow("Bob",   457L, null, null, null)
                .addRow("Bob",   111L, null, null, null)
                .addRow("Carol", null, 17.0, null, null)
                .addRow("Carol", null, 18.0, null, null);

        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addLongColumn("Bar").addDoubleColumn("Baz")
                        .addLongColumn("Waldo").addDoubleColumn("Qux")
                        .addRow(1147L, 47.0, null, null),
                dataFrame.aggregate(Lists.immutable.of(
                        new SumIgnoreNulls("Bar"),
                        new SumIgnoreNulls("Baz"),
                        new SumIgnoreNulls("Waldo"),
                        new SumIgnoreNulls("Qux")
                    ))
        );
    }

    @Test
    public void sumIgnoringNullsWithGrouping()
    {
        DataFrame dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz")
                .addRow("Alice", 123L, null)
                .addRow("Alice", 456L, 12.0)
                .addRow("Bob",   457L, null)
                .addRow("Bob",   111L, null)
                .addRow("Carol", null, 17.0)
                .addRow("Carol", null, 18.0);

        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addStringColumn("Name").addLongColumn("Bar").addDoubleColumn("Baz")
                        .addRow("Alice", 579L, 12.0)
                        .addRow("Bob",   568L, null)
                        .addRow("Carol", null, 35.0),
                dataFrame.aggregateBy(
                        Lists.immutable.of(
                                new SumIgnoreNulls("Bar"),
                                new SumIgnoreNulls("Baz")
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
        public long longInitialValue()
        {
            return 0;
        }

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
        public String getDescription()
        {
            return "Count of null values";
        }

        @Override
        public ValueType targetColumnType(ValueType sourceColumnType)
        {
            return ValueType.LONG;
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

            long nextValue = this.getLongValue(sourceColumn, sourceRowIndex);

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
        public long defaultLongIfEmpty()
        {
            return 0L;
        }

        @Override
        public double defaultDoubleIfEmpty()
        {
            return 0.0;
        }

        @Override
        public boolean nullsArePoisonous()
        {
            return false;
        }
    }
}
