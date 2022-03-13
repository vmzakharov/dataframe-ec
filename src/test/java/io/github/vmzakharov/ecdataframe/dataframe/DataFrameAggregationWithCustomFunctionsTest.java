package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;
import org.junit.Before;
import org.junit.Test;

public class DataFrameAggregationWithCustomFunctionsTest
{
    private DataFrame dataFrame;

    @Before
    public void initialiseDataFrame()
    {
        this.dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Blue",    123L, 10.0, 100.0)
                .addRow("Bob",   "Orange",  456L, 12.0, -25.0)
                .addRow("Bob",   "Orange",  457L, 16.0, -25.0)
                .addRow("Carol", "Green",  -789L, 17.0,  42.0)
                .addRow("Carol", "Purple", -789L, 17.0,  43.0);
    }

    @Test
    public void sumStringLengthsByName()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addStringColumn("Name").addLongColumn("Color")
                        .addRow("Alice",  4)
                        .addRow("Bob",   12)
                        .addRow("Carol", 11),
                this.dataFrame.aggregateBy(Lists.immutable.of(new SumStringLength("Color")), Lists.immutable.of("Name")));
    }

    @Test
    public void sumStringLengths()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addLongColumn("Color")
                        .addRow(27),
                this.dataFrame.aggregate(Lists.immutable.of(new SumStringLength("Color")))
        );
    }

    @Test
    public void constantValuesByName()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar").addDoubleColumn("Qux")
                        .addRow("Alice", "Blue",    123L, 100.0)
                        .addRow("Bob",   "Orange",  null, -25.0)
                        .addRow("Carol", null,     -789L,  null),
                this.dataFrame.aggregateBy(
                        Lists.immutable.of(new ConstantValues("Color"), new ConstantValues("Bar"), new ConstantValues("Qux")),
                        Lists.immutable.of("Name")));
    }

    @Test
    public void constantValues()
    {
        DataFrame df = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar").addDoubleColumn("Baz")
                .addDoubleColumn("Qux").addDoubleColumn("Waldo")
                .addRow("Alice", "Orange", 123L, 123L, 10.0, 100.0)
                .addRow("Bob",   "Orange", 123L, 456L, 12.0, 100.0)
                .addRow("Carol", "Orange", 123L, 123L, 17.0, 100.0);

        DataFrameUtil.assertEquals(
                new DataFrame("Expected")
                        .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar").addDoubleColumn("Baz")
                        .addDoubleColumn("Qux").addDoubleColumn("Waldo")
                        .addRow(null, "Orange", 123L, null, null, 100.0),
                df.aggregate(Lists.immutable.of(
                        new ConstantValues("Name"), new ConstantValues("Color"), new ConstantValues("Bar"), new ConstantValues("Baz"),
                        new ConstantValues("Qux"), new ConstantValues("Waldo"))
                )
        );
    }

    private static class SumStringLength extends AggregateFunction
    {
        public SumStringLength(String newColumnName)
        {
            super(newColumnName);
        }

        @Override
        long longInitialValue()
        {
            return 0;
        }

        @Override
        public long getLongValue(DfColumn sourceColumn, int sourceRowIndex)
        {
            return sourceColumn.getValueAsString(sourceRowIndex).length();
        }

        @Override
        protected long longAccumulator(long currentAggregate, long newValue)
        {
            return currentAggregate + newValue;
        }

        @Override
        public Object applyToObjectColumn(DfObjectColumn<?> objectColumn)
        {
            return objectColumn.toList().collectInt(x -> ((String) x).length()).sum();
        }

        @Override
        public String getDescription()
        {
            return "Sum of Length";
        }

        @Override
        public ValueType targetColumnType(ValueType sourceColumnType)
        {
            return ValueType.LONG;
        }
      }

    private static class ConstantValues extends AggregateFunction
    {
        private static final long INITIAL_VALUE_LONG = System.nanoTime();
        private static final double INITIAL_VALUE_DOUBLE = INITIAL_VALUE_LONG;
        private static final String INITIAL_VALUE_STRING = String.valueOf(INITIAL_VALUE_LONG);

        public ConstantValues(String newColumnName)
        {
            super(newColumnName);
        }

        @Override
        long longInitialValue()
        {
            return INITIAL_VALUE_LONG;
        }

        @Override
        double doubleInitialValue()
        {
            return INITIAL_VALUE_DOUBLE;
        }

        @Override
        Object objectInitialValue()
        {
            return INITIAL_VALUE_STRING;
        }

        @Override
        public boolean handlesObjectIterables()
        {
            return true;
        }

        @Override
        public void aggregateValueIntoLong(DfLongColumnStored targetColumn, int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
        {
            long currentAggregatedValue = targetColumn.getLong(targetRowIndex);
            long nextValue = this.getLongValue(sourceColumn, sourceRowIndex);

            if (currentAggregatedValue == INITIAL_VALUE_LONG)
            {
                targetColumn.setObject(targetRowIndex, nextValue);
            }
            else if (currentAggregatedValue != nextValue)
            {
                targetColumn.setObject(targetRowIndex, null);
            }
        }

        @Override
        public void aggregateValueIntoDouble(DfDoubleColumnStored targetColumn, int targetRowIndex, DfColumn sourceColumn, int sourceRowIndex)
        {
            double currentAggregatedValue = targetColumn.getDouble(targetRowIndex);
            double nextValue = this.getDoubleValue(sourceColumn, sourceRowIndex);

            if (currentAggregatedValue == INITIAL_VALUE_DOUBLE)
            {
                targetColumn.setObject(targetRowIndex, nextValue);
            }
            else if (currentAggregatedValue != nextValue)
            {
                targetColumn.setObject(targetRowIndex, null);
            }
        }

        @Override
        protected Object objectAccumulator(Object currentAggregate, Object newValue)
        {
            if (currentAggregate == INITIAL_VALUE_STRING)
            {
                return newValue;
            }

            if (currentAggregate != null && currentAggregate.equals(newValue))
            {
                return currentAggregate;
            }

            return null;
        }

        @Override
        public Object applyToDoubleColumn(DfDoubleColumn doubleColumn)
        {
            double first = doubleColumn.getDouble(0);
            if (doubleColumn.toDoubleList().allSatisfy(each -> each == first))
            {
                return first;
            }

            return null;
        }

        @Override
        public Object applyToLongColumn(DfLongColumn longColumn)
        {
            long first = longColumn.getLong(0);
            if (longColumn.toLongList().allSatisfy(each -> each == first))
            {
                return first;
            }

            return null;
        }

        @Override
        public Object applyIterable(ListIterable<?> items)
        {
            Object first = items.getFirst();
            if (items.allSatisfy(each -> each.equals(first)))
            {
                return first;
            }

            return null;
        }

        @Override
        public String getDescription()
        {
            return "Const";
        }

        @Override
        public ValueType targetColumnType(ValueType sourceColumnType)
        {
            return sourceColumnType;
        }
    }
}
