package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.STRING;

public class DataFrameAggregationWithCustomFunctionsTest
{
    private DataFrame dataFrame;

    @Before
    public void initialiseDataFrame()
    {
        this.dataFrame = new DataFrame("FrameOfData")
            .addStringColumn("Name").addStringColumn("Color").addLongColumn("Bar").addDoubleColumn("Baz").addDateColumn("Date")
            .addRow("Alice", "Blue",    123L, 10.0, LocalDate.of(2021, 11, 21))
            .addRow("Bob",   "Orange",  456L, 12.0, LocalDate.of(2021, 11, 21))
            .addRow("Bob",   "Violet",  457L, 16.0, LocalDate.of(2021, 11, 23))
            .addRow("Carol", "Green",  -789L, 17.0, LocalDate.of(2021, 12, 22))
            .addRow("Carol", "Purple", -789L, 17.0, LocalDate.of(2021, 12, 22));
    }

    @Test
    public void sumStringLengthsByName()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addStringColumn("Name").addLongColumn("Color").addLongColumn("Name Lengths")
                        .addRow("Alice",  4,  5)
                        .addRow("Bob",   12,  6)
                        .addRow("Carol", 11, 10),
                this.dataFrame.aggregateBy(
                        Lists.immutable.of(new SumStringLength("Color"), new SumStringLength("Name", "Name Lengths")),
                        Lists.immutable.of("Name")));
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
    public void concatByName()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addStringColumn("Name").addStringColumn("Colors")
                        .addRow("Alice", "Blue")
                        .addRow("Bob",   "OrangeViolet")
                        .addRow("Carol", "GreenPurple"),
                this.dataFrame.aggregateBy(
                        Lists.immutable.of(new Concat("Color", "Colors")),
                        Lists.immutable.of("Name")));
    }

    @Test
    public void concat()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("aggregated")
                        .addStringColumn("Name").addStringColumn("Color")
                        .addRow("AliceBobBobCarolCarol", "BlueOrangeVioletGreenPurple")
                ,
                this.dataFrame.aggregate(Lists.immutable.of(new Concat("Name"), new Concat("Color")))
        );
    }

    private static class SumStringLength extends AggregateFunction
    {
        public SumStringLength(String newColumnName)
        {
            super(newColumnName);
        }

        public SumStringLength(String newColumnName, String newTargetColumnName)
        {
            super(newColumnName, newTargetColumnName);
        }

        public ListIterable<ValueType> supportedSourceTypes()
        {
            return Lists.immutable.of(STRING);
        }

        @Override
        public long longInitialValue()
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

    private static class Concat extends AggregateFunction
    {
        public Concat(String newColumnName)
        {
            super(newColumnName);
        }

        public Concat(String newColumnName, String newTargetColumnName)
        {
            super(newColumnName, newTargetColumnName);
        }

        public ListIterable<ValueType> supportedSourceTypes()
        {
            return Lists.immutable.of(STRING);
        }

        @Override
        public Object objectInitialValue()
        {
            return "";
        }

        @Override
        protected Object objectAccumulator(Object currentAggregate, Object newValue)
        {
            return new StringBuilder().append(currentAggregate).append(newValue).toString();
        }

        @Override
        public Object applyToObjectColumn(DfObjectColumn<?> objectColumn)
        {
            return objectColumn.toList().collect(String::valueOf).makeString("");
        }

        @Override
        public String getDescription()
        {
            return "Concat";
        }

        @Override
        public ValueType targetColumnType(ValueType sourceColumnType)
        {
            return STRING;
        }
    }
}
