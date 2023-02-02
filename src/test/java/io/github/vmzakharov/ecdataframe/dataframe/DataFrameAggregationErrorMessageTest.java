package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class DataFrameAggregationErrorMessageTest
{
    private DataFrame dataFrame;

    @Before
    public void initialiseDataFrame()
    {
        this.dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Qux")
                .addRow("Alice", "Abc", 123L, 10.0, 100.0)
                .addRow("Bob", "Def", 456L, 12.0, -25.0)
                .addRow("Carol", "Xyz", -789L, 17.0, 42.0);
    }

    @Test
    public void aggString()
    {
        AggregateFunction aggregator = new BadAggregation("Name");
        this.checkAggMessage(aggregator, "non-numeric values");
    }

    @Test
    public void aggLong()
    {
        AggregateFunction aggregator = new BadAggregation("Bar");
        this.checkAggMessage(aggregator, "long values");
    }

    @Test
    public void aggDouble()
    {
        AggregateFunction aggregator = new BadAggregation("Baz");
        this.checkAggMessage(aggregator, "double values");
    }

    public void checkAggMessage(AggregateFunction aggregator, String valueType)
    {
        try
        {
            this.dataFrame.aggregate(Lists.immutable.of(aggregator));
        }
        catch (Exception e)
        {
            Assert.assertEquals(
                    FormatWithPlaceholders.messageFromKey("AGG_NOT_APPLICABLE")
                            .with("operation", aggregator.getName())
                            .with("operationDescription", aggregator.getDescription())
                            .with("operationScope", valueType).toString(),
                    e.getMessage());
        }
    }

    private static class BadAggregation
    extends AggregateFunction
    {
        public BadAggregation(String newColumnName)
        {
            super(newColumnName);
        }

        @Override
        public ListIterable<ValueType> supportedSourceTypes()
        {
            return Lists.immutable.of(ValueType.STRING, ValueType.LONG);
        }
    }
}
