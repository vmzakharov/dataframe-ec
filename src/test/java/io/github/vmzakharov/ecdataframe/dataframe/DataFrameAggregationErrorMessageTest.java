package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.impl.factory.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;

public class DataFrameAggregationErrorMessageTest
{
    private DataFrame dataFrame;

    @Before
    public void initialiseDataFrame()
    {
        this.dataFrame = new DataFrame("FrameOfData")
                .addStringColumn("Name").addStringColumn("Foo").addLongColumn("Bar").addDoubleColumn("Baz")
                .addIntColumn("Waldo").addDateColumn("Corge")
                .addRow("Alice", "Abc",  123L, 10.0, 100, LocalDate.of(2023, 10, 21))
                .addRow("Bob",   "Def",  456L, 12.0, -25, LocalDate.of(2024, 11, 22))
                .addRow("Carol", "Xyz", -789L, 17.0,  42, LocalDate.of(2025, 12, 23));
    }

    @Test
    public void aggString()
    {
        this.checkAggMessage("Name", "string");
    }

    @Test
    public void aggLong()
    {
        this.checkAggMessage("Bar", "long");
    }

    @Test
    public void aggDouble()
    {
        this.checkAggMessage("Baz", "double");
    }

    @Test
    public void aggInt()
    {
        this.checkAggMessage("Waldo", "int");
    }

    @Test
    public void aggDate()
    {
        this.checkAggMessage("Corge", "date");
    }

    public void checkAggMessage(String columnName, String columnType)
    {
        AggregateFunction aggregator = new BadAggregation(columnName);

        try
        {
            this.dataFrame.aggregate(Lists.immutable.of(aggregator));
        }
        catch (Exception e)
        {
            Assert.assertEquals(
                    FormatWithPlaceholders.messageFromKey("AGG_COL_TYPE_UNSUPPORTED")
                            .with("operation", aggregator.getName())
                            .with("operationDescription", aggregator.getDescription())
                            .with("columnName", columnName)
                            .with("columnType", columnType)
                            .toString(),
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
