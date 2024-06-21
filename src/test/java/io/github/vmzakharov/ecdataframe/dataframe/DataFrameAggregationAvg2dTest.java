package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.*;
import static org.junit.jupiter.api.Assertions.*;

public class DataFrameAggregationAvg2dTest
{
    static private final double TOLERANCE = 0.0000001;

    private DataFrame dataFrame;

    @BeforeEach
    public void initialiseDataFrame()
    {
        this.dataFrame = new DataFrame("Data Fame")
                .addStringColumn("Name").addStringColumn("Foo")
                .addLongColumn("Bar").addDoubleColumn("Baz").addIntColumn("Waldo").addFloatColumn("Thud")
                .addDecimalColumn("Xyzzy")
                .addRow("Alice", "aaa", 100L, 100.0,  5, 11.0f, BigDecimal.valueOf(100, 2))
                .addRow("Alice", "aaa", 121L, 120.0,  4, 12.0f, BigDecimal.valueOf(120, 2))
                .addRow("Bob",   "aaa", -10L,  80.0,  8, 10.0f, BigDecimal.valueOf(90, 2))
                .addRow("Carol", "ccc", 100L, 100.0,  1, 12.0f, BigDecimal.valueOf(110, 2))
                .addRow("Bob",   "bbb", -20L,  60.0,  6, 16.0f, BigDecimal.valueOf(60, 2))
                .addRow("Bob",   "aaa", -30L, 120.0, 12, 10.0f, BigDecimal.valueOf(120, 2))
                ;
    }

    @Test
    public void averageAll()
    {
        DataFrame aggregated = this.dataFrame.aggregate(
                Lists.immutable.of(avg2d("Bar"), avg2d("Baz"), avg2d("Waldo", "Avg Waldo"), avg2d("Thud", "Avg Thud"),
                avg2d("Xyzzy"))
        );

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addDoubleColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Avg Waldo").addDoubleColumn("Avg Thud")
                        .addDecimalColumn("Xyzzy")
                        .addRow(43.5, 96.66666667, 6, 11.83333333, BigDecimal.valueOf(100, 2)),
                aggregated, TOLERANCE
        );
    }

    @Test
    public void averageWithGrouping()
    {
        DataFrame aggregated = this.dataFrame.aggregateBy(
                Lists.immutable.of(avg2d("Bar"), avg2d("Baz"), avg2d("Waldo"), avg2d("Thud"), avg2d("Xyzzy")),
                Lists.immutable.of("Name")
        );

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Name")
                        .addDoubleColumn("Bar").addDoubleColumn("Baz").addDoubleColumn("Waldo").addDoubleColumn("Thud")
                        .addDecimalColumn("Xyzzy")
                        .addRow("Alice", 110.5, 110, 4.5, 11.5, BigDecimal.valueOf(110, 2))
                        .addRow("Bob", -20, 86.66666667, 8.666666667, 12, BigDecimal.valueOf(90, 2))
                        .addRow("Carol", 100, 100, 1, 12, BigDecimal.valueOf(110, 2)),
                aggregated, TOLERANCE
        );
    }

    @Test
    public void averageEmptyDataFrame()
    {
        assertThrows(
            RuntimeException.class,
            () -> new DataFrame("Data Fame")
                    .addStringColumn("Name").addStringColumn("Foo")
                    .addLongColumn("Bar").addDoubleColumn("Baz").addIntColumn("Waldo").addFloatColumn("Thud")
                    .aggregate(Lists.immutable.of(avg2d("Bar"), avg2d("Waldo")))
        );
    }

    @Test
    public void averageEmptyDataFrameWithGrouping()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("Data Fame")
                        .addStringColumn("Name").addDoubleColumn("Bar").addDoubleColumn("Waldo"),

                new DataFrame("Data Fame")
                        .addStringColumn("Name").addStringColumn("Foo")
                        .addLongColumn("Bar").addDoubleColumn("Baz").addIntColumn("Waldo").addFloatColumn("Thud")
                        .aggregateBy(
                                Lists.immutable.of(avg2d("Bar"), avg2d("Waldo")),
                                Lists.immutable.of("Name"))
                );
    }
}
