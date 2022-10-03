package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.avg;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.count;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.max;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.min;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.same;
import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.sum;

public class DataFrameWithDecimalColumnTest
{
    private DataFrame df;

    @Before
    public void initializeDataFrame()
    {
        this.df = new DataFrame("Data Frame")
                .addStringColumn("Name").addDecimalColumn("Foo").addDecimalColumn("Bar").addDecimalColumn("Waldo")
                .addRow("Alice", new BigDecimal("3.2"), new BigDecimal("123.11"), new BigDecimal("1.234"))
                .addRow("Bob", new BigDecimal("1.1"), new BigDecimal("10.1"), new BigDecimal("1.234"))
                .addRow("Alice", new BigDecimal("123.11"), null, new BigDecimal("1.234"))
                .addRow("Carl", new BigDecimal("2.3"), new BigDecimal("20.3"), new BigDecimal("1.234"))
                ;

        this.df.addDecimalColumn("TwoFoo", "Foo * 2");
    }

    @Test
    public void computedColumns()
    {
        this.df.addDecimalColumn("Baz", "Foo + Bar");
        this.df.addDecimalColumn("Qux", "Foo + 1.23");

        Assert.assertEquals(
            Lists.immutable.of(BigDecimal.valueOf(12631, 2), BigDecimal.valueOf(112, 1), null, BigDecimal.valueOf(226, 1)),
            this.df.getDecimalColumn("Baz").toList()
        );

        Assert.assertEquals(
            Lists.immutable.of(BigDecimal.valueOf(12631, 2), BigDecimal.valueOf(112, 1), null, BigDecimal.valueOf(226, 1)),
            this.df.getDecimalColumn("Baz").toList()
        );
    }

    @Test
    public void aggregationSum()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                     .addDecimalColumn("Foo").addDecimalColumn("Bar").addDecimalColumn("TwoFoo")
                     .addRow(BigDecimal.valueOf(12971, 2), null, BigDecimal.valueOf(25942, 2))
                ,
                this.df.sum(Lists.immutable.of("Foo", "Bar", "TwoFoo"))
        );

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Name").addDecimalColumn("SumFoo").addDecimalColumn("SumBar").addDecimalColumn("SumTwoFoo")
                        .addRow("Alice", BigDecimal.valueOf(12631, 2), null, BigDecimal.valueOf(25262, 2))
                        .addRow("Bob", BigDecimal.valueOf(11, 1), BigDecimal.valueOf(101, 1), BigDecimal.valueOf(22, 1))
                        .addRow("Carl", BigDecimal.valueOf(23, 1), BigDecimal.valueOf(203, 1), BigDecimal.valueOf(46, 1))
                ,
                this.df.aggregateBy(Lists.immutable.of(sum("Foo", "SumFoo"), sum("Bar", "SumBar"), sum("TwoFoo", "SumTwoFoo")),
                                    Lists.immutable.of("Name"))
        );
    }

    @Test
    public void aggregationMin()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                     .addDecimalColumn("Foo").addDecimalColumn("Bar").addDecimalColumn("TwoFoo")
                     .addRow(BigDecimal.valueOf(11, 1), null, BigDecimal.valueOf(22, 1)),
                this.df.aggregate(Lists.immutable.of(min("Foo"), min("Bar"), min("TwoFoo")))
        );

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Name").addDecimalColumn("MinFoo").addDecimalColumn("MinBar").addDecimalColumn("MinTwoFoo")
                        .addRow("Alice", BigDecimal.valueOf(32, 1), null, BigDecimal.valueOf(64, 1))
                        .addRow("Bob", BigDecimal.valueOf(11, 1), BigDecimal.valueOf(101, 1), BigDecimal.valueOf(22, 1))
                        .addRow("Carl", BigDecimal.valueOf(23, 1), BigDecimal.valueOf(203, 1), BigDecimal.valueOf(46, 1))
                ,
                this.df.aggregateBy(
                        Lists.immutable.of(min("Foo", "MinFoo"), min("Bar", "MinBar"), min("TwoFoo", "MinTwoFoo")),
                        Lists.immutable.of("Name"))
        );
    }

    @Test
    public void aggregationMax()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                     .addDecimalColumn("Foo").addDecimalColumn("Bar").addDecimalColumn("TwoFoo")
                     .addRow(BigDecimal.valueOf(12311, 2), null, BigDecimal.valueOf(24622, 2)),
                this.df.aggregate(Lists.immutable.of(max("Foo"), max("Bar"), max("TwoFoo")))
        );

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Name").addDecimalColumn("MaxFoo").addDecimalColumn("MaxBar").addDecimalColumn("MaxTwoFoo")
                        .addRow("Alice", BigDecimal.valueOf(12311, 2), null, BigDecimal.valueOf(24622, 2))
                        .addRow("Bob", BigDecimal.valueOf(11, 1), BigDecimal.valueOf(101, 1), BigDecimal.valueOf(22, 1))
                        .addRow("Carl", BigDecimal.valueOf(23, 1), BigDecimal.valueOf(203, 1), BigDecimal.valueOf(46, 1))
                ,
                this.df.aggregateBy(
                        Lists.immutable.of(max("Foo", "MaxFoo"), max("Bar", "MaxBar"), max("TwoFoo", "MaxTwoFoo")),
                        Lists.immutable.of("Name"))
        );
    }

    @Test
    public void aggregationAverage()
    {
        BigDecimal count = BigDecimal.valueOf(4);

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                     .addDecimalColumn("Foo").addDecimalColumn("Bar").addDecimalColumn("TwoFoo")
                     .addRow(BigDecimal.valueOf(12971, 2).divide(count, RoundingMode.HALF_UP), null, BigDecimal.valueOf(25942, 2).divide(count, RoundingMode.HALF_UP)),
                this.df.aggregate(Lists.immutable.of(avg("Foo"), avg("Bar"), avg("TwoFoo")))
        );

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Name").addDecimalColumn("AvgFoo").addDecimalColumn("AvgBar").addDecimalColumn("AvgTwoFoo")
                        .addRow("Alice", BigDecimal.valueOf(63155, 3), null, BigDecimal.valueOf(12631, 2))
                        .addRow("Bob", BigDecimal.valueOf(11, 1), BigDecimal.valueOf(101, 1), BigDecimal.valueOf(22, 1))
                        .addRow("Carl", BigDecimal.valueOf(23, 1), BigDecimal.valueOf(203, 1), BigDecimal.valueOf(46, 1))
                ,
                this.df.aggregateBy(
                        Lists.immutable.of(avg("Foo", "AvgFoo"), avg("Bar", "AvgBar"), avg("TwoFoo", "AvgTwoFoo")),
                        Lists.immutable.of("Name"))
        );
    }

    @Test
    public void aggregationCount()
    {
        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                     .addLongColumn("Foo").addLongColumn("Bar").addLongColumn("TwoFoo")
                     .addRow(4, 4, 4),
                this.df.aggregate(Lists.immutable.of(count("Foo"), count("Bar"), count("TwoFoo")))
        );

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Name").addLongColumn("CountFoo").addLongColumn("CountBar").addLongColumn("CountTwoFoo")
                        .addRow("Alice", 2, 2, 2)
                        .addRow("Bob", 1, 1, 1)
                        .addRow("Carl", 1, 1, 1)
                ,
                this.df.aggregateBy(
                        Lists.immutable.of(count("Foo", "CountFoo"), count("Bar", "CountBar"), count("TwoFoo", "CountTwoFoo")),
                        Lists.immutable.of("Name"))
        );
    }

    @Test
    public void aggregationSame()
    {
//        this.df.addDecimalColumn("TwoWaldo", "Waldo * 2.0");
        this.df.addDecimalColumn("TwoWaldo", "Waldo + Waldo");

        DataFrame aggregated = this.df.aggregate(Lists.immutable.of(same("Foo"), same("Bar"), same("Waldo"), same("TwoWaldo")));

        // forced to use compareTo to compare BigDecimals with different scales as equals returns false in this scenario
        // i.e. BigDecimal.valueOf(2468, 3).equals(BigDecimal.valueOf(24680, 4)) is false, while
        // BigDecimal.valueOf(2468, 3).compareTo(BigDecimal.valueOf(24680, 4)) == 0
        Assert.assertNull(aggregated.getObject("Foo", 0));
        Assert.assertNull(aggregated.getObject("Bar", 0));
        Assert.assertEquals(0, BigDecimal.valueOf(1234, 3).compareTo(aggregated.getDecimal("Waldo", 0)));
        Assert.assertEquals(0, BigDecimal.valueOf(2468, 3).compareTo(aggregated.getDecimal("TwoWaldo", 0)));

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Name").addDecimalColumn("SameFoo").addDecimalColumn("SameWaldo").addDecimalColumn("SameTwoWaldo")
                        .addRow("Alice", null, BigDecimal.valueOf(1234, 3), BigDecimal.valueOf(2468, 3))
                        .addRow("Bob", BigDecimal.valueOf(11, 1), BigDecimal.valueOf(1234, 3), BigDecimal.valueOf(2468, 3))
                        .addRow("Carl", BigDecimal.valueOf(23, 1), BigDecimal.valueOf(1234, 3), BigDecimal.valueOf(2468, 3))
                ,
                this.df.aggregateBy(
                        Lists.immutable.of(same("Foo", "SameFoo"), same("Waldo", "SameWaldo"), same("TwoWaldo", "SameTwoWaldo")),
                        Lists.immutable.of("Name"))
        );
    }
}
