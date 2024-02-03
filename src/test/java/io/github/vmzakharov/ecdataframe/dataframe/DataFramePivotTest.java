package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.impl.factory.Lists;
import org.junit.Before;
import org.junit.Test;

import static io.github.vmzakharov.ecdataframe.dataframe.AggregateFunction.sum;

public class DataFramePivotTest
{
    private DataFrame donutSales;
    
    @Before
    public void initializeDonutSales()
    {
        this.donutSales = new DataFrame("Donut Shop Purchases")
                .addStringColumn("Customer").addStringColumn("Month").addStringColumn("Donut Type").addLongColumn("Qty").addDoubleColumn("Charge")
                .addRow("Alice", "Jan", "Blueberry",     10, 10.00)
                .addRow("Alice", "Feb", "Glazed",        10, 12.00)
                .addRow("Alice", "Feb", "Old Fashioned", 10,  8.00)
                .addRow("Alice", "Jan", "Blueberry",     10, 10.00)
                .addRow("Bob",   "Jan", "Blueberry",      5,  5.00)
                .addRow("Bob",   "Jan", "Pumpkin Spice",  5, 10.00)
                .addRow("Bob",   "Jan", "Apple Cider",    4,  4.40)
                .addRow("Bob",   "Mar", "Apple Cider",    8,  8.80)
                .addRow("Dave",  "Jan", "Blueberry",     10, 10.00)
                .addRow("Dave",  "Jan", "Old Fashioned", 20, 16.00)
                .addRow("Carol", "Jan", "Blueberry",      6,  6.00)
                .addRow("Carol", "Feb", "Old Fashioned", 12,  9.60)
                .addRow("Carol", "Mar", "Jelly",         10, 15.00)
                .addRow("Carol", "Jan", "Apple Cider",   12, 13.20)
                .seal();
    }
    
    @Test
    public void pivotCustomerByTypeAggQty()
    {
        DataFrame pivoted = this.donutSales.pivot(
                Lists.immutable.of("Customer"),
                "Donut Type",
                Lists.immutable.of(sum("Qty"))
        );

        DataFrame expected = new DataFrame("pivoted")
                .addStringColumn("Customer")
                .addLongColumn("Blueberry").addLongColumn("Glazed").addLongColumn("Old Fashioned")
                .addLongColumn("Pumpkin Spice").addLongColumn("Apple Cider").addLongColumn("Jelly")
                .addRow("Alice", 20, 10, 10, 0,  0,  0)
                .addRow("Bob",    5,  0,  0, 5, 12,  0)
                .addRow("Dave",  10,  0, 20, 0,  0,  0)
                .addRow("Carol",  6,  0, 12, 0, 12, 10)
                 ;

        DataFrameUtil.assertEquals(expected, pivoted);
    }

    @Test
    public void pivotCustomerByTypeAggCharge()
    {
        DataFrame pivoted = this.donutSales.pivot(
                Lists.immutable.of("Customer"),
                "Donut Type",
                Lists.immutable.of(sum("Charge"))
        );

        DataFrame expected = new DataFrame("pivoted")
                .addStringColumn("Customer")
                .addDoubleColumn("Blueberry").addDoubleColumn("Glazed").addDoubleColumn("Old Fashioned")
                .addDoubleColumn("Pumpkin Spice").addDoubleColumn("Apple Cider").addDoubleColumn("Jelly")
                .addRow("Alice", 20.0, 12.0,  8.0,  0.0,  0.0,  0.0)
                .addRow("Bob",    5.0,  0.0,  0.0, 10.0, 13.2,  0.0)
                .addRow("Dave",  10.0,  0.0, 16.0,  0.0,  0.0,  0.0)
                .addRow("Carol",  6.0,  0.0,  9.6,  0.0, 13.2, 15.0)
                ;

        DataFrameUtil.assertEquals(expected, pivoted, 0.000001);
    }

    @Test
    public void pivotCustomerByMonthAggQty()
    {
        DataFrame pivoted = this.donutSales.pivot(
                Lists.immutable.of("Customer"),
                "Month",
                Lists.immutable.of(sum("Qty"))
        );

        DataFrame expected = new DataFrame("pivoted")
                .addStringColumn("Customer")
                .addLongColumn("Jan").addLongColumn("Feb").addLongColumn("Mar")
                .addRow("Alice", 20, 20,  0)
                .addRow("Bob",   14,  0,  8)
                .addRow("Dave",  30,  0,  0)
                .addRow("Carol", 18, 12, 10)
                ;

        DataFrameUtil.assertEquals(expected, pivoted);
    }

    @Test
    public void pivotMonthByMonthAggQty()
    {
        DataFrame pivoted = this.donutSales.pivot(
                Lists.immutable.of("Month"),
                "Month",
                Lists.immutable.of(sum("Qty"))
        );

        DataFrame expected = new DataFrame("expected")
                .addStringColumn("Month").addLongColumn("Jan").addLongColumn("Feb").addLongColumn("Mar")
                .addRow("Jan", 82,  0,  0)
                .addRow("Feb",  0, 32,  0)
                .addRow("Mar",  0,  0, 18)
                ;

        DataFrameUtil.assertEquals(pivoted, expected);
    }

    @Test
    public void pivotCustomerAndMonthByTypeAggQty()
    {
        DataFrame pivoted = this.donutSales.pivot(
                Lists.immutable.of("Customer", "Month"),
                "Donut Type",
                Lists.immutable.of(sum("Qty"))
        );

        DataFrame expected = new DataFrame("pivoted")
                .addStringColumn("Customer").addStringColumn("Month")
                .addLongColumn("Blueberry").addLongColumn("Glazed").addLongColumn("Old Fashioned").addLongColumn("Pumpkin Spice").addLongColumn("Apple Cider").addLongColumn("Jelly")
                .addRow("Alice", "Jan", 20,  0,  0, 0,  0,  0)
                .addRow("Alice", "Feb",  0, 10, 10, 0,  0,  0)
                .addRow("Bob",   "Jan",  5,  0,  0, 5,  4,  0)
                .addRow("Bob",   "Mar",  0,  0,  0, 0,  8,  0)
                .addRow("Dave",  "Jan", 10,  0, 20, 0,  0,  0)
                .addRow("Carol", "Jan",  6,  0,  0, 0, 12,  0)
                .addRow("Carol", "Feb",  0,  0, 12, 0,  0,  0)
                .addRow("Carol", "Mar",  0,  0,  0, 0,  0, 10)
                ;

        DataFrameUtil.assertEquals(expected, pivoted);
    }
}
