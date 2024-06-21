package io.github.vmzakharov.ecdataframe.dataframe;

import org.junit.jupiter.api.Test;

public class DataFrameComputedWithNullsTest
{
    @Test
    public void numericValueNulls()
    {
        DataFrame df = new DataFrame("Frame-o-data")
                .addStringColumn("Security").addLongColumn("Qty").addDoubleColumn("Price")
                .addRow("IBM",     10, 100.0)
                .addRow("MSFT",    20,  null)
                .addRow("AAPL",  null, 300.0)
                .addRow("PONGF",   15,  12.0)
                ;

        df.addDoubleColumn("CalcMv", "Price is null ? 0.0 : Price * Qty");
        df.addLongColumn("AdjQty", "Qty is not null ? Qty - 5 : Qty");

        DataFrameUtil.assertEquals(
                new DataFrame("Frame-o-data")
                    .addStringColumn("Security").addLongColumn("Qty").addDoubleColumn("Price")
                    .addDoubleColumn("CalcMv").addLongColumn("AdjQty")
                    .addRow("IBM",   10,   100.0, 1000.0,    5)
                    .addRow("MSFT",  20,   null,     0.0,   15)
                    .addRow("AAPL",  null, 300.0,   null, null)
                    .addRow("PONGF", 15,    12.0,  180.0,   10)
                , df
        );
    }

    @Test
    public void numericValueNullsIntQty()
    {
        DataFrame df = new DataFrame("Frame-o-data")
                .addStringColumn("Security").addIntColumn("Qty").addDoubleColumn("Price")
                .addRow("IBM",     10, 100.0)
                .addRow("MSFT",    20,  null)
                .addRow("AAPL",  null, 300.0)
                .addRow("PONGF",   15,  12.0)
                ;

        df.addDoubleColumn("CalcMv", "Price is null ? 0.0 : Price * Qty");
        df.addLongColumn("AdjQty", "Qty is not null ? Qty - 5 : Qty");

        DataFrameUtil.assertEquals(
                new DataFrame("Frame-o-data")
                        .addStringColumn("Security").addIntColumn("Qty").addDoubleColumn("Price")
                        .addDoubleColumn("CalcMv").addLongColumn("AdjQty")
                        .addRow("IBM",   10,   100.0, 1000.0,    5)
                        .addRow("MSFT",  20,   null,     0.0,   15)
                        .addRow("AAPL",  null, 300.0,   null, null)
                        .addRow("PONGF", 15,    12.0,  180.0,   10)
                , df
        );
    }

    @Test
    public void functionsAndCalculationsWithNulls()
    {
        DataFrame df = new DataFrame("Frame-o-data")
                .addStringColumn("Security").addLongColumn("Qty").addDoubleColumn("Price")
                .addRow("IBM",     10, 100.0)
                .addRow("MSFT",   -20,  null)
                .addRow("AAPL",  null, 300.0)
                .addRow(null,     -15,  12.0)
                ;

        df.addStringColumn("FirstChar", "substr(Security, 0, 1)");
        df.addLongColumn("AbsQty", "abs(Qty)");
        df.addDoubleColumn("MarketValue", "Qty * Price");

        DataFrameUtil.assertEquals(
                new DataFrame("Frame-o-data")
                        .addStringColumn("Security").addLongColumn("Qty").addDoubleColumn("Price")
                        .addStringColumn("FirstChar").addLongColumn("AbsQty").addDoubleColumn("MarketValue")
                        .addRow("IBM",     10,  100.0,  "I",   10, 1000.0)
                        .addRow("MSFT",   -20,   null,  "M",   20,   null)
                        .addRow("AAPL",  null,  300.0,  "A", null,   null)
                        .addRow(null,     -15,   12.0, null,   15, -180.0)
                , df
        );
    }

    @Test
    public void functionsAndCalculationsWithNullsIntQty()
    {
        DataFrame df = new DataFrame("Frame-o-data")
                .addStringColumn("Security").addIntColumn("Qty").addDoubleColumn("Price")
                .addRow("IBM",     10, 100.0)
                .addRow("MSFT",   -20,  null)
                .addRow("AAPL",  null, 300.0)
                .addRow(null,     -15,  12.0)
                ;

        df.addStringColumn("FirstChar", "substr(Security, 0, 1)");
        df.addIntColumn("AbsQty", "abs(Qty)");
        df.addDoubleColumn("MarketValue", "Qty * Price");

        DataFrameUtil.assertEquals(
                new DataFrame("Frame-o-data")
                        .addStringColumn("Security").addIntColumn("Qty").addDoubleColumn("Price")
                        .addStringColumn("FirstChar").addIntColumn("AbsQty").addDoubleColumn("MarketValue")
                        .addRow("IBM",     10,  100.0,  "I",   10, 1000.0)
                        .addRow("MSFT",   -20,   null,  "M",   20,   null)
                        .addRow("AAPL",  null,  300.0,  "A", null,   null)
                        .addRow(null,     -15,   12.0, null,   15, -180.0)
                , df
        );
    }
}
