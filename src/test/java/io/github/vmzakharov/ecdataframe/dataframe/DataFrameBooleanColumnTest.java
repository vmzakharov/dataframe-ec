package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import org.eclipse.collections.api.tuple.Twin;
import org.eclipse.collections.impl.factory.Lists;
import org.eclipse.collections.impl.factory.primitive.BooleanLists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DataFrameBooleanColumnTest
{
    private DataFrame donutOrders;

    @BeforeEach
    public void setUpOrders()
    {
        this.donutOrders = new DataFrame("Donut Orders")
            .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
            .addRow("Alice", "Jelly",         12,  true)
            .addRow("Bob",   "Old Fashioned",  6,  true)
            .addRow("Alice", "Apple Cider",    6, false)
            .addRow("Carol", "Jelly",          2,  true)
            .addRow("Dave",  "Apple Cider",   10, false)
            .addRow("Dave",  "Old Fashioned", 10, false)
            .addRow("Alice", "Apple Cider",    1,  true)
        ;
    }

    @Test
    public void valueAccess()
    {
        DfBooleanColumn deliveredColumn = this.donutOrders.getBooleanColumn("Delivered");

        assertEquals(
            BooleanLists.immutable.of(true, true, false, true, false, false, true),
            deliveredColumn.toBooleanList()
        );

        assertEquals(BooleanValue.TRUE, deliveredColumn.getValue(0));
        assertEquals(BooleanValue.FALSE, deliveredColumn.getValue(2));

        assertTrue(this.donutOrders.getBoolean("Delivered", 0));
        assertTrue(this.donutOrders.getBoolean("Delivered", 1));
        assertFalse(this.donutOrders.getBoolean("Delivered", 2));
        assertTrue(this.donutOrders.getBoolean("Delivered", 3));

        this.donutOrders.sortBy(Lists.immutable.of("Client"));

        assertTrue(this.donutOrders.getBoolean("Delivered", 0));
        assertFalse(this.donutOrders.getBoolean("Delivered", 1));
        assertTrue(this.donutOrders.getBoolean("Delivered", 2));
        assertTrue(this.donutOrders.getBoolean("Delivered", 3));
    }

    @Test
    public void sort()
    {
        DataFrameUtil.assertEquals(
            new DataFrame("Expected")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Alice", "Apple Cider",    6, false)
                .addRow("Dave",  "Apple Cider",   10, false)
                .addRow("Dave",  "Old Fashioned", 10, false)
                .addRow("Alice", "Jelly",         12,  true)
                .addRow("Bob",   "Old Fashioned",  6,  true)
                .addRow("Carol", "Jelly",          2,  true)
                .addRow("Alice", "Apple Cider",    1,  true)
                ,
            this.donutOrders.sortBy(Lists.immutable.of("Delivered")));
    }

    @Test
    public void select()
    {
        DataFrameUtil.assertEquals(
            new DataFrame("Expected")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Alice", "Jelly",         12,  true)
                .addRow("Bob",   "Old Fashioned",  6,  true)
                .addRow("Carol", "Jelly",          2,  true)
                .addRow("Alice", "Apple Cider",    1,  true)
                ,
            this.donutOrders.selectBy("Delivered")
        );

        DataFrameUtil.assertEquals(
            new DataFrame("Expected")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Alice", "Apple Cider",    6, false)
                .addRow("Dave",  "Apple Cider",   10, false)
                .addRow("Dave",  "Old Fashioned", 10, false),
            this.donutOrders.selectBy("not Delivered")
        );
    }

    @Test
    public void computedColum()
    {
        DataFrameUtil.assertEquals(
            new DataFrame("Expected")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity")
                .addBooleanColumn("Delivered").addBooleanColumn("LargeOrder")
                .addRow("Alice", "Jelly",         12,  true,  true)
                .addRow("Bob",   "Old Fashioned",  6,  true, false)
                .addRow("Alice", "Apple Cider",    6, false, false)
                .addRow("Carol", "Jelly",          2,  true, false)
                .addRow("Dave",  "Apple Cider",   10, false,  true)
                .addRow("Dave",  "Old Fashioned", 10, false,  true)
                .addRow("Alice", "Apple Cider",    1,  true, false)
            ,
            this.donutOrders.addColumn("LargeOrder", "Quantity > 6")
        );
    }

    @Test
    public void handlingNulls()
    {
        DataFrame unclearOrders = new DataFrame("Donut Orders")
            .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
            .addRow("Alice", "Jelly",         12,  true)
            .addRow("Bob",   "Old Fashioned",  6,  null)
            .addRow("Alice", "Apple Cider",    6, false)
            .addRow("Carol", "Jelly",          2,  true)
            .addRow("Dave",  "Apple Cider",   10,  null)
            .addRow("Dave",  "Old Fashioned", 10, false)
            .addRow("Alice", "Apple Cider",    1,  null)
        ;

        assertFalse(unclearOrders.isNull("Delivered", 0));
        assertTrue(unclearOrders.isNull("Delivered", 1));
        assertFalse(unclearOrders.isNull("Delivered", 2));
        assertTrue(unclearOrders.isNull("Delivered", 6));

        Twin<DataFrame> unknownAndKnown = unclearOrders.partition("Delivered is null");

        DataFrameUtil.assertEquals(
            new DataFrame("Delivery status unknown")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Bob",   "Old Fashioned",  6,  null)
                .addRow("Dave",  "Apple Cider",   10,  null)
                .addRow("Alice", "Apple Cider",    1,  null)
            ,
            unknownAndKnown.getOne()
        );

        DataFrameUtil.assertEquals(
            new DataFrame("Delivery status known")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Alice", "Jelly",         12,  true)
                .addRow("Alice", "Apple Cider",    6, false)
                .addRow("Carol", "Jelly",          2,  true)
                .addRow("Dave",  "Old Fashioned", 10, false)
            ,
            unknownAndKnown.getTwo()
        );
    }
}
