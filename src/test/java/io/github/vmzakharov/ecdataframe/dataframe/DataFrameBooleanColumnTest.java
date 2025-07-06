package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.StringValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders;
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
            .addRow("Alice", "Jelly", 12, true)
            .addRow("Bob", "Old Fashioned", 6, true)
            .addRow("Alice", "Apple Cider", 6, false)
            .addRow("Carol", "Jelly", 2, true)
            .addRow("Dave", "Apple Cider", 10, false)
            .addRow("Dave", "Old Fashioned", 10, false)
            .addRow("Alice", "Apple Cider", 1, true)
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
                .addRow("Alice", "Apple Cider", 6, false)
                .addRow("Dave", "Apple Cider", 10, false)
                .addRow("Dave", "Old Fashioned", 10, false)
                .addRow("Alice", "Jelly", 12, true)
                .addRow("Bob", "Old Fashioned", 6, true)
                .addRow("Carol", "Jelly", 2, true)
                .addRow("Alice", "Apple Cider", 1, true)
            ,
            this.donutOrders.sortBy(Lists.immutable.of("Delivered")));
    }

    @Test
    public void select()
    {
        DataFrameUtil.assertEquals(
            new DataFrame("Expected")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Alice", "Jelly", 12, true)
                .addRow("Bob", "Old Fashioned", 6, true)
                .addRow("Carol", "Jelly", 2, true)
                .addRow("Alice", "Apple Cider", 1, true)
            ,
            this.donutOrders.selectBy("Delivered")
        );

        DataFrameUtil.assertEquals(
            new DataFrame("Expected")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Alice", "Apple Cider", 6, false)
                .addRow("Dave", "Apple Cider", 10, false)
                .addRow("Dave", "Old Fashioned", 10, false),
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
                .addRow("Alice", "Jelly", 12, true, true)
                .addRow("Bob", "Old Fashioned", 6, true, false)
                .addRow("Alice", "Apple Cider", 6, false, false)
                .addRow("Carol", "Jelly", 2, true, false)
                .addRow("Dave", "Apple Cider", 10, false, true)
                .addRow("Dave", "Old Fashioned", 10, false, true)
                .addRow("Alice", "Apple Cider", 1, true, false)
            ,
            this.donutOrders.addColumn("LargeOrder", "Quantity > 6")
        );
    }

    @Test
    public void handlingNulls()
    {
        DataFrame unclearOrders = new DataFrame("Donut Orders")
            .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
            .addRow("Alice", "Jelly", 12, true)
            .addRow("Bob", "Old Fashioned", 6, null)
            .addRow("Alice", "Apple Cider", 6, false)
            .addRow("Carol", "Jelly", 2, true)
            .addRow("Dave", "Apple Cider", 10, null)
            .addRow("Dave", "Old Fashioned", 10, false)
            .addRow("Alice", "Apple Cider", 1, null)
            ;

        assertFalse(unclearOrders.isNull("Delivered", 0));
        assertTrue(unclearOrders.isNull("Delivered", 1));
        assertFalse(unclearOrders.isNull("Delivered", 2));
        assertTrue(unclearOrders.isNull("Delivered", 6));

        Twin<DataFrame> unknownAndKnown = unclearOrders.partition("Delivered is null");

        DataFrameUtil.assertEquals(
            new DataFrame("Delivery status unknown")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Bob", "Old Fashioned", 6, null)
                .addRow("Dave", "Apple Cider", 10, null)
                .addRow("Alice", "Apple Cider", 1, null)
            ,
            unknownAndKnown.getOne()
        );

        DataFrameUtil.assertEquals(
            new DataFrame("Delivery status known")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Alice", "Jelly", 12, true)
                .addRow("Alice", "Apple Cider", 6, false)
                .addRow("Carol", "Jelly", 2, true)
                .addRow("Dave", "Old Fashioned", 10, false)
            ,
            unknownAndKnown.getTwo()
        );
    }

    @Test
    public void addValue()
    {
        DfBooleanColumn deliveredColumn = this.donutOrders.getBooleanColumn("Delivered");

        int initialSize = deliveredColumn.getSize();

        deliveredColumn.addValue(BooleanValue.TRUE);
        assertEquals(initialSize + 1, deliveredColumn.getSize());
        assertTrue(deliveredColumn.getBoolean(initialSize));

        deliveredColumn.addValue(BooleanValue.FALSE);
        assertEquals(initialSize + 2, deliveredColumn.getSize());
        assertFalse(deliveredColumn.getBoolean(initialSize + 1));

        deliveredColumn.addValue(Value.VOID);
        assertEquals(initialSize + 3, deliveredColumn.getSize());
        assertTrue(deliveredColumn.isNull(initialSize + 2));

        var strValue = new StringValue("Hello");
        Exception e = assertThrows(RuntimeException.class, () -> deliveredColumn.addValue(strValue));

        assertEquals(
                FormatWithPlaceholders.messageFromKey("DF_BAD_VAL_ADD_TO_COL")
                                      .with("valueType", strValue.getType()
                                                                 .toString())
                                      .with("columnName", "Delivered")
                                      .with("columnType", ValueType.BOOLEAN.toString())
                                      .with("value", strValue.asStringLiteral())
                                      .toString()
                ,
                e.getMessage());
    }

    @Test
    public void setObject()
    {
        DfBooleanColumn deliveredColumn = this.donutOrders.getBooleanColumn("Delivered");

        deliveredColumn.setObject(0, true);

        deliveredColumn.setObject(1, null);
        assertTrue(deliveredColumn.isNull(1));

        deliveredColumn.setObject(2, null);
        assertTrue(deliveredColumn.isNull(2));

        deliveredColumn.setObject(2, false);
        assertFalse(deliveredColumn.isNull(2));

        deliveredColumn.setObject(3, false);
        deliveredColumn.setObject(4, false);
        deliveredColumn.setObject(5, true);
        deliveredColumn.setObject(6, false);

        DataFrameUtil.assertEquals(
            this.donutOrders,
            new DataFrame("expected")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Alice", "Jelly", 12, true)
                .addRow("Bob", "Old Fashioned", 6, null)
                .addRow("Alice", "Apple Cider", 6, false)
                .addRow("Carol", "Jelly", 2, false)
                .addRow("Dave", "Apple Cider", 10, false)
                .addRow("Dave", "Old Fashioned", 10, true)
                .addRow("Alice", "Apple Cider", 1, false)
        );
    }

    @Test
    public void setBoolean()
    {
        DfBooleanColumnStored deliveredColumn = (DfBooleanColumnStored) this.donutOrders.getBooleanColumn("Delivered");

        deliveredColumn.setObject(1, null);
        assertTrue(deliveredColumn.isNull(1));

        deliveredColumn.setBoolean(1, true);
        assertTrue(deliveredColumn.getBoolean(1));
        assertFalse(deliveredColumn.isNull(1));
    }

    @Test
    public void populateByColumn()
    {
        DataFrame df = new DataFrame("Delivery Tracker")
                .addStringColumn("Order Number", Lists.immutable.of("1", "2", "3"))
                .addBooleanColumn("Delivered", BooleanLists.immutable.of(true, false, true))
                ;

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                    .addStringColumn("Order Number").addBooleanColumn("Delivered")
                    .addRow("1", true)
                    .addRow("2", false)
                    .addRow("3", true)
                ,
                df
        );
    }
}
