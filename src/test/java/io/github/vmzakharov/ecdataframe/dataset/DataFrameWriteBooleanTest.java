package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import org.junit.jupiter.api.Test;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DataFrameWriteBooleanTest
{
    @Test
    public void writeNoSchema()
    {
        DataFrame donutOrders = new DataFrame("orders")
            .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
            .addRow("Alice", "Jelly", 12, true)
            .addRow("Alice", "Apple Cider", 6, false)
            .addRow("Carol", "Jelly", 2, true)
            .addRow("Dave", "Old Fashioned", 10, false);

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", "");
        dataSet.write(donutOrders);

        String expected = """
                Client,Donut,Quantity,Delivered
                "Alice","Jelly",12,true
                "Alice","Apple Cider",6,false
                "Carol","Jelly",2,true
                "Dave","Old Fashioned",10,false
                """;

        assertEquals(expected, dataSet.getWrittenData());
    }

    @Test
    public void writeNoSchemaWithNulls()
    {
        DataFrame donutOrders = new DataFrame("orders")
            .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
            .addRow("Alice", "Jelly", 12, true)
            .addRow("Bob", "Old Fashioned", 6, null)
            .addRow("Alice", "Apple Cider", 6, false)
            .addRow("Carol", "Jelly", 2, true)
            .addRow("Dave", "Apple Cider", 10, null)
            .addRow("Dave", "Old Fashioned", 10, false)
            .addRow("Alice", "Apple Cider", 1, null);

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", "");

        dataSet.write(donutOrders);

        String expected = """
                Client,Donut,Quantity,Delivered
                "Alice","Jelly",12,true
                "Bob","Old Fashioned",6,
                "Alice","Apple Cider",6,false
                "Carol","Jelly",2,true
                "Dave","Apple Cider",10,
                "Dave","Old Fashioned",10,false
                "Alice","Apple Cider",1,
                """;

        assertEquals(expected, dataSet.getWrittenData());
    }

    @Test
    public void writeWithSchema()
    {
        DataFrame donutOrders = new DataFrame("orders")
            .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
            .addRow("Alice", "Jelly", null, true)
            .addRow("Bob", "Old Fashioned", 6, null)
            .addRow("Alice", "Apple Cider", 6, false)
            .addRow("Carol", "Jelly", 2, true)
            .addRow("Dave", "Apple Cider", 10, null)
            .addRow("Dave", "Old Fashioned", 10, false)
            .addRow("Alice", "Apple Cider", 1, null);

        donutOrders.addColumn("NotDelivered", "not Delivered");

        CsvSchema schema = new CsvSchema()
            .addColumn("Client", STRING)
            .addColumn("Donut", STRING)
            .addColumn("Quantity", INT)
            .addColumn("Delivered", BOOLEAN)
            .addColumn("NotDelivered", BOOLEAN, "t,f")
            .nullMarker("--NULL--")
            ;

        StringBasedCsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "Employees", schema, "");

        dataSet.write(donutOrders);

        String expected = """
            Client,Donut,Quantity,Delivered,NotDelivered
            "Alice","Jelly",--NULL--,true,f
            "Bob","Old Fashioned",6,--NULL--,--NULL--
            "Alice","Apple Cider",6,false,t
            "Carol","Jelly",2,true,f
            "Dave","Apple Cider",10,--NULL--,--NULL--
            "Dave","Old Fashioned",10,false,t
            "Alice","Apple Cider",1,--NULL--,--NULL--
            """;

        assertEquals(expected, dataSet.getWrittenData());
    }
}
