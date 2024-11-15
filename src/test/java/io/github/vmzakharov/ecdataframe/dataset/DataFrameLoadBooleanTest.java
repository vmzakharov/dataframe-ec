package io.github.vmzakharov.ecdataframe.dataset;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrameUtil;
import org.junit.jupiter.api.Test;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DataFrameLoadBooleanTest
{
    @Test
    public void loadData()
    {
        CsvSchema schema = new CsvSchema()
            .addColumn("Client", STRING)
            .addColumn("Donut", STRING)
            .addColumn("Quantity", INT)
            .addColumn("Delivered", BOOLEAN)
            ;

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "DonutOrders", schema,
            """
            Client,Donut,Quantity,Delivered
            "Alice","Jelly",12,true
            "Alice","Apple Cider",6,false
            "Carol","Jelly",2,true
            "Dave","Old Fashioned",10,false"""
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrameUtil.assertEquals(
            new DataFrame("expected")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Alice", "Jelly",         12,  true)
                .addRow("Alice", "Apple Cider",    6, false)
                .addRow("Carol", "Jelly",          2,  true)
                .addRow("Dave",  "Old Fashioned", 10, false)
            ,
            loaded
        );
    }

    @Test
    public void loadDataWithFormat()
    {
        CsvSchema schema = new CsvSchema()
            .addColumn("Client", STRING)
            .addColumn("Donut", STRING)
            .addColumn("Quantity", INT)
            .addColumn("Delivered", BOOLEAN, ".T.,.F.");

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "DonutOrders", schema,
                """
                Client,Donut,Quantity,Delivered
                "Alice","Jelly",12,.T.
                "Alice","Apple Cider",6,.F.
                "Carol","Jelly",2,.T.
                "Dave","Old Fashioned",10,.F."""
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrameUtil.assertEquals(
            new DataFrame("expected")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Alice", "Jelly", 12, true)
                .addRow("Alice", "Apple Cider", 6, false)
                .addRow("Carol", "Jelly", 2, true)
                .addRow("Dave", "Old Fashioned", 10, false)
            ,
            loaded
        );
    }

    @Test
    public void loadDataTreatEmptyElementsAsValues()
    {
        CsvSchema schema = new CsvSchema()
                .addColumn("Client", STRING)
                .addColumn("Donut", STRING)
                .addColumn("Quantity", INT)
                .addColumn("Delivered", BOOLEAN)
                ;

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "DonutOrders", schema,
                """
                Client,Donut,Quantity,Delivered
                "Alice","Jelly",,true
                "Bob","Old Fashioned",6,
                "Alice","Apple Cider",6,false
                "Carol","Jelly",2,true
                "Dave","Apple Cider",10,
                "Dave","Old Fashioned",10,false
                "Alice","Apple Cider",1,"""
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrameUtil.assertEquals(
            new DataFrame("expected")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Alice", "Jelly",          0,  true)
                .addRow("Bob",   "Old Fashioned",  6, false)
                .addRow("Alice", "Apple Cider",    6, false)
                .addRow("Carol", "Jelly",          2,  true)
                .addRow("Dave",  "Apple Cider",   10, false)
                .addRow("Dave",  "Old Fashioned", 10, false)
                .addRow("Alice", "Apple Cider",    1, false)
            ,
            loaded
        );
    }

    @Test
    public void loadDataTreatEmptyElementsAsNulls()
    {
        CsvSchema schema = new CsvSchema()
                .addColumn("Client", STRING)
                .addColumn("Donut", STRING)
                .addColumn("Quantity", INT)
                .addColumn("Delivered", BOOLEAN)
                ;

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "DonutOrders", schema,
                """
                Client,Donut,Quantity,Delivered
                "Alice","Jelly",,true
                "Bob","Old Fashioned",6,
                "Alice","Apple Cider",6,false
                "Carol","Jelly",2,true
                "Dave","Apple Cider",10,
                "Dave","Old Fashioned",10,false
                "Alice","Apple Cider",1,"""
        );

        dataSet.convertEmptyElementsToNulls();

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrameUtil.assertEquals(
            new DataFrame("expected")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Alice", "Jelly",       null,  true)
                .addRow("Bob",   "Old Fashioned",  6,  null)
                .addRow("Alice", "Apple Cider",    6, false)
                .addRow("Carol", "Jelly",          2,  true)
                .addRow("Dave",  "Apple Cider",   10,  null)
                .addRow("Dave",  "Old Fashioned", 10, false)
                .addRow("Alice", "Apple Cider",    1,  null)
            ,
            loaded
        );
    }

    @Test
    public void loadDataWithMarkedNulls()
    {
        CsvSchema schema = new CsvSchema()
                .addColumn("Client", STRING)
                .addColumn("Donut", STRING)
                .addColumn("Quantity", INT)
                .addColumn("Delivered", BOOLEAN)
                .nullMarker("null")
                ;

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "DonutOrders", schema,
                """
                Client,Donut,Quantity,Delivered
                "Alice","Jelly",12,true
                "Bob","Old Fashioned",6,null
                "Alice","Apple Cider",6,false
                "Carol","Jelly",2,true
                "Dave","Apple Cider",10,null
                "Dave","Old Fashioned",10,false
                "Alice","Apple Cider",1,null"""
        );

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrameUtil.assertEquals(
            new DataFrame("expected")
                .addStringColumn("Client").addStringColumn("Donut").addIntColumn("Quantity").addBooleanColumn("Delivered")
                .addRow("Alice", "Jelly",         12,  true)
                .addRow("Bob",   "Old Fashioned",  6,  null)
                .addRow("Alice", "Apple Cider",    6, false)
                .addRow("Carol", "Jelly",          2,  true)
                .addRow("Dave",  "Apple Cider",   10,  null)
                .addRow("Dave",  "Old Fashioned", 10, false)
                .addRow("Alice", "Apple Cider",    1,  null)
            ,
            loaded
        );
    }

    @Test
    public void invalidFormat()
    {
        Exception e = assertThrows(
                RuntimeException.class,
                () -> new CsvSchema()
                            .addColumn("Client", STRING)
                            .addColumn("Donut", STRING)
                            .addColumn("Quantity", INT)
                            .addColumn("Delivered", BOOLEAN, "Hello")
        );

        assertEquals("Invalid format string 'Hello' for type 'boolean'", e.getMessage());
    }

    @Test
    public void invalidTrueFalseValues()
    {
        CsvSchema schema = new CsvSchema()
                .addColumn("Client", STRING)
                .addColumn("Donut", STRING)
                .addColumn("Quantity", INT)
                .addColumn("Delivered", BOOLEAN)
                ;

        CsvDataSet dataSet = new StringBasedCsvDataSet("Foo", "DonutOrders", schema,
                """
                Client,Donut,Quantity,Delivered
                "Alice","Jelly",12,cabbage
                "Alice","Apple Cider",6,false
                "Carol","Jelly",2,true
                "Dave","Old Fashioned",10,false"""
        );

        Exception e = assertThrows(RuntimeException.class, dataSet::loadAsDataFrame);

        assertEquals("Failed to parse input string to boolean: 'cabbage'", e.getMessage());
    }
}
