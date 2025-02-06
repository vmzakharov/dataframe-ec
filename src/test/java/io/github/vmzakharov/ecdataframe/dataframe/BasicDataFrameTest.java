package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.primitive.BooleanLists;
import org.eclipse.collections.impl.list.Interval;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

public class BasicDataFrameTest
{
    @Test
    public void createSimpleDataFrame()
    {
        DataFrame df = new DataFrame("df1");
        df.addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value");
        df
           .addRow("Alice", 5, 23.45)
           .addRow("Bob",  10, 12.34)
           .addRow("Carl", 11, 56.78)
           .addRow("Deb",   0,  7.89);
        
        assertEquals("df1", df.getName());

        assertEquals(3, df.columnCount());
        assertEquals(4, df.rowCount());

        assertEquals("Alice", df.getObject(0, 0));
        assertEquals(10L, df.getObject(1, 1));
        assertEquals(56.78, df.getObject(2, 2));

        assertEquals(11L, df.getObject("Count", 2));
        assertEquals("Alice", df.getObject("Name", 0));

        assertEquals(10L, df.getLong("Count", 1));
        assertEquals(56.78, df.getDouble("Value", 2), 0.0);

        assertEquals("Alice", df.getValueAsString(0, 0));
        assertEquals("\"Alice\"", df.getValueAsStringLiteral(0, 0));

        assertEquals("11", df.getValueAsString(2, 1));
        assertEquals("11", df.getValueAsStringLiteral(2, 1));
    }

    @Test
    public void addEmptyRow()
    {
        DataFrame df = new DataFrame("df1")
            .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
            .addRow("Alice", 5, 23.45)
            .addRow()
            .addRow("Bob",  10, 12.34)
            .addRow();

        assertEquals(Lists.immutable.of("Alice", null, "Bob", null), df.getStringColumn("Name").toList());
        
        assertEquals(5, df.getLong("Count", 0));
        assertEquals(10, df.getLong("Count", 2));
        assertEquals(23.45, df.getDouble("Value", 0));
        assertEquals(12.34, df.getDouble("Value", 2));

        this.assertNullValuesInColumn(df, "Count", false, true, false, true);
        this.assertNullValuesInColumn(df, "Value", false, true, false, true);
    }

    @Test
    public void renameDataFrame()
    {
        DataFrame df = new DataFrame("df1");
        assertEquals("df1", df.getName());
        df.setName("beep-boop");
        assertEquals("beep-boop", df.getName());
    }

    @Test
    public void addColumn()
    {
        DataFrame dataFrame = new DataFrame("df1")
                .addColumn("String", ValueType.STRING)
                .addColumn("Long", ValueType.LONG)
                .addColumn("Double", ValueType.DOUBLE)
                .addColumn("Date", ValueType.DATE)
                .addColumn("DateTime", ValueType.DATE_TIME)
                .addColumn("Decimal", ValueType.DECIMAL)
                .addColumn("StringComp", ValueType.STRING, "String + \"-meep\"")
                .addColumn("LongComp", ValueType.LONG, "Long * 2")
                .addColumn("DoubleComp", ValueType.DOUBLE, "Double + 10.0")
                .addColumn("DateComp", ValueType.DATE, "toDate(2021, 11, 15)")
                .addColumn("DateTimeComp", ValueType.DATE_TIME, "toDateTime(2022, 12, 25, 13, 12, 10)")
                .addColumn("DecimalComp", ValueType.DECIMAL, "toDecimal(456,5)")
                ;

        dataFrame.addRow("Beep", 10, 20.0, LocalDate.of(2020, 10, 20), LocalDateTime.of(2022, 8, 22, 10, 10, 10),
                BigDecimal.valueOf(123.456));

        assertEquals("Beep", dataFrame.getString("String", 0));
        assertEquals("Beep-meep", dataFrame.getString("StringComp", 0));

        assertEquals(10, dataFrame.getLong("Long", 0));
        assertEquals(20, dataFrame.getLong("LongComp", 0));

        assertEquals(20.0, dataFrame.getDouble("Double", 0), 0.000001);
        assertEquals(30.0, dataFrame.getDouble("DoubleComp", 0), 0.000001);

        assertEquals(LocalDate.of(2020, 10, 20), dataFrame.getDate("Date", 0));
        assertEquals(LocalDateTime.of(2022, 8, 22, 10, 10, 10), dataFrame.getDateTime("DateTime", 0));

        assertEquals(LocalDate.of(2021, 11, 15), dataFrame.getDate("DateComp", 0));
        assertEquals(LocalDateTime.of(2022, 12, 25, 13, 12, 10), dataFrame.getDateTime("DateTimeComp", 0));

        assertEquals(BigDecimal.valueOf(123.456), dataFrame.getDecimal("Decimal", 0));
        assertEquals(BigDecimal.valueOf(456, 5), dataFrame.getDecimal("DecimalComp", 0));
    }

    @Test
    public void newColumn()
    {
        MutableList<DfColumn> addedColumns = Lists.mutable.of();

        DataFrame dataFrame = new DataFrame("df1");

        addedColumns
            .with(dataFrame.newColumn("String", ValueType.STRING))
            .with(dataFrame.newColumn("Long", ValueType.LONG))
            .with(dataFrame.newColumn("Double", ValueType.DOUBLE))
            .with(dataFrame.newColumn("Date", ValueType.DATE))
            .with(dataFrame.newColumn("DateTime", ValueType.DATE_TIME))
            .with(dataFrame.newColumn("Decimal", ValueType.DECIMAL))
            .with(dataFrame.newColumn("StringComp", ValueType.STRING, "String + \"-meep\""))
            .with(dataFrame.newColumn("LongComp", ValueType.LONG, "Long * 2"))
            .with(dataFrame.newColumn("DoubleComp", ValueType.DOUBLE, "Double + 10.0"))
            .with(dataFrame.newColumn("DateComp", ValueType.DATE, "toDate(2021, 11, 15)"))
            .with(dataFrame.newColumn("DateTimeComp", ValueType.DATE_TIME, "toDateTime(2022, 12, 25, 13, 12, 10)"))
            .with(dataFrame.newColumn("DecimalComp", ValueType.DECIMAL, "toDecimal(456,5)"))
        ;

        dataFrame.addRow("Beep", 10, 20.0, LocalDate.of(2020, 10, 20), LocalDateTime.of(2022, 8, 22, 10, 10, 10),
                BigDecimal.valueOf(123.456));

        Lists.immutable.of("String", "Long", "Double", "Date", "DateTime", "Decimal",
                           "StringComp", "LongComp", "DoubleComp", "DateComp", "DateTimeComp", "DecimalComp")
           .forEachWithIndex(
                   (colName, index) -> assertSame(dataFrame.getColumnNamed(colName), addedColumns.get(index))
           );

        assertEquals("Beep", dataFrame.getString("String", 0));
        assertEquals("Beep-meep", dataFrame.getString("StringComp", 0));

        assertEquals(10, dataFrame.getLong("Long", 0));
        assertEquals(20, dataFrame.getLong("LongComp", 0));

        assertEquals(20.0, dataFrame.getDouble("Double", 0), 0.000001);
        assertEquals(30.0, dataFrame.getDouble("DoubleComp", 0), 0.000001);

        assertEquals(LocalDate.of(2020, 10, 20), dataFrame.getDate("Date", 0));
        assertEquals(LocalDateTime.of(2022, 8, 22, 10, 10, 10), dataFrame.getDateTime("DateTime", 0));

        assertEquals(LocalDate.of(2021, 11, 15), dataFrame.getDate("DateComp", 0));
        assertEquals(LocalDateTime.of(2022, 12, 25, 13, 12, 10), dataFrame.getDateTime("DateTimeComp", 0));

        assertEquals(BigDecimal.valueOf(123.456), dataFrame.getDecimal("Decimal", 0));
        assertEquals(BigDecimal.valueOf(456, 5), dataFrame.getDecimal("DecimalComp", 0));
    }

    @Test
    public void dropColumn()
    {
        DataFrame df = new DataFrame("df1")
            .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
            .addRow("Alice", 5, 23.45)
            .addRow("Deb",   0,  7.89);

        df.dropColumn("Count");

        DataFrame expected = new DataFrame("expected")
            .addStringColumn("Name").addDoubleColumn("Value")
            .addRow("Alice", 23.45)
            .addRow("Deb",    7.89);

        DataFrameUtil.assertEquals(expected, df);
    }

    @Test
    public void dropManyColumns()
    {
        DataFrame df = new DataFrame("df1")
            .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addStringColumn("Foo")
            .addRow("Alice", 5, 23.45, "abc")
            .addRow("Deb",   0,  7.89, "xyz");

        df.dropColumns(Lists.immutable.of("Value", "Count"));

        DataFrame expected = new DataFrame("expected")
            .addStringColumn("Name").addStringColumn("Foo")
            .addRow("Alice", "abc")
            .addRow("Deb",   "xyz");

        DataFrameUtil.assertEquals(expected, df);
    }

    @Test
    public void dataFrameValuesIsNull()
    {
        DataFrame df = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addDateColumn("Foo")
                .addRow("Deb",   0,  7.89, null)
                .addRow("Bob", null, 23.45, LocalDate.of(2020, 10, 20))
                .addRow(null, 5, 23.45, LocalDate.of(2020, 10, 20))
                .addRow("Carl", 5, null, LocalDate.of(2020, 10, 20))
                ;

        this.assertNullValuesInColumn(df, "Name", false, false, true, false);
        this.assertNullValuesInColumn(df, "Count", false, true, false, false);
        this.assertNullValuesInColumn(df, "Value", false, false, false, true);
        this.assertNullValuesInColumn(df, "Foo", true, false, false, false);

        df.sortBy(Lists.immutable.of("Name"));

        this.assertNullValuesInColumn(df, "Name", true, false, false, false);
        this.assertNullValuesInColumn(df, "Count", false, true, false, false);
        this.assertNullValuesInColumn(df, "Value", false, false, true, false);
        this.assertNullValuesInColumn(df, "Foo", false, false, false, true);
    }

    @Test
    public void isEmpty()
    {
        DataFrame df = new DataFrame("df1");

        assertTrue(df.isEmpty());

        df.addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addDateColumn("Foo")
                .addRow("Deb",   0,  7.89, null)
                .addRow("Bob", null, 23.45, LocalDate.of(2020, 10, 20))
                .addRow(null, 5, 23.45, LocalDate.of(2020, 10, 20))
                .addRow("Carl", 5, null, LocalDate.of(2020, 10, 20))
        ;

        assertFalse(df.isEmpty());
    }

    @Test
    public void isNotEmpty()
    {
        DataFrame df = new DataFrame("df1");

        assertFalse(df.isNotEmpty());

        df.addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addDateColumn("Foo")
                .addRow("Deb",   0,  7.89, null)
                .addRow("Bob", null, 23.45, LocalDate.of(2020, 10, 20))
                .addRow(null, 5, 23.45, LocalDate.of(2020, 10, 20))
                .addRow("Carl", 5, null, LocalDate.of(2020, 10, 20))
        ;

        assertTrue(df.isNotEmpty());
    }

    private void assertNullValuesInColumn(DataFrame df, String columnName, boolean... expectedValues)
    {
        assertEquals(
                BooleanLists.immutable.of(expectedValues),
                Interval.zeroTo(df.rowCount() - 1).collectBoolean(e -> df.isNull(columnName, e)).toList(),
                columnName
        );
    }

    @Test
    public void keepColumns()
    {
        DataFrame df = new DataFrame("df1")
            .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value").addStringColumn("Foo")
            .addRow("Alice", 5, 23.45, "abc")
            .addRow("Deb",   0,  7.89, "xyz");

        DataFrame result = df.keepColumns(Lists.immutable.of("Name", "Foo"));

        DataFrame expected = new DataFrame("expected")
            .addStringColumn("Name").addStringColumn("Foo")
            .addRow("Alice", "abc")
            .addRow("Deb",   "xyz");

        DataFrameUtil.assertEquals(expected, df);
        assertSame(result, df);
    }

    @Test
    public void dropFirstAndLastColumn()
    {
        DataFrame df = new DataFrame("df1")
            .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
            .addRow("Alice", 5, 23.45)
            .addRow("Deb",   0,  7.89);

        df.dropColumn("Name").dropColumn("Value");
        assertFalse(df.hasColumn("Name"));
        assertFalse(df.hasColumn("Value"));

        DataFrame expected = new DataFrame("expected")
            .addLongColumn("Count")
            .addRow(5)
            .addRow(0);

        DataFrameUtil.assertEquals(expected, df);
    }

    @Test
    public void dropNonExistingColumn()
    {
        DataFrame df = new DataFrame("df1")
            .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
            .addRow("Alice", 5, 23.45)
            .addRow("Deb",   0,  7.89);

        assertThrows(RuntimeException.class, () -> df.dropColumn("Giraffe"));
    }

    @Test
    public void dropNonExistingColumn2()
    {
        DataFrame df = new DataFrame("df1")
            .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
            .addRow("Alice", 5, 23.45)
            .addRow("Deb",   0,  7.89);

        assertThrows(RuntimeException.class, () -> df.dropColumns(Lists.immutable.of("Name", "Giraffe")));
    }

    @Test
    public void keepNonExistingColumns()
    {
        DataFrame df = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Alice", 5, 23.45)
                .addRow("Deb",   0,  7.89);

        assertThrows(RuntimeException.class, () -> df.keepColumns(Lists.immutable.of("Name", "Giraffe")));
    }

    @Test
    public void escapingColumnNames()
    {
        DataFrame df = new DataFrame("df1")
                .addStringColumn("Person's Name").addLongColumn("Pet Count").addStringColumn("number-as-string").addStringColumn("Three letters")
                .addRow("Alice", 5, "1", "abc")
                .addRow("Deb",   1, "2", "xyz");

        df.addStringColumn("First Letter plus", "substr(${Person's Name}, 0, 1) + ' ' + ${Three letters}");
        df.addLongColumn("Math, and stuff", "${Pet Count} * toLong(${number-as-string})");

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Person's Name").addLongColumn("Pet Count").addStringColumn("number-as-string")
                        .addStringColumn("Three letters").addStringColumn("First Letter plus").addLongColumn("Math, and stuff")
                        .addRow("Alice", 5, "1", "abc", "A abc", 5)
                        .addRow("Deb",   1, "2", "xyz", "D xyz", 2)
                , df);

    }

    @Test
    public void sealingMisshapenDataFrame()
    {
        DataFrame df = new DataFrame("df1")
                .addStringColumn("Foo").addStringColumn("Bar");

        DfStringColumn fooColumn = df.getStringColumn("Foo");
        fooColumn.addObject("A");
        fooColumn.addObject("B");

        DfStringColumn barColumn = df.getStringColumn("Bar");
        barColumn.addObject("X");

        assertThrows(RuntimeException.class, df::seal);
    }

    @Test
    public void dataFrameRowsDifSize()
    {
        assertThrows(
                RuntimeException.class,
                () -> new DataFrame("df1")
                    .addStringColumn("Foo", Lists.immutable.of("A", "B", "C"))
                    .addStringColumn("Bar", Lists.immutable.of("X", "Y")));
    }

    @Test
    public void columnSchema()
    {
        DataFrame df = new DataFrame("df1")
                .addStringColumn("Name").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("Alice", 5, 23.45)
                .addRow("Bob",  10, 12.34);

        df.addColumn("Two Values", "Value * 2");

        DataFrameUtil.assertEquals(
                new DataFrame("Expected Schema")
                        .addStringColumn("Name").addStringColumn("Type").addStringColumn("Stored").addStringColumn("Expression")
                        .addRow("Name", "STRING", "Y", "")
                        .addRow("Count", "LONG", "Y", "")
                        .addRow("Value", "DOUBLE", "Y", "")
                        .addRow("Two Values", "DOUBLE", "N", "Value * 2"),
                df.schema()
        );
    }
}
