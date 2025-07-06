package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ImmutableList;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

public class DataFrameCopyTest
{
    @Test
    public void copySchema()
    {
        DataFrame df = new DataFrame("df1");
        df
                .addStringColumn("Name")
                .addLongColumn("Count")
                .addIntColumn("IntCount")
                .addDoubleColumn("Value");
        df
                .addRow("Alice", 5, 5, 23.45)
                .addRow("Bob",  10, 10, 12.34)
                .addRow("Carl", 11, 11, 56.78)
                .addRow("Deb",   0, 0, 7.89);

        df.addDoubleColumn("Twice", "Value * 2");

        DataFrame clonedSchema = df.cloneStructure("Cloned");

        assertEquals(0, clonedSchema.rowCount());
        assertEquals(df.columnCount(), clonedSchema.columnCount());
        for (int i = 0; i < df.columnCount(); i++)
        {
            DfColumn sourceColumn = df.getColumnAt(i);
            DfColumn copyColumn = clonedSchema.getColumnAt(i);

            assertEquals(sourceColumn.getName(), copyColumn.getName());
            if (sourceColumn.isComputed())
            {
                assertTrue(copyColumn.isComputed());
                assertEquals(
                        ((DfColumnComputed) sourceColumn).getExpressionAsString(),
                        ((DfColumnComputed) copyColumn).getExpressionAsString());
            }
            assertSame(clonedSchema, copyColumn.getDataFrame());
        }
    }

    @Test
    public void copySchemaConvertComputedToStored()
    {
        DataFrame df = new DataFrame("df1");
        df
                .addStringColumn("Name")
                .addLongColumn("Count")
                .addIntColumn("IntCount")
                .addDoubleColumn("Value");
        df
                .addRow("Alice", 5, 5, 23.45)
                .addRow("Bob",  10, 10, 12.34)
                .addRow("Carl", 11, 11, 56.78)
                .addRow("Deb",   0, 0, 7.89);

        df.addDoubleColumn("Twice", "Value * 2");

        DataFrame clonedSchema = df.cloneStructureAsStored("Cloned");

        assertEquals(0, clonedSchema.rowCount());
        assertEquals(df.columnCount(), clonedSchema.columnCount());
        for (int i = 0; i < df.columnCount(); i++)
        {
            DfColumn sourceColumn = df.getColumnAt(i);
            DfColumn copyColumn = clonedSchema.getColumnAt(i);

            assertEquals(sourceColumn.getName(), copyColumn.getName());
            assertEquals(sourceColumn.getType(), copyColumn.getType());
            assertTrue(copyColumn.isStored());
            assertSame(clonedSchema, copyColumn.getDataFrame());
        }
    }

    @Test
    public void copyWholeDataFrame()
    {
        DataFrame df = new DataFrame("df1");
        df
                .addStringColumn("Name")
                .addLongColumn("Count")
                .addIntColumn("IntCount")
                .addDoubleColumn("Value");
        df
                .addRow("Alice", 5, 5, 23.45)
                .addRow("Bob",  10, 10, 12.34)
                .addRow("Carl", 11, 11, 56.78)
                .addRow("Deb",   0, 0, 7.89);

        df.addDoubleColumn("Twice", "Value * 2");

        DataFrame copiedDataFrame = df.copy("MyNewCopy");

        DataFrameUtil.assertEquals(df, copiedDataFrame);

    }

    @Test
    public void copySomeColumnsToANewDataFrame()
    {
        DataFrame originalDataFrame = new DataFrame("df1");
        originalDataFrame
                .addStringColumn("Name")
                .addLongColumn("Count")
                .addIntColumn("IntCount")
                .addDoubleColumn("Value");

        originalDataFrame
                .addRow("Alice", 5, 5, 23.45)
                .addRow("Bob",  10, 10, 12.34)
                .addRow("Carl", 11, 11, 56.78)
                .addRow("Deb",   0, 0, 7.89);

        originalDataFrame.addDoubleColumn("Twice", "Value * 2");

        DataFrame expectedDataFrame = new DataFrame("df2");
        expectedDataFrame
                .addStringColumn("Name")
                .addDoubleColumn("Twice");

        expectedDataFrame
                .addRow("Alice",  46.9)
                .addRow("Bob",    24.68)
                .addRow("Carl",  113.56)
                .addRow("Deb",    15.78);

        ImmutableList<String> columnNamesToCopy = Lists.immutable.of("Name", "Twice");
        DataFrame copiedDataFrame = originalDataFrame.copy("MyNewCopy", columnNamesToCopy);

        DataFrameUtil.assertEquals(expectedDataFrame, copiedDataFrame);
    }

    @Test
    public void copyAllTypes()
    {
        DataFrame original = new DataFrame("df1")
                .addStringColumn("string1")
                .addLongColumn("long1").addIntColumn("int1")
                .addDoubleColumn("double1").addFloatColumn("float1")
                .addBooleanColumn("boolean1")
                .addDateColumn("date1").addDateTimeColumn("dateTime1")
                .addDecimalColumn("decimal1")
                .addRow("a",   10L, null, 13.25, 41.0f,  null,  LocalDate.of(2020, 10, 20),                                   null, BigDecimal.valueOf(123, 2))
                .addRow("b",  null,   22, 23.25,  null, false,                        null, LocalDateTime.of(2020, 10, 20, 16, 32), BigDecimal.valueOf(456, 2))
                .addRow(null,  30L,   23,  null, 43.0f, false,  LocalDate.of(2020, 10, 24), LocalDateTime.of(2020, 11, 20, 17, 33),                       null)
                .addRow("d",   40L,   24, 43.25, 44.5f,  true,  LocalDate.of(2020, 10, 26), LocalDateTime.of(2020, 12, 20, 18, 34), BigDecimal.valueOf(901, 2))
                ;

        DataFrame copy = original.copy("MyNewCopy");

        assertNotSame(original, copy);

        DataFrameUtil.assertEquals(original, copy);
    }
}
