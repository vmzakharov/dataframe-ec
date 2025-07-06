package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.FloatValue;
import io.github.vmzakharov.ecdataframe.util.ConfigureMessages;
import io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class DataFrameComputedColumnsFailureTest
{
    private DataFrame df;

    @BeforeAll
    static public void initializeMessages()
    {
        ConfigureMessages.initialize();
    }

    @BeforeEach
    public void initializeDataFrame()
    {
        this.df = new DataFrame("df1")
                .addStringColumn("Name")
                .addLongColumn("Count")
                .addDoubleColumn("Value")
                .addIntColumn("PetCount")
                .addFloatColumn("Floatie")
                .addRow("Alice", 5, 23.45, 2, 1.25f)
                .addRow("Bob", 10, 12.34, 1, 2.5f)
                .addRow("Carol", 11, 56.78, 0, 4.75f)
                .addRow("Dan", 0, 7.89, 6, 1.0f);
    }

    @Test
    public void incompatibleTypesInExpression()
    {
        Exception e = assertThrows(RuntimeException.class, () -> this.df.addColumn("Wat", "Name + Count"));

        assertEquals(
            FormatWithPlaceholders.messageFromKey("DF_CALC_COL_INFER_TYPE")
                    .with("dataFrameName", "df1")
                    .with("columnName", "Wat")
                    .with("expression", "Name + Count")
                    .with("errorList", FormatWithPlaceholders.messageFromKey("TYPE_INFER_TYPES_IN_EXPRESSION") + ": (Name + Count)")
                    .toString()
            ,
            e.getMessage());
    }

    @Test
    public void typeConflictExpressionVsColumn1()
    {
        Exception e = assertThrows(RuntimeException.class, () -> this.df.addFloatColumn("Wat", "Name + 'abc'"));
        assertEquals(
                FormatWithPlaceholders.messageFromKey("DF_CALC_COL_TYPE_MISMATCH")
                                      .with("dataFrameName", "df1")
                                      .with("columnName", "Wat")
                                      .with("expression", "Name + 'abc'")
                                      .with("inferredType", "STRING")
                                      .with("specifiedType", "FLOAT")
                                      .toString()
                ,
                e.getMessage());
    }

    @Test
    public void typeConflictExpressionVsColumn2()
    {
        Exception e = assertThrows(RuntimeException.class, () -> this.df.addFloatColumn("Wat", "Floatie + 1.23"));

        assertEquals(
                FormatWithPlaceholders.messageFromKey("DF_CALC_COL_TYPE_MISMATCH")
                                      .with("dataFrameName", "df1")
                                      .with("columnName", "Wat")
                                      .with("expression", "Floatie + 1.23")
                                      .with("inferredType", "DOUBLE")
                                      .with("specifiedType", "FLOAT")
                                      .toString()
                ,
                e.getMessage());
    }

    @Test
    public void undefinedReference()
    {
        Exception e = assertThrows(RuntimeException.class,
                () -> this.df.addLongColumn("Wat", "Count + Chocola"));
        assertEquals(
                FormatWithPlaceholders.messageFromKey("DF_CALC_COL_INFER_TYPE")
                                      .with("dataFrameName", "df1")
                                      .with("columnName", "Wat")
                                      .with("expression", "Count + Chocola")
                                      .with("errorList", FormatWithPlaceholders.messageFromKey("TYPE_INFER_UNDEFINED_VARIABLE") + ": Chocola")
                                      .toString()
                ,
                e.getMessage());
    }

    @Test void setObjectThrows()
    {
        this.df.addColumn("TwoFloaties", "Floatie * 2");

        Exception e = assertThrows(RuntimeException.class,
                () -> this.df.getColumnNamed("TwoFloaties").setObject(1, 1.25));

        assertEquals(
            FormatWithPlaceholders.messageFromKey("DF_SET_VAL_ON_COMP_COL").with("columnName", "TwoFloaties").toString(),
            e.getMessage());
    }

    @Test void addValueThrows()
    {
        this.df.addColumn("TwoFloaties", "Floatie * 2");

        Exception e = assertThrows(RuntimeException.class,
                () -> this.df.getColumnNamed("TwoFloaties").addValue(new FloatValue(10.25f))
        );

        assertEquals(
            FormatWithPlaceholders.messageFromKey("DF_CALC_COL_MODIFICATION").with("columnName", "TwoFloaties").toString(),
            e.getMessage());
    }

    @Test void addObjectThrowsUnlessNull()
    {
        this.df.addColumn("TwoFloaties", "Floatie * 2");

        assertEquals(4, this.df.getColumnNamed("TwoFloaties").getSize());
        this.df.getColumnNamed("TwoFloaties").addObject(null); // doesn't do anything
        assertEquals(4, this.df.getColumnNamed("TwoFloaties").getSize());

        Exception e = assertThrows(RuntimeException.class,
                () -> this.df.getColumnNamed("TwoFloaties").addObject(10.25f)
        );

        assertEquals(
            FormatWithPlaceholders.messageFromKey("DF_CALC_COL_MODIFICATION").with("columnName", "TwoFloaties").toString(),
            e.getMessage());
    }
}
