package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.util.ConfigureMessages;
import io.github.vmzakharov.ecdataframe.util.FormatWithPlaceholders;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DataFrameComputedColumnsFailureTest
{
    private DataFrame df;

    @BeforeClass
    static public void initializeMessages()
    {
        ConfigureMessages.initialize();
    }

    @Before
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
        try
        {
            this.df.addColumn("Wat", "Name + Count");
            Assert.fail("Should have thrown an exception");
        }
        catch (RuntimeException e)
        {
            Assert.assertEquals(
                    FormatWithPlaceholders.messageFromKey("DF_CALC_COL_INFER_TYPE")
                            .with("dataFrameName", "df1")
                            .with("columnName", "Wat")
                            .with("expression", "Name + Count")
                            .with("errorList", FormatWithPlaceholders.messageFromKey("TYPE_INFER_TYPES_IN_EXPRESSION") + ": (Name + Count)")
                            .toString()
                    ,
                    e.getMessage());
        }
    }

    @Test
    public void typeConflictExpressionVsColumn1()
    {
        try
        {
            this.df.addFloatColumn("Wat", "Name + 'abc'");
            Assert.fail("Should have thrown an exception");
        }
        catch (Exception e)
        {
            Assert.assertEquals(
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
    }

    @Test
    public void typeConflictExpressionVsColumn2()
    {
        try
        {
            this.df.addFloatColumn("Wat", "Floatie + 1.23");
            Assert.fail("Should have thrown an exception");
        }
        catch (Exception e)
        {
            Assert.assertEquals(
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
    }

    @Test
    public void undefinedReference()
    {
        try
        {
            this.df.addLongColumn("Wat", "Count + Chocola");
            Assert.fail("Should have thrown an exception");
        }
        catch (Exception e)
        {
            Assert.assertEquals(
                    FormatWithPlaceholders.messageFromKey("DF_CALC_COL_INFER_TYPE")
                                          .with("dataFrameName", "df1")
                                          .with("columnName", "Wat")
                                          .with("expression", "Count + Chocola")
                                          .with("errorList", FormatWithPlaceholders.messageFromKey("TYPE_INFER_UNDEFINED_VARIABLE") + ": Chocola")
                                          .toString()
                    ,
                    e.getMessage());
        }
    }
}
