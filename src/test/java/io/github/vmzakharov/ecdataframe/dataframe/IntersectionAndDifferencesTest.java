package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.tuple.Triplet;
import org.junit.Test;

public class IntersectionAndDifferencesTest
{
    @Test
    public void simpleTest()
    {
        DataFrame sideA = new DataFrame("Side A")
                .addStringColumn("Key").addLongColumn("Value")
                .addRow("A", 1)
                .addRow("B", 2)
                ;

        DataFrame sideB = new DataFrame("Side B")
                .addStringColumn("Id").addLongColumn("Count")
                .addRow("B", 10)
                .addRow("C", 20)
                ;

        Triplet<DataFrame> result = sideA.joinWithComplements(sideB, Lists.immutable.of("Key"), Lists.immutable.of("Id"));

        DataFrameUtil.assertEquals(
            new DataFrame("Complement of B in A")
            .addStringColumn("Key").addLongColumn("Value")
            .addRow("A", 1)
            ,
            result.getOne()
        );

        DataFrameUtil.assertEquals(
            new DataFrame("Join of A and B - intersection of keys")
            .addStringColumn("Key").addLongColumn("Value").addLongColumn("Count")
            .addRow("B", 2, 10)
            ,
            result.getTwo()
        );

        DataFrameUtil.assertEquals(
            new DataFrame("Complement of A in B")
            .addStringColumn("Id").addLongColumn("Count")
            .addRow("C", 20)
            ,
            result.getThree()
        );
    }

    @Test
    public void complementsAreEmpty()
    {
        DataFrame sideA = new DataFrame("Side A")
                .addLongColumn("Value").addStringColumn("Key")
                .addRow(2, "B")
                .addRow(1, "A")
                .addRow(3, "C")
                ;

        DataFrame sideB = new DataFrame("Side B")
                .addStringColumn("Id").addLongColumn("Number").addStringColumn("Text")
                .addRow("C", 30, "xyz")
                .addRow("A", 10, "abc")
                .addRow("B", 20, "def")
                ;

        Triplet<DataFrame> result = sideA.joinWithComplements(sideB, Lists.immutable.of("Key"), Lists.immutable.of("Id"));

        DataFrameUtil.assertEquals(
            new DataFrame("Complement of B in A").addLongColumn("Value").addStringColumn("Key"),
            result.getOne()
        );

        DataFrameUtil.assertEquals(
            new DataFrame("Join of A and B - intersection of keys")
            .addLongColumn("Value").addStringColumn("Key").addLongColumn("Number").addStringColumn("Text")
            .addRow(1, "A", 10, "abc")
            .addRow(2, "B", 20, "def")
            .addRow(3, "C", 30, "xyz")
            ,
            result.getTwo()
        );

        DataFrameUtil.assertEquals(
            new DataFrame("Complement of A in B").addStringColumn("Id").addLongColumn("Number").addStringColumn("Text"),
            result.getThree()
        );
    }

    @Test
    public void calculatedColumns()
    {
        DataFrame sideA = new DataFrame("Side A")
                .addStringColumn("Key").addLongColumn("Value")
                .addRow("A", 1)
                .addRow("B", 2)
                .addRow("C", 3)
                ;

        sideA.addStringColumn("ColumnX", "Key + 'x'");

        DataFrame sideB = new DataFrame("Side B")
                .addStringColumn("Id").addLongColumn("Count")
                .addRow("Bx", 10)
                .addRow("Cx", 20)
                .addRow("Dx", 30)
                ;

        sideB.addLongColumn("DoubleCount", "Count * 2");

        Triplet<DataFrame> result = sideA.joinWithComplements(sideB, Lists.immutable.of("ColumnX"), Lists.immutable.of("Id"));

        DataFrameUtil.assertEquals(
                new DataFrame("Complement of B in A")
                        .addStringColumn("Key").addLongColumn("Value").addStringColumn("ColumnX")
                        .addRow("A", 1, "Ax")
                ,
                result.getOne()
        );

        DataFrameUtil.assertEquals(
                new DataFrame("Join of A and B - intersection of keys")
                        .addStringColumn("Key").addLongColumn("Value").addStringColumn("ColumnX").addLongColumn("Count").addLongColumn("DoubleCount")
                        .addRow("B", 2, "Bx", 10, 20)
                        .addRow("C", 3, "Cx", 20, 40)
                ,
                result.getTwo()
        );

        DataFrameUtil.assertEquals(
                new DataFrame("Complement of A in B")
                        .addStringColumn("Id").addLongColumn("Count").addLongColumn("DoubleCount")
                        .addRow("Dx", 30, 60)
                ,
                result.getThree()
        );
    }

    @Test
    public void withAdditionalSortOrder()
    {
        DataFrame sideA = new DataFrame("Side A")
                .addStringColumn("Key").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("B", 4, 11.0)
                .addRow("B", 3, 12.0)
                .addRow("A", 2, 13.0)
                .addRow("A", 1, 14.0)
                .addRow("C", 3, 15.0)
                .addRow("C", 5, 16.0)
                ;

        DataFrame sideB = new DataFrame("Side B")
                .addStringColumn("Id").addLongColumn("Number").addDoubleColumn("Price")
                .addRow("B",  5, 3.03)
                .addRow("A",  2, 1.01)
                .addRow("B",  3, 2.02)
                .addRow("D", 30, 6.06)
                .addRow("C", 20, 4.04)
                .addRow("D", 20, 5.05)
                ;

        Triplet<DataFrame> result = sideA.joinWithComplements(
                sideB,
                Lists.immutable.of("Key"), Lists.immutable.of("Count"),
                Lists.immutable.of("Id"), Lists.immutable.of("Number"));

        DataFrameUtil.assertEquals(
                new DataFrame("Complement of B in A")
                        .addStringColumn("Key").addLongColumn("Count").addDoubleColumn("Value")
                        .addRow("A", 2, 13.0)
                        .addRow("C", 5, 16.0)
                ,
                result.getOne()
        );

        DataFrameUtil.assertEquals(
                new DataFrame("Join of A and B - intersection of keys")
                        .addStringColumn("Key").addLongColumn("Count").addDoubleColumn("Value").addLongColumn("Number").addDoubleColumn("Price")
                        .addRow("A", 1, 14.0,  2, 1.01)
                        .addRow("B", 3, 12.0,  3, 2.02)
                        .addRow("B", 4, 11.0,  5, 3.03)
                        .addRow("C", 3, 15.0, 20, 4.04)
                ,
                result.getTwo()
        );

        DataFrameUtil.assertEquals(
                new DataFrame("Complement of A in B")
                        .addStringColumn("Id").addLongColumn("Number").addDoubleColumn("Price")
                        .addRow("D", 20, 5.05)
                        .addRow("D", 30, 6.06)
                ,
                result.getThree()
        );
    }

    @Test
    public void withAdditionalSortOrderAndNullValues()
    {
        DataFrame sideA = new DataFrame("Side A")
                .addStringColumn("Key").addLongColumn("Count").addDoubleColumn("Value")
                .addRow("B", 4, 11.0)
                .addRow("B", 3, 12.0)
                .addRow("B", 3, 11.0)
                .addRow("B", 3, null)
                .addRow("A", null, 13.0)
                .addRow("A", 1, 14.0)
                .addRow("C", 3, 15.0)
                .addRow("C", null, 16.0)
                ;

        DataFrame sideB = new DataFrame("Side B")
                .addStringColumn("Id").addLongColumn("Number").addDoubleColumn("Price")
                .addRow("B",  4, 3.03)
                .addRow("A",  null, 1.01)
                .addRow("B",  3, null)
                .addRow("B",  3, 1.02)
                .addRow("B",  3, 2.02)
                .addRow("D", 30, 6.06)
                .addRow("C",  3, 4.04)
                .addRow("D", 20, 5.05)
                ;

        Triplet<DataFrame> result = sideA.joinWithComplements(
                sideB,
                Lists.immutable.of("Key", "Count"), Lists.immutable.of("Value"),
                Lists.immutable.of("Id", "Number"), Lists.immutable.of("Price"));

        DataFrameUtil.assertEquals(
                new DataFrame("Complement of B in A")
                        .addStringColumn("Key").addLongColumn("Count").addDoubleColumn("Value")
                        .addRow("A", 1, 14.0)
                        .addRow("C", null, 16.0)
                ,
                result.getOne()
        );

        DataFrameUtil.assertEquals(
                new DataFrame("Join of A and B - intersection of keys")
                        .addStringColumn("Key").addLongColumn("Count").addDoubleColumn("Value").addDoubleColumn("Price")
                        .addRow("A", null, 13.0, 1.01)
                        .addRow("B", 3, null, null)
                        .addRow("B", 3, 11.0, 1.02)
                        .addRow("B", 3, 12.0, 2.02)
                        .addRow("B", 4, 11.0, 3.03)
                        .addRow("C", 3, 15.0, 4.04)
                ,
                result.getTwo()
        );

        DataFrameUtil.assertEquals(
                new DataFrame("Complement of A in B")
                        .addStringColumn("Id").addLongColumn("Number").addDoubleColumn("Price")
                        .addRow("D", 20, 5.05)
                        .addRow("D", 30, 6.06)
                ,
                result.getThree()
        );
    }
}
