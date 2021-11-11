package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.tuple.Triplet;
import org.junit.Ignore;
import org.junit.Test;

public class IntersectionAndDifferencesTest
{
    @Ignore
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

        DataFrame aComplementOfB = result.getOne();
        DataFrameUtil.assertEquals(
            new DataFrame("Complement of B in A")
            .addStringColumn("Key").addLongColumn("Value")
            .addRow("A", 1)
            ,
            aComplementOfB
        );

        DataFrame intersection = result.getTwo();
        DataFrameUtil.assertEquals(
            new DataFrame("Join of A and B - intersection of keys")
            .addStringColumn("Key").addLongColumn("Value").addLongColumn("Count")
            .addRow("B", 1, 10)
            ,
            intersection
        );

        DataFrame bComplementOfA = result.getThree();
        DataFrameUtil.assertEquals(
            new DataFrame("Complement of A in B")
            .addStringColumn("Id").addLongColumn("Count")
            .addRow("C", 20)
            ,
            bComplementOfA
        );
    }
}
