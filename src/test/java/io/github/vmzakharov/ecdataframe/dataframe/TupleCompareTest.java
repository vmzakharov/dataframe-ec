package io.github.vmzakharov.ecdataframe.dataframe;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TupleCompareTest
{
    @Test
    public void compare()
    {
        DfTuple t1 = new DfTuple(0, "B", null);
        DfTuple t2 = new DfTuple(0, null, null);
        DfTuple t3 = new DfTuple(0, "A", null);
        DfTuple t4 = new DfTuple(0, null, 2L);
        DfTuple t5 = new DfTuple(0, "A", 2L);
        DfTuple t6 = new DfTuple(0, null, null);

        Assert.assertTrue(t1.compareTo(t2) > 0);
        Assert.assertTrue(t2.compareTo(t1) < 0);
        Assert.assertNotEquals(t1, t2);

        Assert.assertTrue(t5.compareTo(t3) > 0);
        Assert.assertTrue(t3.compareTo(t5) < 0);
        Assert.assertNotEquals(t5, t3);

        Assert.assertTrue(t5.compareTo(t4) > 0);
        Assert.assertTrue(t4.compareTo(t5) < 0);
        Assert.assertNotEquals(t5, t3);

        Assert.assertEquals(0, t2.compareTo(t6));
        Assert.assertEquals(t2, t6);

        Assert.assertEquals(0, t4.compareTo(t4));
        Assert.assertEquals(0, t5.compareTo(t5));
        Assert.assertEquals(0, t6.compareTo(t6));
    }

    @Test
    public void sort()
    {
        DfTuple t2 = new DfTuple(0, null, null);
        DfTuple t6 = new DfTuple(0, null, null);
        DfTuple t4 = new DfTuple(0, null, 2L);
        DfTuple t3 = new DfTuple(0, "A", null);
        DfTuple t5 = new DfTuple(0, "A", 2L);
        DfTuple t1 = new DfTuple(0, "B", null);

        DfTuple[] tuples = new DfTuple[] {t1, t2, t3, t4, t5, t6};

        Arrays.sort(tuples);

        Assert.assertArrayEquals(new DfTuple[] {t2, t6, t4, t3, t5, t1}, tuples);
    }
}
