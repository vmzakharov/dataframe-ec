package io.github.vmzakharov.ecdataframe.dataframe;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

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

        assertTrue(t1.compareTo(t2) > 0);
        assertTrue(t2.compareTo(t1) < 0);
        assertNotEquals(t1, t2);

        assertTrue(t5.compareTo(t3) > 0);
        assertTrue(t3.compareTo(t5) < 0);
        assertNotEquals(t5, t3);

        assertTrue(t5.compareTo(t4) > 0);
        assertTrue(t4.compareTo(t5) < 0);
        assertNotEquals(t5, t3);

        assertEquals(0, t2.compareTo(t6));
        assertEquals(t2, t6);

        assertEquals(0, t4.compareTo(t4));
        assertEquals(0, t5.compareTo(t5));
        assertEquals(0, t6.compareTo(t6));
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

        assertArrayEquals(new DfTuple[] {t2, t6, t4, t3, t5, t1}, tuples);
    }
}
