package io.github.vmzakharov.ecdataframe;

import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;

public class DateExpressionTest
{
    @Test
    public void parseDate()
    {
        LocalDate date = ExpressionTestUtil.evaluateToDate("toDate(\"2020-02-15\")");
        Assert.assertEquals(LocalDate.of(2020, 2, 15), date);
    }

    @Test
    public void dateProximity()
    {
        Assert.assertTrue(ExpressionTestUtil.evaluateToBoolean("withinDays(toDate(\"2020-02-15\"), toDate(\"2020-02-17\"), 3)"));
        Assert.assertFalse(ExpressionTestUtil.evaluateToBoolean("withinDays(toDate(\"2020-02-11\"), toDate(\"2020-02-17\"), 3)"));
    }
}
