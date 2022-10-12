package io.github.vmzakharov.ecdataframe.dataframe.compare;

import java.time.LocalDate;

public class DateComparisonResult
 extends ObjectComparisonResult<LocalDate>
{
    public DateComparisonResult(LocalDate thisObject, LocalDate thatObject)
    {
        super(thisObject, thatObject);
    }
}
