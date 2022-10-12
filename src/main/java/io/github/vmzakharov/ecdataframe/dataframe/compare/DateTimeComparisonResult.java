package io.github.vmzakharov.ecdataframe.dataframe.compare;

import java.time.LocalDateTime;

public class DateTimeComparisonResult
        extends ObjectComparisonResult<LocalDateTime>
{
    public DateTimeComparisonResult(LocalDateTime thisObject, LocalDateTime thatObject)
    {
        super(thisObject, thatObject);
    }
}
