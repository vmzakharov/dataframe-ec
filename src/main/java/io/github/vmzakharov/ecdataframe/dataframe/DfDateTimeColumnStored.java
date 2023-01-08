package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DateTimeValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.util.ErrorReporter;
import org.eclipse.collections.api.list.ListIterable;

import java.time.LocalDateTime;

public class DfDateTimeColumnStored
extends DfObjectColumnStored<LocalDateTime>
implements DfDateTimeColumn
{
    public DfDateTimeColumnStored(DataFrame owner, String newName)
    {
        super(owner, newName);
    }

    public DfDateTimeColumnStored(DataFrame owner, String newName, ListIterable<LocalDateTime> newValues)
    {
        super(owner, newName, newValues);
    }

    @Override
    public void addObject(Object newObject)
    {
        this.addMyType((LocalDateTime) newObject);
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isVoid())
        {
            this.addObject(null);
        }
        else if (value.isDateTime())
        {
            this.addMyType(((DateTimeValue) value).dateTimeValue());
        }
        else
        {
            ErrorReporter.reportAndThrow(
                "Attempting to add a value of type " + value.getType() + " to a date column " + this.getName()
                + ": " + value.asStringLiteral());
        }
    }
}
