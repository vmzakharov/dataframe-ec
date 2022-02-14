package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DateValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.list.ListIterable;

import java.time.LocalDate;

public class DfDateColumnStored
extends DfObjectColumnStored<LocalDate>
implements DfDateColumn
{
    public DfDateColumnStored(DataFrame owner, String newName)
    {
        super(owner, newName);
    }

    public DfDateColumnStored(DataFrame owner, String newName, ListIterable<LocalDate> newValues)
    {
        super(owner, newName, newValues);
    }

    @Override
    public Value getValue(int rowIndex)
    {
        return new DateValue(this.getTypedObject(rowIndex));
    }

    @Override
    public void addObject(Object newObject)
    {
        this.addMyType((LocalDate) newObject);
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isVoid())
        {
            this.addObject(null);
        }
        else if (value.isDate())
        {
            this.addMyType(((DateValue) value).dateValue());
        }
        else
        {
            ErrorReporter.reportAndThrow(
                "Attempting to add a value of type " + value.getType()
                + " to a date column " + this.getName()
                + ": " + value.asStringLiteral());
        }
    }
}
