package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.util.ErrorReporter;
import org.eclipse.collections.api.list.ListIterable;

public class DfStringColumnStored
extends DfObjectColumnStored<String>
implements DfStringColumn
{
    public DfStringColumnStored(DataFrame owner, String newName)
    {
        super(owner, newName);
    }

    public DfStringColumnStored(DataFrame owner, String newName, ListIterable<String> newValues)
    {
        super(owner, newName, newValues);
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isVoid())
        {
            this.addObject(null);
        }
        else if (value.isString())
        {
            this.addMyType(value.stringValue());
        }
        else
        {
            ErrorReporter.reportAndThrow(
                "Attempting to add a value of type " + value.getType()
                + " to a string column " + this.getName()
                + ": " + value.asStringLiteral());
        }
    }

    @Override
    public void addObject(Object newObject)
    {
        this.addMyType((String) newObject);
    }
}
