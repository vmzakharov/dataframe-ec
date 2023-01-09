package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.Value;
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
            this.throwAddingIncompatibleValueException(value);
        }
    }

    @Override
    public void addObject(Object newObject)
    {
        this.addMyType((String) newObject);
    }
}
