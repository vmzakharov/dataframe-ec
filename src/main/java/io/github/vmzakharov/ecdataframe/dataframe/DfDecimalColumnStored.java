package io.github.vmzakharov.ecdataframe.dataframe;

import io.github.vmzakharov.ecdataframe.dsl.value.DecimalValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import org.eclipse.collections.api.list.ListIterable;

import java.math.BigDecimal;

public class DfDecimalColumnStored
extends DfObjectColumnStored<BigDecimal>
implements DfDecimalColumn
{
    public DfDecimalColumnStored(DataFrame owner, String newName)
    {
        super(owner, newName);
    }

    public DfDecimalColumnStored(DataFrame owner, String newName, ListIterable<BigDecimal> newValues)
    {
        super(owner, newName, newValues);
    }

    @Override
    public void addObject(Object newObject)
    {
        this.addMyType((BigDecimal) newObject);
    }

    @Override
    public void addValue(Value value)
    {
        if (value.isVoid())
        {
            this.addObject(null);
        }
        else if (value.isDecimal())
        {
            this.addMyType(((DecimalValue) value).decimalValue());
        }
        else
        {
            this.throwAddingIncompatibleValueException(value);
        }
    }
}
