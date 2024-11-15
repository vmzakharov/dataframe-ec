package io.github.vmzakharov.ecdataframe.dataframe.compare;

import java.util.function.BooleanSupplier;

public class BooleanComparisonResult
extends ComparisonResult
{
    private int delta;

    public BooleanComparisonResult(BooleanSupplier thisValueSupplier, BooleanSupplier otherValueSupplier, boolean thisIsNull, boolean otherIsNull)
    {
        this.dealWithNullsIfAny(thisIsNull, otherIsNull);

        if (this.noNulls())
        {
            boolean thisValue = thisValueSupplier.getAsBoolean();
            boolean otherValue = otherValueSupplier.getAsBoolean();

            this.delta(Boolean.compare(thisValue, otherValue));
        }
    }

    @Override
    public long delta()
    {
        return this.delta;
    }

    @Override
    protected void delta(int newDelta)
    {
        this.delta = newDelta;
    }

    @Override
    public int compared()
    {
        return Integer.compare(this.delta, 0);
    }
}
