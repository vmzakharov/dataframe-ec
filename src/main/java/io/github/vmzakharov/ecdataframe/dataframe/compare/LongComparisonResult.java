package io.github.vmzakharov.ecdataframe.dataframe.compare;

import java.util.function.LongSupplier;

public class LongComparisonResult
        extends ComparisonResult
{
    private long delta;

    public LongComparisonResult(LongSupplier thisValueSupplier, LongSupplier otherValueSupplier, boolean thisIsNull, boolean otherIsNull)
    {
        this.dealWithNullsIfAny(thisIsNull, otherIsNull);

        if (this.noNulls())
        {
            long thisValue = thisValueSupplier.getAsLong();
            long otherValue = otherValueSupplier.getAsLong();
            this.delta = thisValue - otherValue;

            // check for overflows
            if ((thisValue > otherValue) && (this.delta < 0))
            {
                this.delta = Long.MAX_VALUE;
            }
            else if ((thisValue < otherValue) && (this.delta > 0))
            {
                this.delta = Long.MIN_VALUE;
            }
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
        return this.delta == 0 ? 0 : (this.delta > 0 ? 1 : -1);
    }
}
