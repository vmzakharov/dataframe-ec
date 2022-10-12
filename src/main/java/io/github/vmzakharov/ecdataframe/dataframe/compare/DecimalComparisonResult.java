package io.github.vmzakharov.ecdataframe.dataframe.compare;

import java.math.BigDecimal;

public class DecimalComparisonResult
extends ComparisonResult
{
    private BigDecimal decDelta;

    public DecimalComparisonResult(BigDecimal thisObject, BigDecimal thatObject)
    {
        this.dealWithNullsIfAny(thisObject == null, thatObject == null);

        if (this.noNulls())
        {
            this.decDelta = thisObject.subtract(thatObject);
        }
    }

    @Override
    public double dDelta()
    {
        return this.decDelta.doubleValue();
    }

    public BigDecimal decDelta()
    {
        return this.decDelta;
    }

    @Override
    public int compared()
    {
        return this.decDelta.signum();
    }

    @Override
    public long delta()
    {
        return this.decDelta.longValue();
    }

    @Override
    protected void delta(int newDelta)
    {
        this.decDelta = BigDecimal.valueOf(newDelta);
    }
}
