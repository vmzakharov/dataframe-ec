package io.github.vmzakharov.ecdataframe.dataframe.compare;

import java.util.function.DoubleSupplier;

public class DoubleComparisonResult
extends ComparisonResult
{
    private double delta;

    public DoubleComparisonResult(DoubleSupplier thisValueSupplier, DoubleSupplier otherValueSupplier, boolean thisIsNull, boolean otherIsNull)
    {
        this.dealWithNullsIfAny(thisIsNull, otherIsNull);

        if (this.noNulls())
        {
            double thisValue = thisValueSupplier.getAsDouble();
            double otherValue = otherValueSupplier.getAsDouble();
            this.delta = thisValue - otherValue;
        }
    }

    @Override
    public long delta()
    {
        return Math.round(this.delta);
    }

    @Override
    public double dDelta()
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
        return Double.compare(this.delta, 0.0);
    }
}
