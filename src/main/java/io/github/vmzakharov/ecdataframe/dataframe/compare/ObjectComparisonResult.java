package io.github.vmzakharov.ecdataframe.dataframe.compare;

abstract public class ObjectComparisonResult<T extends Comparable<? super T>>
extends ComparisonResult
{
    private int delta;

    public ObjectComparisonResult(T thisObject, T thatObject)
    {
        this.dealWithNullsIfAny(thisObject == null, thatObject == null);

        if (this.noNulls())
        {
            this.delta(thisObject.compareTo(thatObject));
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
        return this.delta;
    }
}
