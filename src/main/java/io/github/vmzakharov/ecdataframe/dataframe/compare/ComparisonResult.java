package io.github.vmzakharov.ecdataframe.dataframe.compare;

import static io.github.vmzakharov.ecdataframe.dataframe.compare.ComparisonResult.NullSide.BOTH_NULLS;
import static io.github.vmzakharov.ecdataframe.dataframe.compare.ComparisonResult.NullSide.LEFT_NULL;
import static io.github.vmzakharov.ecdataframe.dataframe.compare.ComparisonResult.NullSide.NO_NULLS;
import static io.github.vmzakharov.ecdataframe.dataframe.compare.ComparisonResult.NullSide.RIGHT_NULL;

abstract public class ComparisonResult
{
    private NullSide nullSide = NO_NULLS;

    /**
     * result of comparison that follows the contract of a Java comparator
     * @return zero if the values in the data frame cells being compared are equals, a negative values if the first
     * value is less than the second one, a positive value if the first value greater than the second one
     */
    abstract public int compared();

    /**
     * Calculates the difference as a {@code long} value for the compared values, where applicable (e.g., for integer values).
     * If there is no "natural" difference that can be computed the value follows the contract of the regular Java
     * compare method but is meaningless otherwise
     * @return the difference between the values being compared, if applicable
     */
    abstract public long delta();

    /**
     * Calculates the difference as a {@code double} value for the compared values, where applicable (e.g., for double values).
     * If there is no "natural" difference that can be computed then the value follows the contract of the regular Java
     * compare method but is meaningless otherwise
     * @return the difference between the values being compared, if applicable
     */
    public double dDelta()
    {
        return this.delta();
    }

    /*
    used to populate the delta value in case one of the values being compared is null. Otherwise, populating delta is
    delegated to the relevant type specific subclass
     */
    abstract protected void delta(int newDelta);

    public boolean bothAreNulls()
    {
        return this.nullSide == BOTH_NULLS;
    }

    public boolean leftIsNull()
    {
        return this.nullSide == LEFT_NULL;
    }

    public boolean rightIsNull()
    {
        return this.nullSide == RIGHT_NULL;
    }

    public boolean noNulls()
    {
        return this.nullSide == NO_NULLS;
    }

    public NullSide nullSide()
    {
        return this.nullSide;
    }

    public void nullSide(NullSide newNullSide)
    {
        this.nullSide = newNullSide;
    }

    protected void dealWithNullsIfAny(boolean thisIsNull, boolean thatIsNull)
    {
        if (thisIsNull)
        {
            if (thatIsNull)
            {
                this.delta(0);
                this.nullSide(BOTH_NULLS);
            }
            else
            {
                this.delta(-1);
                this.nullSide(LEFT_NULL);
            }
        }
        else if (thatIsNull)
        {
            this.delta(1);
            this.nullSide(RIGHT_NULL);
        }
        else
        {
            this.nullSide(NO_NULLS);
        }
    }

    enum NullSide
    {
        LEFT_NULL, RIGHT_NULL, BOTH_NULLS, NO_NULLS
    }
}
