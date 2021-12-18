package io.github.vmzakharov.ecdataframe.dataframe;

import java.time.LocalDate;
import java.util.function.DoubleSupplier;
import java.util.function.LongSupplier;

import static io.github.vmzakharov.ecdataframe.dataframe.ComparisonResult.NullSide.BOTH_NULLS;
import static io.github.vmzakharov.ecdataframe.dataframe.ComparisonResult.NullSide.LEFT_NULL;
import static io.github.vmzakharov.ecdataframe.dataframe.ComparisonResult.NullSide.NO_NULLS;
import static io.github.vmzakharov.ecdataframe.dataframe.ComparisonResult.NullSide.RIGHT_NULL;

abstract public class ComparisonResult
{
    private NullSide nullSide = NO_NULLS;

    abstract public int asDirection();

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

    static public class DoubleComparisonResult
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

        public double delta()
        {
            return this.delta;
        }

        @Override
        protected void delta(int newDelta)
        {
            this.delta = newDelta;
        }

        @Override
        public int asDirection()
        {
            return Double.compare(this.delta, 0.0);
        }
    }

    static public class LongComparisonResult
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
                if (thisValue > otherValue)
                {
                    if (this.delta < 0)
                    {
                        this.delta = Long.MAX_VALUE;
                    }
                }
                else
                {
                    if (thisValue < otherValue)
                    {
                        if (this.delta > 0)
                        {
                            this.delta = Long.MIN_VALUE;
                        }
                    }
                }
            }
        }

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
        public int asDirection()
        {
            return this.delta == 0 ? 0 : (this.delta > 0 ? 1 : -1);
        }
    }

    abstract static public class ObjectComparisonResult<T extends Comparable<? super T>>
    extends ComparisonResult
    {
        private int delta;

        public ObjectComparisonResult(T thisObject, T thatObject)
        {
            this.dealWithNullsIfAny(thisObject == null, thatObject == null);

            if (this.noNulls())
            {
                this.delta = thisObject.compareTo(thatObject);
            }
        }

        public int delta()
        {
            return this.delta;
        }

        protected void delta(int newDelta)
        {
            this.delta = newDelta;
        }

        @Override
        public int asDirection()
        {
            return this.delta;
        }
    }

    static public class StringComparisonResult
    extends ObjectComparisonResult<String>
    {
        public StringComparisonResult(String thisObject, String thatObject)
        {
            super(thisObject, thatObject);
        }
    }

    static public class DateComparisonResult
    extends ObjectComparisonResult<LocalDate>
    {
        public DateComparisonResult(LocalDate thisObject, LocalDate thatObject)
        {
            super(thisObject, thatObject);
        }
    }
}
