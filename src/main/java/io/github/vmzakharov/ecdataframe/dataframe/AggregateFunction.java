package io.github.vmzakharov.ecdataframe.dataframe;

import org.eclipse.collections.api.DoubleIterable;
import org.eclipse.collections.api.LongIterable;

public abstract class AggregateFunction
{
    private final String description;

    public AggregateFunction(String newDescription)
    {
        this.description = newDescription;
    }

    public static AggregateFunction SUM = new AggregateFunction("SUM")
    {
        @Override
        public double apply(DoubleIterable items)
        {
            return items.sum();
        }

        @Override
        public long apply(LongIterable items)
        {
            return items.sum();
        }

        @Override
        long longInitialValue()
        {
            return 0L;
        }

        @Override
        double doubleInitialValue()
        {
            return 0.0;
        }

        @Override
        long longAccumulator(long currentAggregate, long newValue)
        {
            return currentAggregate + newValue;
        }

        @Override
        double doubleAccumulator(double currentAggregate, double newValue)
        {
            return currentAggregate + newValue;
        }

        @Override
        public long defaultLongIfEmpty()
        {
            return 0L;
        }

        @Override
        public double defaultDoubleIfEmpty()
        {
            return 0.0;
        }
    };

    public static AggregateFunction MAX = new AggregateFunction("MAX")
    {
        @Override
        public double apply(DoubleIterable items)
        {
            return items.max();
        }

        @Override
        public long apply(LongIterable items)
        {
            return items.max();
        }

        @Override
        long longInitialValue()
        {
            return Long.MIN_VALUE;
        }

        @Override
        double doubleInitialValue()
        {
            return Double.MIN_VALUE;
        }

        @Override
        long longAccumulator(long currentAggregate, long newValue)
        {
            return Math.max(currentAggregate, newValue);
        }

        @Override
        double doubleAccumulator(double currentAggregate, double newValue)
        {
            return Math.max(currentAggregate, newValue);
        }
    };

    public static AggregateFunction MIN = new AggregateFunction("MIN")
    {
        @Override
        public double apply(DoubleIterable items)
        {
            return items.min();
        }

        @Override
        public long apply(LongIterable items)
        {
            return items.min();
        }

        @Override
        long longInitialValue()
        {
            return Long.MAX_VALUE;
        }

        @Override
        double doubleInitialValue()
        {
            return Double.MAX_VALUE;
        }

        @Override
        long longAccumulator(long currentAggregate, long newValue)
        {
            return Math.min(currentAggregate, newValue);
        }

        @Override
        double doubleAccumulator(double currentAggregate, double newValue)
        {
            return Math.min(currentAggregate, newValue);
        }
    };

    public static AggregateFunction AVG = new AggregateFunction("AVG")
    {
        @Override
        public double apply(DoubleIterable items)
        {
            return items.average();
        }

        @Override
        public long apply(LongIterable items)
        {
            return Math.round(items.average());
        }

        @Override
        long longAccumulator(long currentAggregate, long newValue)
        {
            return currentAggregate + newValue;
        }

        @Override
        double doubleAccumulator(double currentAggregate, double newValue)
        {
            return currentAggregate + newValue;
        }

        @Override
        long longInitialValue()
        {
            return 0;
        }

        @Override
        double doubleInitialValue()
        {
            return 0;
        }

        @Override
        public long defaultLongIfEmpty()
        {
            return 0L;
        }

        @Override
        public double defaultDoubleIfEmpty()
        {
            return 0.0;
        }
    };

    abstract double apply(DoubleIterable items);
    abstract long apply(LongIterable items);

    abstract long longInitialValue();
    abstract double doubleInitialValue();

    abstract long longAccumulator(long currentAggregate, long newValue);
    abstract double doubleAccumulator(double currentAggregate, double newValue);

    public long defaultLongIfEmpty()
    {
        throw new UnsupportedOperationException("Operation " + this.description + " is not defined on empty lists");
    }

    public double defaultDoubleIfEmpty()
    {
        throw new UnsupportedOperationException("Operation " + this.description + " is not defined on empty lists");
    }
}
