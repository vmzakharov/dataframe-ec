package io.github.vmzakharov.ecdataframe.dsl;

import io.github.vmzakharov.ecdataframe.dsl.value.BooleanValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.VectorValue;

public interface ContainsOp
extends PredicateOp
{
    ContainsOp IN = new ContainsOp()
    {
        @Override
        public BooleanValue applyWithVector(Value value, VectorValue vectorValue)
        {
            return BooleanValue.valueOf(vectorValue.getElements().anySatisfy(e -> ComparisonOp.EQ.apply(e, value).isTrue()));
        }

        @Override
        public BooleanValue applyString(String operand1, String operand2)
        {
            return BooleanValue.valueOf(operand2.contains(operand1));
        }

        @Override
        public String asString()
        {
            return "in";
        }
    };

    ContainsOp NOT_IN = new ContainsOp()
    {
        @Override
        public BooleanValue applyWithVector(Value value, VectorValue vectorValue)
        {
            return BooleanValue.valueOf(vectorValue.getElements().allSatisfy(e -> ComparisonOp.NE.apply(e, value).isTrue()));
        }

        @Override
        public BooleanValue applyString(String operand1, String operand2)
        {
            return BooleanValue.valueOf(!operand2.contains(operand1));
        }

        @Override
        public String asString()
        {
            return "not in";
        }
    };

    @Override
    default BooleanValue apply(Value operand1, Value operand2)
    {
        if (operand2.isVector())
        {
            return this.applyWithVector(operand1, (VectorValue) operand2);
        }
        return operand1.applyPredicate(operand2, this);
    }

    BooleanValue applyWithVector(Value value, VectorValue vectorValue);

    String asString();
}
