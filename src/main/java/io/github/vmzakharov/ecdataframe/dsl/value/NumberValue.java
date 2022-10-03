package io.github.vmzakharov.ecdataframe.dsl.value;

import java.math.BigDecimal;

public interface NumberValue
extends Value
{
    double doubleValue();

    BigDecimal decimalValue();
}
