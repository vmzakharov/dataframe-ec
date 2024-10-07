package io.github.vmzakharov.ecdataframe.dsl.projection;

import java.math.BigDecimal;

public record Donut(String name, float price, int calories)
{
    public double priceAsDouble()
    {
        return this.price;
    }

    public BigDecimal priceAsDecimal()
    {
        return BigDecimal.valueOf(this.price);
    }

    public long caloriesAsLong()
    {
        return this.calories;
    }
}
