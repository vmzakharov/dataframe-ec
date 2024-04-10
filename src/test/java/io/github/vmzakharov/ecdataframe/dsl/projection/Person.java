package io.github.vmzakharov.ecdataframe.dsl.projection;

import java.time.LocalDate;
import java.time.LocalDateTime;

public record Person(String name, long luckyNumber, double temperature, LocalDate specialDate, FavoriteFood favoriteFood)
{
    public Person(String newName, long newLuckyNumber, double newTemperature, LocalDate newSpecialDate,
                  String favoriteFoodName, LocalDateTime timeLastEaten)
    {
        this(newName, newLuckyNumber, newTemperature, newSpecialDate, new FavoriteFood(favoriteFoodName, timeLastEaten));
    }

    public Long bigLuckyNumber()
    {
        return this.luckyNumber;
    }

    public Double bigTemperature()
    {
        return this.temperature;
    }

    public record FavoriteFood(String description, LocalDateTime lastEaten)
    {
    }
}
