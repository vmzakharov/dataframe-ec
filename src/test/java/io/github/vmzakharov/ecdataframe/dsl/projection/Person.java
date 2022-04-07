package io.github.vmzakharov.ecdataframe.dsl.projection;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Person
{
    private String name;
    private long luckyNumber;
    private double temperature;
    private LocalDate specialDate;
    private LocalDateTime specialDateTime;
    private FavoriteFood favoriteFood;

    public Person(String newName, long newLuckyNumber, double newTemperature, LocalDate newSpecialDate,
                  String favoriteFoodName, LocalDateTime timeLastEaten)
    {
        this.name = newName;
        this.luckyNumber = newLuckyNumber;
        this.temperature = newTemperature;
        this.specialDate = newSpecialDate;
        this.favoriteFood = new FavoriteFood(favoriteFoodName, timeLastEaten);
    }

    public String name()
    {
        return this.name;
    }

    public long luckyNumber()
    {
        return this.luckyNumber;
    }

    public Long bigLuckyNumber()
    {
        return this.luckyNumber;
    }

    public double temperature()
    {
        return this.temperature;
    }

    public Double bigTemperature()
    {
        return this.temperature;
    }

    public LocalDate specialDate()
    {
        return this.specialDate;
    }

    public FavoriteFood favoriteFood()
    {
        return this.favoriteFood;
    }

    public static class FavoriteFood
    {
        private String description;
        private LocalDateTime lastEaten;

        public FavoriteFood(String newDescription, LocalDateTime newLastEaten)
        {
            this.description = newDescription;
            this.lastEaten = newLastEaten;
        }

        public String description()
        {
            return this.description;
        }

        public LocalDateTime lastEaten()
        {
            return this.lastEaten;
        }
    }
}
