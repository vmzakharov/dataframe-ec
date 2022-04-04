package io.github.vmzakharov.ecdataframe.dsl.projection;

public class Person
{
    private String name;
    private long luckyNumber;
    private double temperature;
    private FavoriteFood favoriteFood;

    public Person(String newName, long newLuckyNumber, double newTemperature)
    {
        this.name = newName;
        this.luckyNumber = newLuckyNumber;
        this.temperature = newTemperature;
    }

    public Person(String newName, long newLuckyNumber, double newTemperature, String favoriteFoodName)
    {
        this.name = newName;
        this.luckyNumber = newLuckyNumber;
        this.temperature = newTemperature;
        this.favoriteFood = new FavoriteFood(favoriteFoodName);
    }

    public String name()
    {
        return this.name;
    }

    public long luckyNumber()
    {
        return this.luckyNumber;
    }

    public double temperature()
    {
        return this.temperature;
    }

    public FavoriteFood favoriteFood()
    {
        return this.favoriteFood;
    }

    public static class FavoriteFood
    {
        private String description;

        public FavoriteFood(String newDescription)
        {
            this.description = newDescription;
        }

        public String description()
        {
            return this.description;
        }
    }
}
