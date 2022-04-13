package io.github.vmzakharov.ecdataframe.dsl.projection;

import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrameUtil;
import io.github.vmzakharov.ecdataframe.dataset.HierarchicalDataSet;
import io.github.vmzakharov.ecdataframe.dataset.ObjectListDataSet;
import io.github.vmzakharov.ecdataframe.dsl.EvalContext;
import io.github.vmzakharov.ecdataframe.dsl.ProjectionExpr;
import io.github.vmzakharov.ecdataframe.dsl.Script;
import io.github.vmzakharov.ecdataframe.dsl.SimpleEvalContext;
import io.github.vmzakharov.ecdataframe.dsl.value.DataFrameValue;
import io.github.vmzakharov.ecdataframe.dsl.value.Value;
import io.github.vmzakharov.ecdataframe.dsl.value.ValueType;
import io.github.vmzakharov.ecdataframe.dsl.visitor.InMemoryEvaluationVisitor;
import io.github.vmzakharov.ecdataframe.dsl.visitor.TypeInferenceVisitor;
import io.github.vmzakharov.ecdataframe.util.ExpressionParserHelper;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.ListIterable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.DOUBLE;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.LONG;
import static io.github.vmzakharov.ecdataframe.dsl.value.ValueType.STRING;

public class HierarchicalDataSetProjectionTest
{
    private HierarchicalDataSet dataSet;

    @Before
    public void initializeDataSet()
    {
        this.dataSet = new ObjectListDataSet("Person", Lists.immutable.of(
                new Person("Alice", 123, 95.7, LocalDate.of(2022, 4, 3), "Lutefisk", LocalDateTime.of(1911, 11, 11, 11, 11, 11)),
                new Person("Bob", 456, 36.6, LocalDate.of(2022, 4, 2), "Chocolate", LocalDateTime.of(2022, 4, 6, 15, 24, 35))
        ));
    }

    @Test
    public void simpleDataSetOperations()
    {
        Assert.assertTrue(this.dataSet.hasNext());

        this.dataSet.next();

        Assert.assertEquals("Alice", this.dataSet.getValue("name"));
        Assert.assertEquals(123L, this.dataSet.getValue("luckyNumber"));
        Assert.assertEquals(95.7, this.dataSet.getValue("temperature"));

        Assert.assertTrue(this.dataSet.hasNext());

        this.dataSet.next();

        Assert.assertEquals("Bob", this.dataSet.getValue("name"));
        Assert.assertEquals(456L, this.dataSet.getValue("luckyNumber"));
        Assert.assertEquals(36.6, this.dataSet.getValue("temperature"));

        Assert.assertFalse(this.dataSet.hasNext());
    }

    @Test
    public void dataSetOperationsWithPropertyChain()
    {
        this.dataSet.next();

        Assert.assertEquals("Alice", this.dataSet.getValue("name"));
        Assert.assertEquals(123L, this.dataSet.getValue("luckyNumber"));
        Assert.assertEquals(123L, this.dataSet.getValue("bigLuckyNumber"));
        Assert.assertEquals(95.7, this.dataSet.getValue("temperature"));
        Assert.assertEquals(95.7, this.dataSet.getValue("bigTemperature"));
        Assert.assertEquals("Lutefisk", this.dataSet.getValue("favoriteFood.description"));

        this.dataSet.next();

        Assert.assertEquals("Bob", this.dataSet.getValue("name"));
        Assert.assertEquals(456L, this.dataSet.getValue("luckyNumber"));
        Assert.assertEquals(456L, this.dataSet.getValue("bigLuckyNumber"));
        Assert.assertEquals(36.6, this.dataSet.getValue("bigTemperature"));
        Assert.assertEquals(LocalDate.of(2022, 4, 2), this.dataSet.getValue("specialDate"));
        Assert.assertEquals("Chocolate", this.dataSet.getValue("favoriteFood.description"));
        Assert.assertEquals(LocalDateTime.of(2022, 4, 6, 15, 24, 35), this.dataSet.getValue("favoriteFood.lastEaten"));
    }

    @Test
    public void typeInference()
    {
        String scriptString =
                "project {\n"
                + "    Person.name,\n"
                + "    ${Lucky Number} : Person.luckyNumber,\n"
                + "    Temp : Person.temperature,\n"
                + "    Abc : \"ABC\"\n"
                + "}";

        EvalContext context = new SimpleEvalContext();
        context.addDataSet(this.dataSet);
        TypeInferenceVisitor visitor = new TypeInferenceVisitor(context);

        ProjectionExpr expr = (ProjectionExpr) ExpressionParserHelper.DEFAULT.toProjection(scriptString);

        ListIterable<ValueType> projectionExpressionTypes = expr.getProjectionExpressions().collect(visitor::inferExpressionType);

        Assert.assertEquals(Lists.immutable.of(STRING, LONG, DOUBLE, STRING), projectionExpressionTypes);
    }

    @Test
    public void simpleProjection()
    {
        String scriptString =
            "project {\n"
            + "    Person.name,\n"
            + "    ${Lucky Number} : Person.luckyNumber,\n"
            + "    Temp : Person.temperature,\n"
            + "    Abc : \"ABC\"\n"
            + "}";

        Script script = ExpressionParserHelper.DEFAULT.toScript(scriptString);

        InMemoryEvaluationVisitor visitor = new InMemoryEvaluationVisitor();
        visitor.getContext().addDataSet(this.dataSet);
        this.dataSet.openFileForReading();

        Value result = script.evaluate(visitor);

        Assert.assertEquals(DataFrameValue.class, result.getClass());

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Person.name").addLongColumn("Lucky Number").addDoubleColumn("Temp").addStringColumn("Abc")
                        .addRow("Alice", 123, 95.7, "ABC")
                        .addRow("Bob",   456, 36.6, "ABC"),
                ((DataFrameValue) result).dataFrameValue()
        );
    }

    @Test
    public void projectionWithBigNumbers()
    {
        String scriptString =
                "project {\n"
                + "    Person.name,\n"
                + "    ${Big Lucky Number} : Person.bigLuckyNumber,\n"
                + "    BigTemp : Person.bigTemperature\n"
                + "}";

        Script script = ExpressionParserHelper.DEFAULT.toScript(scriptString);

        InMemoryEvaluationVisitor visitor = new InMemoryEvaluationVisitor();
        visitor.getContext().addDataSet(this.dataSet);
        this.dataSet.openFileForReading();

        Value result = script.evaluate(visitor);

        Assert.assertEquals(DataFrameValue.class, result.getClass());

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Person.name").addLongColumn("Big Lucky Number").addDoubleColumn("BigTemp")
                        .addRow("Alice", 123, 95.7)
                        .addRow("Bob",   456, 36.6),
                ((DataFrameValue) result).dataFrameValue()
        );
    }

    @Test
    public void nestedObjectProjection()
    {
        String scriptString =
                "project {\n"
                + "    Person.name,\n"
                + "    ${Lucky Number} : Person.luckyNumber,\n"
                + "    Temp : Person.temperature,\n"
                + "    Food : Person.favoriteFood.description\n"
                + "} where Person.luckyNumber < 200";

        Script script = ExpressionParserHelper.DEFAULT.toScript(scriptString);

        InMemoryEvaluationVisitor visitor = new InMemoryEvaluationVisitor();
        visitor.getContext().addDataSet(this.dataSet);
        this.dataSet.openFileForReading();

        Value result = script.evaluate(visitor);

        Assert.assertEquals(DataFrameValue.class, result.getClass());

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Person.name").addLongColumn("Lucky Number").addDoubleColumn("Temp").addStringColumn("Food")
                        .addRow("Alice", 123, 95.7, "Lutefisk"),
                ((DataFrameValue) result).dataFrameValue()
        );
    }

    @Test
    public void nestedObjectProjectionWithDates()
    {
        String scriptString =
                "project {\n"
                + "    Person.name,\n"
                + "    ${Lucky Number} : Person.luckyNumber,\n"
                + "    Date : Person.specialDate,\n"
                + "    FavoriteFood : Person.favoriteFood.description,\n"
                + "    LastEaten : Person.favoriteFood.lastEaten,\n"
                + "    LastEatenDateOnly : Person.favoriteFood.lastEaten.toLocalDate\n"
                + "}";

        Script script = ExpressionParserHelper.DEFAULT.toScript(scriptString);

        InMemoryEvaluationVisitor visitor = new InMemoryEvaluationVisitor();
        visitor.getContext().addDataSet(this.dataSet);
        this.dataSet.openFileForReading();

        Value result = script.evaluate(visitor);

        Assert.assertEquals(DataFrameValue.class, result.getClass());

        DataFrameUtil.assertEquals(
                new DataFrame("expected")
                        .addStringColumn("Person.name").addLongColumn("Lucky Number").addDateColumn("Date")
                        .addStringColumn("FavoriteFood").addDateTimeColumn("LastEaten").addDateColumn("LastEatenDateOnly")
                        .addRow("Alice", 123, LocalDate.of(2022, 4, 3), "Lutefisk",  LocalDateTime.of(1911, 11, 11, 11, 11, 11), LocalDate.of(1911, 11, 11))
                        .addRow("Bob",   456, LocalDate.of(2022, 4, 2), "Chocolate", LocalDateTime.of(2022, 4, 6, 15, 24, 35),   LocalDate.of(2022, 4, 6)),
                ((DataFrameValue) result).dataFrameValue()
        );
    }
}
