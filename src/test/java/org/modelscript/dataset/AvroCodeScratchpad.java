package org.modelscript.dataset;

import org.apache.avro.generic.GenericRecord;
import org.eclipse.collections.api.list.ListIterable;
import org.eclipse.collections.api.list.MutableList;
import org.eclipse.collections.impl.factory.Lists;
import org.modelscript.ExpressionTestUtil;
import org.modelscript.expr.Script;
import org.modelscript.expr.value.Value;
import org.modelscript.expr.value.ValueType;
import org.modelscript.expr.visitor.InMemoryEvaluationVisitor;
import org.modelscript.util.ExpressionParserHelper;

public class AvroCodeScratchpad
{
    public static void main(String[] args)
    {
        System.out.println("Current working directory: "+System.getProperty("user.dir"));

//        writeStuff();
//        System.out.println("-----------");
        readStuff();
        System.out.println("-----------");
//        System.out.println("-----------");
        projectionScript();
    }

    private static void writeStuff()
    {
        AvroDataSet dataSet = new AvroDataSet("src/test/resources/user.avsc", "User", "users.avro");

        GenericRecord user1 = dataSet.createRecord();
        user1.put("name", "Alyssa");
        user1.put("favorite_number", 256);

        GenericRecord address1 = dataSet.createRecordForField("address");
        address1.put("state", "ZZ");
        address1.put("city", "Mwa");

        user1.put("address", address1);

        GenericRecord user2 = dataSet.createRecord();
        user2.put("name", "Ben");
        user2.put("favorite_number", 7);
        user2.put("favorite_color", "red");

        GenericRecord user3 = dataSet.createRecord();
        user3.put("name", "Clair");
        user3.put("favorite_number", 2);
        user3.put("favorite_color", "lavender");

        GenericRecord address3 = dataSet.createRecordForField("address");
        address3.put("state", "JJ");
        address3.put("city",  "Springfield");

        user3.put("address", address3);

        dataSet.write(user1, user2, user3);
    }

    private static void readStuff()
    {
        AvroDataSet dataSet = new AvroDataSet("src/test/resources/user.avsc", "User", "users.avro");

        System.out.println(
                Lists.immutable.of("name", "favorite_number", "favorite_color", "address")
                        .collect(e -> e + " -> " + dataSet.getFieldType(e))
                        .makeString("\n"));

        MutableList<Object> users = Lists.mutable.of();

        dataSet.openFileForReading();
        while (dataSet.hasNext())
        {
            users.add(dataSet.next());
        }

        users.forEach(System.out::println);
    }

    private static void projectionScript()
    {
        AvroDataSet dataSet = new AvroDataSet("src/test/resources/user.avsc", "User", "users.avro");

        String scriptString = "" +
                "x = \"Ben\"\n" +
                "limit = 1\n" +
                "project {\n" +
                "    User.name,\n" +
                "    Color : User.favorite_color,\n" +
                "    Oompa : \"Loompa\",\n" +
                "    Fav : User.favorite_number\n" +
                "} where User.favorite_number > limit + 2";

        System.out.println(scriptString);
        System.out.println("-----------");
        Script script = ExpressionParserHelper.toScript(scriptString);

        InMemoryEvaluationVisitor visitor = new InMemoryEvaluationVisitor();
        visitor.getContext().addDataSet(dataSet);
        Value result = script.evaluate(visitor);
        System.out.println(result.stringValue());
    }

}
