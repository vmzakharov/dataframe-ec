package io.github.vmzakharov.ecdataframe.dataset;

import org.eclipse.collections.api.block.function.primitive.BooleanFunction;
import org.eclipse.collections.impl.utility.StringIterate;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public class BooleanFormatter
{
    private final BooleanFunction<String> booleanParser;
    private final String trueString;
    private final String falseString;

    public BooleanFormatter(String pattern)
    {
        if (StringIterate.isEmpty(pattern))
        {
            this.trueString = "true";
            this.falseString = "false";
        }
        else if (pattern.contains(","))
        {
            String[] elements = pattern.split(",");
            this.trueString = elements[0];
            this.falseString = elements[1];
        }
        else
        {
            throw exceptionByKey("CSV_INVALID_FORMAT_STR")
                    .with("format", pattern)
                    .with("type", "boolean")
                    .get();
        }

        this.booleanParser = s -> {
            if (this.trueString.equals(s)) return true;
            if (this.falseString.equals(s)) return false;

            throw exceptionByKey("CSV_PARSE_ERR")
                    .with("type", "boolean")
                    .with("inputString", s)
                    .get();
        };
    }

    public boolean parseAsBoolean(String aString)
    {
        if (aString == null || aString.isEmpty())
        {
            return false;
        }

        return this.booleanParser.booleanValueOf(aString);
    }

    public String format(boolean booleanValue)
    {
        return booleanValue ? this.trueString : this.falseString;
    }
}
