package io.github.vmzakharov.ecdataframe.dataset;

import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.impl.utility.StringIterate;

import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.regex.Pattern;

import static io.github.vmzakharov.ecdataframe.util.ExceptionFactory.exceptionByKey;

public class DecimalFormatter
{
    private DecimalFormat decimalFormat;
    private final Function<String, BigDecimal> decimalParser;
    private Pattern nonNumericPattern;

    public DecimalFormatter(String pattern)
    {
        if (StringIterate.isEmpty(pattern))
        {
            this.decimalParser = BigDecimal::new;
        }
        else if (pattern.length() == 1)
        {
            this.nonNumericPattern = Pattern.compile("[^0-9" + pattern + "]");
            this.decimalParser = s -> new BigDecimal(this.stripNonNumericCharacters(s).replace(pattern, "."));
        }
        else
        {
            this.decimalFormat = new DecimalFormat(pattern);
            this.decimalFormat.setParseBigDecimal(true);

            this.decimalParser = s -> {
                try
                {
                    return (BigDecimal) this.decimalFormat.parse(s.trim());
                }
                catch (ParseException e)
                {
                    throw exceptionByKey("CSV_PARSE_ERR")
                            .with("type", "a floating point number")
                            .with("inputString", s).get(e);
                }
            };
        }
    }

    public BigDecimal parseAsDecimal(String aString)
    {
        if (aString == null || aString.isEmpty())
        {
            return null;
        }

        return this.decimalParser.apply(aString);
    }

    private String stripNonNumericCharacters(String s)
    {
        return this.nonNumericPattern.matcher(s).replaceAll("");
    }

    public String format(BigDecimal decimalValue)
    {
        return this.decimalFormat == null ? String.valueOf(decimalValue) : this.decimalFormat.format(decimalValue);
    }
}
