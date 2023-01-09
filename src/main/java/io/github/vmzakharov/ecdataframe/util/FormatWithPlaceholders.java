package io.github.vmzakharov.ecdataframe.util;

import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;

import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FormatWithPlaceholders
{
    private final String template;
    private final MutableMap<String, String> valuesByName = Maps.mutable.of();

    public FormatWithPlaceholders(String newTemplate)
    {
        this.template = newTemplate;
    }

    public static FormatWithPlaceholders format(String newTemplate)
    {
        return new FormatWithPlaceholders(newTemplate);
    }

    public FormatWithPlaceholders with(String name, String value)
    {
        this.valuesByName.put(name, value);
        return this;
    }

    @Override
    public String toString()
    {
        return this.substitute();
    }

    public Supplier<String> toStringSupplier()
    {
        return this::substitute;
    }

    private String substitute()
    {
        StringBuilder result = new StringBuilder();

        Pattern placeholderPattern = Pattern.compile("\\$\\{[\\w]++}");

        Matcher placeholderMatcher = placeholderPattern.matcher(this.template);

        int lastIndex = 0;
        while (placeholderMatcher.find())
        {
            String placeholder = placeholderMatcher.group(0);

            String key = placeholder.substring(2, placeholder.length() - 1);
            String value = this.valuesByName.getIfAbsentValue(key, "(" + key + " is unknown)");

            result
                    .append(this.template, lastIndex, placeholderMatcher.start(0))
                    .append(value);

            lastIndex = placeholderMatcher.end(0);
        }

        result.append(this.template.substring(lastIndex));

        return result.toString();
    }
}
