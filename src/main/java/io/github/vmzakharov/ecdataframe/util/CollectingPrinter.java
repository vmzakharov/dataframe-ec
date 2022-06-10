package io.github.vmzakharov.ecdataframe.util;

public class CollectingPrinter
implements Printer
{
    private final StringBuilder buffer = new StringBuilder();

    @Override
    public void print(String value)
    {
        this.buffer.append(value);
    }

    public String toString()
    {
        return this.buffer.toString();
    }

    public void clear()
    {
        this.buffer.setLength(0);
    }
}
