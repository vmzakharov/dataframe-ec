package io.github.vmzakharov.ecdataframe.util;

public interface Printer
{
    void print(String value);

    default void newLine()
    {
        this.print("\n");
    }

    default void println(String value)
    {
        this.print(value);
        this.newLine();
    }
}
