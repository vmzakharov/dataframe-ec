package org.modelscript.util;

public interface Printer
{
    void print(String value);

    default void newLine() { print("\n"); }

    default void println(String value)
    {
        this.print(value);
        this.newLine();
    }
}
