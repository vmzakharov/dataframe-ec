package org.modelscript.dataset;

public interface DataSet
{
    void openFileForReading();

    String getName();

    Object next();

    boolean hasNext();

    void close();
}
