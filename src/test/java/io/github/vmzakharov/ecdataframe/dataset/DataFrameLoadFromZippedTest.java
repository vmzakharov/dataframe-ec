package io.github.vmzakharov.ecdataframe.dataset;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrame;
import io.github.vmzakharov.ecdataframe.dataframe.DataFrameUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class DataFrameLoadFromZippedTest
{
    private FileSystem fileSystem;

    @Before
    public void configureFileSystem()
    {
        this.fileSystem = Jimfs.newFileSystem(Configuration.unix());
    }

    @Test
    public void loadDataFromZip()
    throws IOException
    {
        String text =
                "Name,EmployeeId,HireDate,Dept,Salary\n"
                + "\"Alice\",1234,2020-01-01,\"Accounting\",110000.00\n"
                + "\"Bob\",1233,2010-01-01,\"Bee-bee-boo-boo\",100000.00\n"
                + "\"Carl\",10000,2005-11-21,\"Controllers\",130000.00\n"
                + "\"Diane\",10001,2012-09-20,\"\",130000.00\n"
                + "\"Ed\",10002,,,0.00"
                ;

        this.writeTextToZippedFile("/foo", "employees.csv", text);

        Path filePath = this.fileSystem.getPath("/foo").resolve("employees.csv.zip");

        CsvDataSet dataSet = new CsvDataSet(filePath, "Employees");

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addLongColumn("EmployeeId").addDateColumn("HireDate").addStringColumn("Dept").addDoubleColumn("Salary")
                .addRow("Alice", 1234, LocalDate.of(2020, 1, 1), "Accounting", 110000.0)
                .addRow("Bob", 1233, LocalDate.of(2010, 1, 1), "Bee-bee-boo-boo", 100000.0)
                .addRow("Carl", 10000, LocalDate.of(2005, 11, 21), "Controllers", 130000.0)
                .addRow("Diane", 10001, LocalDate.of(2012, 9, 20), "", 130000.0)
                .addRow("Ed", 10002, null, "", 0.0);

        DataFrameUtil.assertEquals(expected, loaded);
    }

    @Test
    public void loadDateFromGZip()
    throws IOException
    {
        String text =
                "Name,Date\n"
                + "\"Alice\",2020-01-01\n"
                + "\"Bob\",  2010-1-01\n"
                + "\"Carl\", 2005-11-21\n"
                + "\"Diane\",2012-09-2\n"
                + "\"Ed\","
                ;
        this.writeTextToGZippedFile("/foo", "dates.csv", text);

        Path filePath = this.fileSystem.getPath("/foo").resolve("dates.csv.gz");

        CsvDataSet dataSet = new CsvDataSet(filePath, "Dates");

        DataFrame loaded = dataSet.loadAsDataFrame();

        DataFrame expected = new DataFrame("Expected")
                .addStringColumn("Name").addDateColumn("Date")
                .addRow("Alice", LocalDate.of(2020, 1, 1))
                .addRow("Bob", LocalDate.of(2010, 1, 1))
                .addRow("Carl", LocalDate.of(2005, 11, 21))
                .addRow("Diane", LocalDate.of(2012, 9, 2))
                .addRow("Ed", null);

        DataFrameUtil.assertEquals(expected, loaded);
    }

    private void writeTextToZippedFile(String root, String fileName, String fileText)
    throws IOException
    {
        Path archivedFile = this.prepareFile(root, fileName, ".zip");

        OutputStream fos = Files.newOutputStream(archivedFile);
        ZipOutputStream zos = new ZipOutputStream(fos);

        ZipEntry zipEntry = new ZipEntry(fileName);
        zos.putNextEntry(zipEntry);
        zos.write(fileText.getBytes());
        zos.close();
    }

    private void writeTextToGZippedFile(String root, String fileName, String fileText)
    throws IOException
    {
        Path archivedFile = this.prepareFile(root, fileName, ".gz");

        OutputStream fos = Files.newOutputStream(archivedFile);
        GZIPOutputStream gzos = new GZIPOutputStream(fos);

        gzos.write(fileText.getBytes());
        gzos.close();
    }

    private Path prepareFile(String root, String fileName, String archiveExtension)
    throws IOException
    {
        Path rootPath = this.fileSystem.getPath(root);
        Files.createDirectory(rootPath);

        return rootPath.resolve(fileName + archiveExtension);
    }
}
