package com.jowilf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import scala.Tuple2;

public class HBaseWriter {
    private Table table;
    private Connection connection;
    TableUtils tableUtils = new TableUtils();

    public HBaseWriter() throws IOException {
        // Create a new connection to HBase
        connection = tableUtils.newConnection();
    }

    void writeEvents(List<Tuple2<String, Integer>> events, String time) throws IOException {
        // Get the table reference for "events"
        table = connection.getTable(TableUtils.EVENTS_TABLE);
        // Create a new row with the specified timestamp
        Put p = new Put(b(time));
        // Iterate through the list of events, and add each event to the row
        for (Tuple2<String, Integer> event : events) {
            p.addColumn(b(TableUtils.CF_EVENTS), b(event._1()), b(event._2().toString()));
        }
        // Add the row to the table
        table.put(p);
        // Close the connection
        close();
    }

    /**
     * Writes the given rows containing brand report data to the HBase table within
     * the specified column.
     * 
     * @param rows a list of tuples, where the first element of each tuple is a
     *             brand name and the second element is the report data (views
     *             count, cart count or purchase count)
     * @param col  the name of the column to write the report data to
     * @throws IOException if there is an error accessing the HBase table
     */
    void writeBrandReport(List<Tuple2<String, Long>> rows, String col) throws IOException {
        // Get the table reference for "analytics"
        table = connection.getTable(TableUtils.ANALYTICS_TABLE);
        List<Put> puts = new ArrayList<>();
        for (Tuple2<String, Long> row : rows) {
            Put p = new Put(b(row._1()));
            p.addColumn(TableUtils.CF_REPORT.getBytes(), b(col), Bytes.toBytes(row._2().toString()));
            puts.add(p);
        }
        // Add the rows to the table
        table.put(puts);
        // Close the connection
        close();
    }

    // Close the table
    public void close() throws IOException {
        table.close();
    }

    // Shortcut for Bytes.toBytes
    public byte[] b(String v) {
        return Bytes.toBytes(v);
    }
}
