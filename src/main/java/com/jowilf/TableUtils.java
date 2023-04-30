package com.jowilf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

public class TableUtils {
    public static TableName EVENTS_TABLE = TableName.valueOf("electronic-store");
    public static TableName ANALYTICS_TABLE = TableName.valueOf("electronic-analytics");
    public static String CF_EVENTS = "events";
    public static String CF_REPORT = "report";

    public Configuration getConfig() {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "zookeeper");
        config.set("hbase.zookeeper.property.clientPort", "2181");
        return config;
    }

    public Connection newConnection() throws IOException {
        return ConnectionFactory.createConnection(getConfig());
    }

    public void createRealTimeTable() throws IOException {
        // Create a connection to the HBase server
        Connection connection = newConnection();
        // Create an admin object to perform table administration operations
        Admin admin = connection.getAdmin();

        // Create a table descriptor
        TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(EVENTS_TABLE)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_EVENTS))
                .build();

        // Create the table
        if (admin.tableExists(EVENTS_TABLE)) {
            admin.disableTable(EVENTS_TABLE);
            admin.deleteTable(EVENTS_TABLE);
        }
        admin.createTable(tableDesc);
        System.out.println("==== Events table created!!! ====");
    }

    public void createAnalyticsTable() throws IOException {
        // Create a connection to the HBase server
        Connection connection = newConnection();
        // Create an admin object to perform table administration operations
        Admin admin = connection.getAdmin();

        // Create a table descriptor
        TableDescriptor tableDesc = TableDescriptorBuilder.newBuilder(ANALYTICS_TABLE)
                .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_REPORT))
                .build();

        // Create the table
        if (admin.tableExists(ANALYTICS_TABLE)) {
            admin.disableTable(ANALYTICS_TABLE);
            admin.deleteTable(ANALYTICS_TABLE);
        }
        admin.createTable(tableDesc);
        System.out.println("==== Analytics table  created!!! ====");
    }
}
