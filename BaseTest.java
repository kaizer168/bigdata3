package org.geekbang.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class BaseTest {

    public static void main(String[] args) throws IOException {
        // 建立连接
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "emr-worker-2");
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.master", "emr-header-1:60000");
        Connection conn = ConnectionFactory.createConnection(configuration);
        Admin admin = conn.getAdmin();

        TableName tableName = TableName.valueOf("cheechuen:student");
        String colFamily = "name";
        String colFamily2 = "info";
        String colFamily3 = "score";
        int rowKey = 1;

        // 建表
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists");
        } else {
            HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(colFamily);
            HColumnDescriptor hColumnDescriptor2 = new HColumnDescriptor(colFamily2);
            HColumnDescriptor hColumnDescriptor3 = new HColumnDescriptor(colFamily3);
            hTableDescriptor.addFamily(hColumnDescriptor);
            hTableDescriptor.addFamily(hColumnDescriptor2);
            hTableDescriptor.addFamily(hColumnDescriptor3);
            admin.createTable(hTableDescriptor);
            System.out.println("Table create successful");
        }

        // 插入数据
        Put put = new Put(Bytes.toBytes(rowKey)); // row key
        put.addColumn(Bytes.toBytes(colFamily), Bytes.toBytes("name"), Bytes.toBytes("CheeChuen")); // col1
        put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("student_id"), Bytes.toBytes("G20210698030004")); // col2.1
        put.addColumn(Bytes.toBytes(colFamily2), Bytes.toBytes("class"), Bytes.toBytes("1")); // col2.2
        put.addColumn(Bytes.toBytes(colFamily3), Bytes.toBytes("understanding"), Bytes.toBytes("70")); // col3.1
        put.addColumn(Bytes.toBytes(colFamily3), Bytes.toBytes("programming"), Bytes.toBytes("80")); // col3.1
        conn.getTable(tableName).put(put);
        System.out.println("Data insert success");

        // 查看数据
        Get get = new Get(Bytes.toBytes(rowKey));
        if (!get.isCheckExistenceOnly()) {
            Result result = conn.getTable(tableName).get(get);
            for (Cell cell : result.rawCells()) {
                String colName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("Data get success, colName: " + colName + ", value: " + value);
            }
        }

        // 删除数据
        Delete delete = new Delete(Bytes.toBytes(rowKey));      // 指定rowKey
        conn.getTable(tableName).delete(delete);
        System.out.println("Delete Success");

        // 删除表
        if (admin.tableExists(tableName)) {
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
            System.out.println("Table Delete Successful");
        } else {
            System.out.println("Table does not exist!");
        }
    }
}
