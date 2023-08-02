package com.ly.hbase;

/**
 * @description:
 * @author:Zane
 * @createTime:2021/8/18 1:10
 * @version:1.0
 */

import com.ly.util.Property;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public abstract class TableInputFormat<T extends Tuple> extends AbstractTableInputFormat<T> {
    private static final long serialVersionUID = 1L;
    private static Connection conn;

    public TableInputFormat() {
    }

    @Override
    protected abstract Scan getScanner();

    @Override
    protected abstract String getTableName();

    protected abstract T mapResultToTuple(Result var1);

    /*@Override
    public void configure(Configuration parameters) {
        this.table = this.createTable();
        if (this.table != null) {
            this.scan = this.getScanner();
        }

    }*/

    @Override
    public void configure(Configuration parameters) {
        try {
            this.table = (HTable) createTable();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.scan = new Scan();
    }

    /*private HTable createTable() {
        LOG.info("Initializing HBaseConfiguration");
        org.apache.hadoop.conf.Configuration hConf = HBaseConfiguration.create();

        try {
            return new HTable(hConf, this.getTableName());
        } catch (Exception var3) {
            LOG.error("Error instantiating a new HTable instance", var3);
            return null;
        }
    }*/

    private Table createTable() throws IOException {
        LOG.info("Initializing HBaseConfiguration");
        org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", Property.getStrValue("hbase.rootdir"));
        conf.set("hbase.zookeeper.quorum", Property.getStrValue("hbase.zookeeper.quorum"));
        conf.set("hbase.client.scanner.timeout.period", Property.getStrValue("hbase.client.scanner.timeout.period"));
        conf.set("hbase.rpc.timeout", Property.getStrValue("hbase.rpc.timeout"));
        System.out.println(Property.getStrValue("hbase.rootdir"));
        conn = ConnectionFactory.createConnection(conf);
        System.out.println("this.getTableName()>>>>"+this.getTableName());
        Table table = conn.getTable(TableName.valueOf(this.getTableName()));

        //ClusterConnection connection, TableBuilderBase builder, RpcRetryingCallerFactory rpcCallerFactory, RpcControllerFactory rpcControllerFactory, ExecutorService pool
        return table;
    }

    @Override
    protected T mapResultToOutType(Result r) {
        return this.mapResultToTuple(r);
    }
}
