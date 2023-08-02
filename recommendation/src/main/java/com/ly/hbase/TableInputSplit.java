package com.ly.hbase;

/**
 * @description:
 * @author:Zane
 * @createTime:2021/8/18 0:57
 * @version:1.0
 */
import org.apache.flink.core.io.LocatableInputSplit;

public class TableInputSplit extends LocatableInputSplit {
    private static final long serialVersionUID = 1L;
    private final byte[] tableName;
    private final byte[] startRow;
    private final byte[] endRow;

    TableInputSplit(int splitNumber, String[] hostnames, byte[] tableName, byte[] startRow, byte[] endRow) {
        super(splitNumber, hostnames);
        this.tableName = tableName;
        this.startRow = startRow;
        this.endRow = endRow;
    }

    public byte[] getTableName() {
        return this.tableName;
    }

    public byte[] getStartRow() {
        return this.startRow;
    }

    public byte[] getEndRow() {
        return this.endRow;
    }
}
