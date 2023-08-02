package com.ly.hbase;

/**
 * @description:
 * @author:Zane
 * @createTime:2021/8/18 0:57
 * @version:1.0
 */
//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTableInputFormat<T> extends RichInputFormat<T, TableInputSplit> {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractTableInputFormat.class);
    protected boolean endReached = false;
    protected transient HTable table = null;
    protected transient Scan scan = null;
    protected ResultScanner resultScanner = null;
    protected byte[] currentRow;
    protected long scannedRows;

    public AbstractTableInputFormat() {
    }

    protected abstract Scan getScanner();

    protected abstract String getTableName();

    protected abstract T mapResultToOutType(Result var1);

    @Override
    public abstract void configure(Configuration var1);

    @Override
    public void open(TableInputSplit split) throws IOException {
        if (this.table == null) {
            throw new IOException("The HBase table has not been opened! This needs to be done in configure().");
        } else if (this.scan == null) {
            throw new IOException("Scan has not been initialized! This needs to be done in configure().");
        } else if (split == null) {
            throw new IOException("Input split is null!");
        } else {
            this.logSplitInfo("opening", split);
            this.currentRow = split.getStartRow();
            this.scan.setStartRow(this.currentRow);
            this.scan.setStopRow(split.getEndRow());
            this.resultScanner = this.table.getScanner(this.scan);
            this.endReached = false;
            this.scannedRows = 0L;
        }
    }

    @Override
    public T nextRecord(T reuse) throws IOException {
        if (this.resultScanner == null) {
            throw new IOException("No table result scanner provided!");
        } else {
            Result res;
            try {
                res = this.resultScanner.next();
            } catch (Exception var4) {
                this.resultScanner.close();
                LOG.warn("Error after scan of " + this.scannedRows + " rows. Retry with a new scanner...", var4);
                this.scan.withStartRow(this.currentRow, false);
                this.resultScanner = this.table.getScanner(this.scan);
                res = this.resultScanner.next();
            }

            if (res != null) {
                ++this.scannedRows;
                this.currentRow = res.getRow();
                return this.mapResultToOutType(res);
            } else {
                this.endReached = true;
                return null;
            }
        }
    }

    private void logSplitInfo(String action, TableInputSplit split) {
        int splitId = split.getSplitNumber();
        String splitStart = Bytes.toString(split.getStartRow());
        String splitEnd = Bytes.toString(split.getEndRow());
        String splitStartKey = splitStart.isEmpty() ? "-" : splitStart;
        String splitStopKey = splitEnd.isEmpty() ? "-" : splitEnd;
        String[] hostnames = split.getHostnames();
        LOG.info("{} split (this={})[{}|{}|{}|{}]", new Object[]{action, this, splitId, hostnames, splitStartKey, splitStopKey});
    }

    public boolean reachedEnd() throws IOException {
        return this.endReached;
    }

    public void close() throws IOException {
        LOG.info("Closing split (scanned {} rows)", this.scannedRows);
        this.currentRow = null;

        try {
            if (this.resultScanner != null) {
                this.resultScanner.close();
            }
        } finally {
            this.resultScanner = null;
        }

    }

    @Override
    public void closeInputFormat() throws IOException {
        try {
            if (this.table != null) {
                this.table.close();
            }
        } finally {
            this.table = null;
        }

    }

    @Override
    public TableInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (this.table == null) {
            throw new IOException("The HBase table has not been opened! This needs to be done in configure().");
        } else if (this.scan == null) {
            throw new IOException("Scan has not been initialized! This needs to be done in configure().");
        } else {
            Pair<byte[][], byte[][]> keys = this.table.getRegionLocator().getStartEndKeys();
            if (keys != null && keys.getFirst() != null && ((byte[][])keys.getFirst()).length != 0) {
                byte[] startRow = this.scan.getStartRow();
                byte[] stopRow = this.scan.getStopRow();
                boolean scanWithNoLowerBound = startRow.length == 0;
                boolean scanWithNoUpperBound = stopRow.length == 0;
                List<TableInputSplit> splits = new ArrayList(minNumSplits);

                for(int i = 0; i < ((byte[][])keys.getFirst()).length; ++i) {
                    byte[] startKey = ((byte[][])keys.getFirst())[i];
                    byte[] endKey = ((byte[][])keys.getSecond())[i];
                    String regionLocation = this.table.getRegionLocator().getRegionLocation(startKey, false).getHostnamePort();
                    if (this.includeRegionInScan(startKey, endKey)) {
                        String[] hosts = new String[]{regionLocation};
                        boolean isLastRegion = endKey.length == 0;
                        if ((scanWithNoLowerBound || isLastRegion || Bytes.compareTo(startRow, endKey) < 0) && (scanWithNoUpperBound || Bytes.compareTo(stopRow, startKey) > 0)) {
                            byte[] splitStart = !scanWithNoLowerBound && Bytes.compareTo(startKey, startRow) < 0 ? startRow : startKey;
                            byte[] splitStop = (scanWithNoUpperBound || Bytes.compareTo(endKey, stopRow) <= 0) && !isLastRegion ? endKey : stopRow;
                            int id = splits.size();
                            TableInputSplit split = new TableInputSplit(id, hosts, Bytes.toBytes(String.valueOf(table.getName())), splitStart, splitStop);
                            splits.add(split);
                        }
                    }
                }

                LOG.info("Created " + splits.size() + " splits");
                Iterator var18 = splits.iterator();

                while(var18.hasNext()) {
                    TableInputSplit split = (TableInputSplit)var18.next();
                    this.logSplitInfo("created", split);
                }

                return (TableInputSplit[])splits.toArray(new TableInputSplit[splits.size()]);
            } else {
                throw new IOException("Expecting at least one region.");
            }
        }
    }

    protected boolean includeRegionInScan(byte[] startKey, byte[] endKey) {
        return true;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(TableInputSplit[] inputSplits) {
        return new LocatableInputSplitAssigner(inputSplits);
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) {
        return null;
    }
}
