package com.ly.recommend_backend.util;


import com.ly.recommend_backend.entity.ConnEntiy;
import lombok.extern.slf4j.Slf4j;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.*;
import java.util.ArrayList;

/**
 * 使用官方jdbc驱动包
 * https://blog.csdn.net/m0_43405302/article/details/124528676
 */
@Slf4j
public class ClickHouseJdbcUtils implements JdbcUtils {

    @Override
    public Connection connection(ConnEntiy connEntiy) {
        Connection conn = null;
        try {
            //Class.forName(connEntiy.getDriverName());
            conn = DriverManager.getConnection(connEntiy.getUrl());
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("connection fail ,please check your entities");
        }
        return conn;
    }

    public Connection connection2(ConnEntiy connEntiy) {
        Connection conn = null;
        try {
            Class.forName(connEntiy.getDriverName());
            ClickHouseProperties clickHouseProperties = new ClickHouseProperties();
            clickHouseProperties.setSocketTimeout(60000);
            clickHouseProperties.setSsl(false);
            clickHouseProperties.setSslMode("none");
            ClickHouseDataSource clickHouseDataSource = new ClickHouseDataSource(connEntiy.getUrl(), clickHouseProperties);
            conn = clickHouseDataSource.getConnection(connEntiy.getUser(), connEntiy.getPassword());

        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("connection fail ,please check your entities");
        }
        return conn;
    }

    @Override
    public void close(AutoCloseable... closes) {
        for (AutoCloseable close : closes) {
            if (close != null) {
                try {
                    close.close();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    close = null;
                }
            }
        }
    }

    @Override
    public boolean insert(Connection connection, String sql, String... params) {
        boolean b = false;
        ClickHousePreparedStatement pst = null;
        if (connection == null) {
            System.out.println("connection is empty");
            System.exit(-1);
        }
        try {
            pst = (ClickHousePreparedStatement) connection.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pst.setObject(i + 1, params[i]);
            }
            b = pst.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            close(pst, connection);
        }

        return b;
    }

    @Override
    public boolean delete(Connection connection, String sql, String... params) {
        boolean b = false;
        ClickHousePreparedStatement pst = null;
        if (connection == null) {
            System.out.println("connection is empty");
            System.exit(-1);
        }
        try {
            pst = (ClickHousePreparedStatement) connection.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pst.setObject(i + 1, params[i]);
            }
            b = pst.execute();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            close(pst, connection);
        }

        return b;
    }

    @Override
    public ResultSet queryResultSet(Connection connection, String sql, String... params) {
        ResultSet rst = null;
        ClickHousePreparedStatement pst = null;
        if (connection == null) {
            System.out.println("connection is empty");
            System.exit(-1);
        }
        try {
            pst = (ClickHousePreparedStatement) connection.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pst.setObject(i + 1, params[i]);
            }
            rst = pst.executeQuery();

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            close(rst, pst, connection);
        }
        return rst;
    }

    @Override
    public ArrayList<ArrayList<String>> queryResultList(Connection connection, String sql, String... params) {
        ArrayList<ArrayList<String>> resultArrayList = new ArrayList<ArrayList<String>>();
        ResultSet rst = null;
        ClickHousePreparedStatement pst = null;
        if (connection == null) {
            System.out.println("connection is empty");
            System.exit(-1);
        }
        try {
            pst = (ClickHousePreparedStatement) connection.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pst.setObject(i + 1, params[i]);
            }
            rst = pst.executeQuery();
            resultArrayList=rstToList(rst);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } finally {
            close(rst, pst, connection);
        }
        return resultArrayList;
    }

    private ArrayList<ArrayList<String>> rstToList(ResultSet resultSet) {
        ArrayList<ArrayList<String>> resultArrayList = new ArrayList<ArrayList<String>>();
        Connection connection = null;
        Statement statement = null;
        try {
            if (null != resultSet && null != resultSet.getMetaData()) {
                int columnCount = resultSet.getMetaData().getColumnCount();
                ArrayList<String> rowResultArray = new ArrayList<String>();
                for (int j = 1; j <= columnCount; j++) {
                    rowResultArray.add(resultSet.getMetaData().getColumnName(j));
                }
                resultArrayList.add(rowResultArray);
                while (resultSet.next()) {
                    rowResultArray = new ArrayList<String>();
                    for (int j = 1; j <= columnCount; j++) {
                        rowResultArray.add(resultSet.getString(j));
                    }
                    if (rowResultArray.size() != 0) {
                        resultArrayList.add(rowResultArray);
                    }
                }
            }
        } catch (Exception e) {
            log.warn(e.toString());
        }
        return resultArrayList;
    }
}

