package com.ly.recommend_backend.util;


import com.ly.recommend_backend.entity.ConnEntiy;
import ru.yandex.clickhouse.ClickHouseDataSource;
import ru.yandex.clickhouse.ClickHousePreparedStatement;
import ru.yandex.clickhouse.settings.ClickHouseProperties;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 使用官方jdbc驱动包
 * https://blog.csdn.net/m0_43405302/article/details/124528676
 */
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
    public ResultSet QueryResultSet(Connection connection, String sql, String... params) {
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
}

