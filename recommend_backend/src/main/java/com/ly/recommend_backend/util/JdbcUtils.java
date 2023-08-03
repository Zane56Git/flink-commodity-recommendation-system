package com.ly.recommend_backend.util;



import com.ly.recommend_backend.entity.ConnEntiy;

import java.sql.Connection;
import java.sql.ResultSet;

public interface JdbcUtils {
    public Connection connection(ConnEntiy connEntiy);

    public void close(AutoCloseable... closes);

    public boolean insert(Connection connection, String sql, String... params);

    public boolean delete(Connection connection, String sql, String... params);

    public ResultSet QueryResultSet(Connection connection, String sql, String... params);
}
