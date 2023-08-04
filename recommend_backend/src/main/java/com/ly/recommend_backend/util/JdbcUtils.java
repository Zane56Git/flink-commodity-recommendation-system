package com.ly.recommend_backend.util;



import com.ly.recommend_backend.entity.ConnEntiy;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;

public interface JdbcUtils {
    public Connection connection(ConnEntiy connEntiy);

    public void close(AutoCloseable... closes);

    public boolean insert(Connection connection, String sql, String... params);

    public boolean delete(Connection connection, String sql, String... params);

    public ResultSet queryResultSet(Connection connection, String sql, String... params);

    public ArrayList<ArrayList<String>> queryResultList(Connection connection, String sql, String... params);


}
