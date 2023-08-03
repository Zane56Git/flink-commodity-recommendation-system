package com.ly.recommend_backend.entity;

/**
 * 连接实体类
 */
public class ConnEntiy {
    // 驱动器
    String driverName;
    // 连接对象地址
    String url;
    // 用户
    String user;
    // 密码
    String password;

    public ConnEntiy() {
    }

    public ConnEntiy(String driverName, String url, String user, String password) {
        this.driverName = driverName;
        this.url = url;
        this.user = user;
        this.password = password;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
