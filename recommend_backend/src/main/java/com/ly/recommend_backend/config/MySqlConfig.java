package com.ly.recommend_backend.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class MySqlConfig implements Serializable {
    private String url;
    private String username;
    private String password;

}
