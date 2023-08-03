package com.ly.recommend_backend.config;

import lombok.Data;

import java.io.Serializable;

@Data
public class ClickhouseConfig implements Serializable {
    private String url;
    private String username;
    private String password;


}
