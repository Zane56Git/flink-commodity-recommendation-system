package com.ly.recommend_backend.config;

import lombok.Data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
public class RedisConfig implements Serializable {
    private String host;
    private List<String> hosts = new ArrayList<>();

    private String password;
    private Integer expireSecond;
    private Integer connectionTimeout;
    private Integer soTimeout;
    private Integer maxAttempts;
    private Integer poolMaxTotal;
    private Integer poolMaxIdle;
    private Integer poolMinIdle;
    private String response;


}
