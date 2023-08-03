package com.ly.recommend_backend.util;


import com.ly.recommend_backend.config.ProgramConfig;
import lombok.Data;

/**
 * 单例模式
 */
@Data
public class ConfigSingletonUtil {

    private static volatile ConfigSingletonUtil cacheSingletonUtil;

    private static ProgramConfig programConfig;

    private ConfigSingletonUtil() {
        programConfig = new ProgramConfig();
    }

    /*
     * 单例模式有两种类型
     * 懒汉式：在真正需要使用对象时才去创建该单例类对象
     * 饿汉式：在类加载时已经创建好该单例对象，等待被程序使用
     */

    // 懒汉式单例模式
    public static ConfigSingletonUtil getInstance() {
        if (cacheSingletonUtil == null) {// 线程A和线程B同时看到cacheSingletonUtil = null，如果不为null，则直接返回cacheSingletonUtil
            synchronized (ConfigSingletonUtil.class) {// 线程A或线程B获得该锁进行初始化
                if (cacheSingletonUtil == null) {// 其中一个线程进入该分支，另外一个线程则不会进入该分支
                    cacheSingletonUtil = new ConfigSingletonUtil();
                }
            }
        }
        return cacheSingletonUtil;
    }


    public void setConfig(ProgramConfig config) {
        programConfig = config;
    }

    public ProgramConfig getConfig() {
        return programConfig;
    }


}
