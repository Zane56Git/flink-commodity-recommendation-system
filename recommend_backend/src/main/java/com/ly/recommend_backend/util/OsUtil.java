package com.ly.recommend_backend.util;

/**
 * @description:
 * @author:Zane
 * @createTime:2021/9/9 23:32
 * @version:1.0
 */
public class OsUtil {
    public static boolean isWindows() {
        return System.getProperties().getProperty("os.name").toUpperCase().indexOf("WINDOWS") != -1;
    }
}
