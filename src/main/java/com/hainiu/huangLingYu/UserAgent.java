package com.hainiu.huangLingYu;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import java.io.IOException;


public class UserAgent {

    static UASparser uasParser = null;

    static {

        try {

            uasParser = new UASparser(OnlineUpdater.getVendoredInputStream());

            // java.lang.UnsupportedClassVersionError:

            // cz/mallat/uasparser/UASparser : Unsupported major.minor version 51.0

            // 用jdk1.6测试时会报以上错，需要jdk1.7以上版本支持

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        String str = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.130 Safari/537.36";

        try {

            UserAgentInfo userAgentInfo = UserAgent.uasParser.parse(str);

            System.out.println("操作系统家族：" + userAgentInfo.getOsFamily());

            System.out.println("操作系统详细名称：" + userAgentInfo.getOsName());

            System.out.println("浏览器名称和版本:" + userAgentInfo.getUaName());

            System.out.println("类型：" + userAgentInfo.getType());

            System.out.println("浏览器名称：" + userAgentInfo.getUaFamily());

            System.out.println("浏览器版本：" + userAgentInfo.getBrowserVersionInfo());

            System.out.println("设备类型：" + userAgentInfo.getDeviceType());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}