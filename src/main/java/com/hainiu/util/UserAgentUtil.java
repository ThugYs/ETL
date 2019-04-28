package com.hainiu.util; /**
 * UserAgentUtil.java
 * com.hainiuxy.etl.util
 * Copyright (c) 2019, 海牛版权所有.
 * @author   潘牛                      
*/



import java.io.IOException;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;

/**
 * user_agent 解析工具
 * @author   潘牛                      
 * @Date	 2019年3月7日 	 
 */
public class UserAgentUtil {
	
	UASparser uasParser = null;
	
	/**
	 * 设置user_agent 解析对象
	 * 一个util只设置一次即可
	*/
	public void setUASparser(){
		try {
			uasParser = new UASparser(OnlineUpdater.getVendoredInputStream());
			// java.lang.UnsupportedClassVersionError:
			// cz/mallat/uasparser/UASparser : Unsupported major.minor version 51.0
			// 用jdk1.6测试时会报以上错，需要jdk1.7以上版本支持
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 解析user_agent 为 UserAgentInfo 对象，该对象中封装了所有解析出来的信息
	 * @param user_agent 
	 * @return 
	 * @throws IOException 
	*/
	public UserAgentInfo parse(String user_agent) throws IOException{
		UserAgentInfo userAgentInfo = uasParser.parse(user_agent);
		return userAgentInfo;
	}
	
	public static void main(String[] args) {
		String str = "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.130 Safari/537.36";
		try {
			UserAgentUtil util = new UserAgentUtil();
			util.setUASparser();
			UserAgentInfo userAgentInfo = util.parse(str);
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

