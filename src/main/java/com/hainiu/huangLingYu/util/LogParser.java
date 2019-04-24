package com.hainiu.huangLingYu.util;

import com.hainiu.util.IPParser;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class LogParser {

    private Logger logger = LoggerFactory.getLogger(LogParser.class);

    public Map<String, String> parse2(String log)  {

        Map<String, String> logInfo = new HashMap<String,String>();
        IPParser ipParse = IPParser.getInstance();
        if(StringUtils.isNotBlank(log)) {
            String[] splits = log.split("\001");

//curl -s -o /dev/null -H "User-Agent:${user_agent}" -e "${ref}" "http://nn1.hadoop?id=${md5}&country=${country}
//192.168.217.210-[23/Apr/2019:23:59:31 +0800]"GET /?id=b026324c6904b2a9cb4b88d6d61c81d1&country=CN HTTP/1.1"200555"www.baidu.com" "User-Agent:Mozilla/5.0(iPad;U;CPUOS4_3_3likeMacOSX;en-us)AppleWebKit/533.17.9(KHTML,likeGecko)Version/5.0.2Mobile/8J2Safari/6533.18.5""-"
            String date = splits[2];
            String idCountry=splits[3];
            String[] arr = idCountry.split("&");
            String id = arr[0].split("=")[1];
            String country = arr[1].split("=")[1];
            String ref =splits[7];
            String useAgent=splits[8];
            String[] arr2=useAgent.split("/");
            //浏览器
            String browser=arr2[0].split(":")[1];
            String type=arr2[1];
            String version=arr2[2];

            logInfo.put("date",date);
            logInfo.put("idCountry",idCountry);
            logInfo.put("id",id);
            logInfo.put("country",country);

            logInfo.put("ref",ref);
            logInfo.put("useAgent",useAgent);
            logInfo.put("browser",browser);
            logInfo.put("type",type);
            logInfo.put("version",version);


        } else{
            logger.error("日志记录的格式不正确：" + log);
        }

        return logInfo;
    }



}