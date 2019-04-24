package com.hainiu;

import com.hainiu.util.IPParser;
import org.junit.Test;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("120.196.100.99");
        System.out.println(regionInfo.getProvince());
    }
}
