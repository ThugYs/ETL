package com.hainiu;

import com.hainiu.util.IPUtil;
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
        IPUtil ipUtil = new IPUtil();
        System.out.println(ipUtil.getIpArea("103.219.186.27"));
    }
}
