package com.hainiu.driver;

import com.hainiu.huangLingYu.AvroOrcJob;
import com.hainiu.huangLingYu.TextAvroJob;
import com.hainiu.qiaoChunYu.logETL.LogETLTestJob;
import org.apache.hadoop.util.ProgramDriver;

public class GlobaDiver {

    public static void main(String[] args) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();
        try {
            programDriver.addClass("TextAvro" , TextAvroJob.class , "hfile文件转Avro");
            programDriver.addClass("AvroOrc" , AvroOrcJob.class , "Avro转Orc");

            programDriver.addClass("LogToAvro" , LogETLTestJob.class , "Log日志转Avro");

            exitCode = programDriver.run(args);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        System.exit(exitCode);
    }
}
