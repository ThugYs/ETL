package com.hainiu.driver;

import org.apache.hadoop.util.ProgramDriver;

public class GlobaDiver {

    public static void main(String[] args) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();
        try {
//            programDriver.addClass("AvroToHFile" , AvroToHFileJob.class , "Avro文件转hfile文件");

            exitCode = programDriver.run(args);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }
        System.exit(exitCode);
    }
}
