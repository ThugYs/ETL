package com.hainiu.qiaoChunYu.logETL;

import com.hainiu.util.JobRunResult;
import com.hainiu.util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LogETLTestJob extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = getConf();
        JobControl jobControl = new JobControl("LogETLTestJob");
        LogETLTest logETLTest = new LogETLTest();
        logETLTest.setConf(configuration);

        ControlledJob controlledJob = logETLTest.getControlledJob();

        jobControl.addJob(controlledJob);
        JobRunResult result = JobRunUtil.run(jobControl);
        result.print(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new LogETLTestJob() , args));
    }

}
