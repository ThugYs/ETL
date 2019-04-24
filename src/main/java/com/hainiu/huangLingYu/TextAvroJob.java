package com.hainiu.huangLingYu;

import com.hainiu.util.JobRunResult;
import com.hainiu.util.JobRunUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TextAvroJob extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
//		1）获取配置对象
        Configuration conf = getConf();
//		2）创建任务链对象 JobControl
        JobControl jobc = new JobControl("TextAvroJob");
//		3）创建任务链中要添加的任务对象  ControlledJob
        TextAvro avro = new TextAvro();
        avro.setConf(conf);
        ControlledJob orcCJob = avro.getControlledJob();
//		5）将任务添加到任务链中
        jobc.addJob(orcCJob);

        JobRunResult result = JobRunUtil.run(jobc);
        result.print(true);
        return 0;
    }
    public static void main(String[] args) throws Exception {
//		 -Dtask.id=TextAvroJob -Dtask.input.dir=/tmp/etl/input  -Dtask.base.dir=/tmp/etl/output
        System.exit(ToolRunner.run(new TextAvroJob(), args));
    }
}
