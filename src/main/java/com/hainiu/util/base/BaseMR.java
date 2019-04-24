/**
 * BaseMR.java
 * com.hainiuxy.mapreduce.mrrun.base
 * Copyright (c) 2019, 海牛版权所有.
 * @author   潘牛                      
*/

package com.hainiu.util.base;


import com.hainiu.util.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;

import java.io.IOException;


/**
 * 任务链公共基类
 * @author   潘牛                      
 * @Date	 2019年3月29日 	 
 */
public abstract class BaseMR {
	public static Configuration conf = null;


	public static void setConf(Configuration conf) {
		BaseMR.conf = conf;
	}


	public ControlledJob getControlledJob() throws IOException{

		//	创建任务链中的ControlledJob对象
		ControlledJob cjob = new ControlledJob(conf);

		Job job = getJob();

		// 自动删除输出目录
		FileSystem fs = FileSystem.get(conf);
		Path outputDir = getOutputPath(getJobNameWithTaskId());
		if (fs.exists(outputDir)){
			fs.delete(outputDir, true);
			System.out.println("delete output dir: " + outputDir.toString() + " success!");
		}
		
		//	将任务对象和 ControlledJob 进行关联
		cjob.setJob(job);
		
		return cjob;
	}
	
	
	/**
	 * 获取job对象，由子类去实现
	 * @return job对象
	 * @throws 
	*/
	public abstract Job getJob() throws IOException;
	
	
	/**
	 * 获取最基础的任务名称，由子类去实现
	 * @return 任务名
	*/
	public abstract String getJobName();
	
	
	/**
	 * 获取个性化的任务名称
	 * wordcount_0329_lufei
	*/
	public String getJobNameWithTaskId(){
		return getJobName() + "_" + conf.get(Constants.TASK_ID_ATTR);
	}
	
	/**
	 * 获取首个任务的输入目录
	 * @return path对象
	*/
	public Path getFirstJobInputPath(){
		return new Path(conf.get(Constants.TASK_INPUT_DIR_ATTR));
	}
	
	/**
	 * 获取任务的输出目录
	 * @param jobName
	 * @return 任务的输出目录path对象
	*/
	public Path getOutputPath(String jobName){
//		/tmp/mr/task/wordcount_0329_lufei
		String basePath = conf.get(Constants.TASK_BASE_DIR_ATTR);
		return new Path(basePath + "/" + jobName);
	}
}

