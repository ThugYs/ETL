 /**
 * JobRunUtil.java
 * com.hainiuxy.mapreduce.mrrun.util
 * Copyright (c) 2019, 海牛版权所有.
 * @author   潘牛                      
*/

package com.hainiu.util;

import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.FutureTask;

/**
 * 运行任务链和监控任务链完成情况，并返回任务链的运行结果
 * @author   潘牛                      
 * @Date	 2019年3月30日 	 
 */
public class JobRunUtil {
	
	public static JobRunResult run(JobControl jobc) throws Exception{
		
		// 运行任务链
		new Thread(jobc).start();
		
		MonitorJobCAndReturnResultCallable callable = new  MonitorJobCAndReturnResultCallable(jobc);
		
		FutureTask<JobRunResult> future = new FutureTask<>(callable);
		
		new Thread(future).start();
		// 阻塞的方法，等待线程完成，返回结果
		return future.get();
	}
	
	
	public static class MonitorJobCAndReturnResultCallable implements Callable<JobRunResult>{
		private JobControl jobc;
		
		public MonitorJobCAndReturnResultCallable(JobControl jobc){
			this.jobc = jobc;
		}
		
		@Override
		public JobRunResult call() throws Exception {
			long startTime = System.currentTimeMillis();
			//循环判断任务链任务是否都完成，如果没完成，就睡眠等待再判断
			while(! jobc.allFinished()){
				
				try {
					Thread.sleep(200L);
				} catch (InterruptedException e) {
					e.printStackTrace();
					
				}
			}
			//任务链的任务都完成了
			
			long endTime = System.currentTimeMillis();
			
			
			JobRunResult result = new JobRunResult();
			
			long runTime = endTime - startTime;
			
			result.setRunTime(formatRunTime(runTime));
			
			// 获取失败任务列表
			List<ControlledJob> failedJobList = jobc.getFailedJobList();
			if(failedJobList.isEmpty()){
				result.setSuccessed(true);
			}else{
				result.setSuccessed(false);
				
				for(ControlledJob cjob : failedJobList){
					// 存失败列表
					result.setfailedJobName(cjob.getJobName());
				}
			}
			
			List<ControlledJob> successfulJobList = jobc.getSuccessfulJobList();
			for(ControlledJob cjob : successfulJobList){

				result.setCounters(cjob.getJobName(), cjob.getJob().getCounters());
			}
			
			//停止任务链运行
			jobc.stop();
			
			return result;
			
		}

		/**
		 * 将long类型的毫秒数 转化成 格式： x天x小时x分x秒
		 * @param runTime 毫秒数
		 * @return String
		*/
		private String formatRunTime(long runTime) {
			StringBuilder sb = new StringBuilder();
			
			long days = runTime / (1000 * 60 * 60 * 24);
			
			long hours = runTime % (1000 * 60 * 60 * 24) / (1000 * 60 * 60);
			
			long minutes = runTime % (1000 * 60 * 60 * 24) % (1000 * 60 * 60) / (1000 * 60);
			
			long seconds = runTime % (1000 * 60 * 60 * 24) % (1000 * 60 * 60) % (1000 * 60) / 1000;
			
			
			if(days != 0){
				sb.append(days).append("天");
			}
			if(hours != 0){
				sb.append(hours).append("小时");
			}
			if(minutes != 0){
				sb.append(minutes).append("分");
			}
			if(seconds != 0){
				sb.append(seconds).append("秒");
			}

			return sb.toString();
			
		}
		
		
		
	}

}

