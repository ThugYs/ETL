/**
 * JobRunResult.java
 * com.hainiuxy.mapreduce.mrrun.util
 * Copyright (c) 2019, 海牛版权所有.
 * @author   潘牛                      
*/

package com.hainiu.util;

import org.apache.hadoop.mapreduce.Counters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * 任务链运行结果类，存运行结果信息
 * @author   潘牛                      
 * @Date	 2019年3月30日 	 
 */
public class JobRunResult {
	
	/**
	 * 任务链是否都运行成功
	 */
	private boolean isSuccessed;
	
	
	/**
	 * 运行时长： 格式： x天x小时x分x秒
	 */
	private String runTime;
	
	
	/**
	 * 失败的任务名称列表
	 */
	private List<String> failedJobNames = new ArrayList<String>();
	
	
	
	/**
	 * 每个任务的counters
	 */
	private Map<String, Counters> countersMap = new  HashMap<String, Counters>();
	
	
	/**
	 * 打印任务链结果信息
	 * @param isPrintCounters 是否打印counter， true:打印；false:不打印
	*/
	public void print(boolean isPrintCounters){
		StringBuilder sb = new StringBuilder();
		if(this.isSuccessed){
			sb.append("任务链所有任务运行成功\n");
		}else{
			sb.append("任务链部分任务运行失败，失败任务名称如下：\n");
			for(String failedJobName : failedJobNames){
				sb.append(failedJobName).append("\n");
			}
		}
		sb.append("任务链运行时长：").append(this.runTime).append("\n");
		
		
		if(isPrintCounters){
			for(Entry<String,Counters> entry : countersMap.entrySet()){
				String jobName = entry.getKey();
				Counters counters = entry.getValue();
				sb.append("jobName:").append(jobName)
				.append("\n")
				.append(counters.toString())
				.append("\n--------------\n");
				
			}
		}
		
		
		System.out.println(sb.toString());
	}



	public boolean isSuccessed() {
		return isSuccessed;
	}



	public void setSuccessed(boolean isSuccessed) {
		this.isSuccessed = isSuccessed;
	}



	public String getRunTime() {
		return runTime;
	}



	public void setRunTime(String runTime) {
		this.runTime = runTime;
	}



	public List<String> getFailedJobNames() {
		return failedJobNames;
	}



	public void setfailedJobName(String jobName) {
		this.failedJobNames.add(jobName);
	}



	public Counters getCounters(String jobName) {
		return countersMap.get(jobName);
	}



	public void setCounters(String jobName, Counters counters) {
		this.countersMap.put(jobName, counters);
	}
	
	
	

}

