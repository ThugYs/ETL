package com.hainiu.huangLingYu;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFile.Reader;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/*
 *  读取Hfile文件的inputformat和recordreader
 */
//ImmutableBytesWritable 行的rowkey  cell
public class HFileInputFormat extends FileInputFormat<ImmutableBytesWritable, Cell> {

    @Override
    public RecordReader<ImmutableBytesWritable, Cell> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        //返回读取hfile文件的recordreader
        return new HFileRecordReader((FileSplit)split,context);
    }
    public static class HFileRecordReader extends RecordReader<ImmutableBytesWritable, Cell>{
       //读取hfile文件的reader
        Reader reader = null;
        //扫描器对象
        HFileScanner scanner =null;
        //统计计数
        long count=0;

        public HFileRecordReader(FileSplit fileSplit,TaskAttemptContext context) throws  IOException{
          //获取对象
            Configuration conf =context.getConfiguration();
          //获取文件系统
            FileSystem fs = FileSystem.get(conf);
            Path path =fileSplit.getPath();
            CacheConfig cacheConf=new CacheConfig(conf);
            //初始化hbase的recordreader
            reader=HFile.createReader(fs,path,cacheConf,conf);
            //获取读取hfile文件的扫描器  不缓存    不随机读写
            scanner=reader.getScanner(false,false);
            //把扫描器调到首行
            scanner.seekTo();
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
           //如果是首行 直接返回true
            if(count == 0){
               count++;
               return true;
           }
            //如果不是首行，执行next找下一个有没有，同时扫描器下移
            // 当调用该方法时，扫描器下移
           boolean hasNext=this.scanner.next();
            if(hasNext){
                count++;
            }
           return hasNext;
        }

        @Override
        public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
          Cell cell=this.scanner.getKeyValue();
          ImmutableBytesWritable key=new ImmutableBytesWritable();
          key.set(CellUtil.cloneRow(cell));
          return  key;
        }

        @Override
        public Cell getCurrentValue() throws IOException, InterruptedException {
            return  this.scanner.getKeyValue();
        }
       //获取一个进度
        @Override
        public float getProgress() throws IOException, InterruptedException {
           return (float)count /this.reader.getEntries();
        }

        @Override
        public void close() throws IOException {
         if(this.reader !=null){
             this.reader.close();
          }
        }
    }
}
