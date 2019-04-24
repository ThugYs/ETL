package com.hainiu.huangLingYu;

import com.hainiu.huangLingYu.util.LogParser;
import com.hainiu.util.base.BaseMR;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Map;


public class TextAvro extends BaseMR {
    public static Schema schema = null;
    public static Schema.Parser parser = new Schema.Parser();

    public static class TextAvroMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if(schema == null){
                schema = parser.parse(TextAvro.class.getResourceAsStream("/avro_schema.txt"));
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String lineValue = value.toString();

            LogParser logParser = new LogParser();
            Map<String, String> valueOut = logParser.parse2(lineValue);

            GenericRecord genericRecord = new GenericData.Record(schema);
//            SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");

            genericRecord.put("id",valueOut.get("id") == null ? "":valueOut.get("id"));
            genericRecord.put("country",valueOut.get("country") == null ? "":valueOut.get("country"));
            genericRecord.put("ref",valueOut.get("ref") == null ? "":valueOut.get("ref"));
            genericRecord.put("browser",valueOut.get("browser") == null ? "":valueOut.get("browser"));
            genericRecord.put("type",valueOut.get("type") == null ? "":valueOut.get("type"));
            genericRecord.put("version",valueOut.get("version") == null ? "":valueOut.get("version"));

            context.write(new AvroKey<GenericRecord>(genericRecord) , NullWritable.get());
        }
    }


    @Override
    public Job getJob() throws IOException {
        Job job = Job.getInstance(conf, getJobNameWithTaskId());
        job.setJarByClass(TextAvro.class);
        job.setMapperClass(TextAvroMapper.class);
        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);

        //输入和输出
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        //把schema文件格式解析到schema对象中
        schema = parser.parse(TextAvro.class.getResourceAsStream("/avro_schema.txt"));
        AvroJob.setMapOutputKeySchema(job, schema);

        //一个region的输入目录
       FileInputFormat.addInputPath(job, getFirstJobInputPath());
        //多个目录输入
//        FileSystem fs = FileSystem.get(conf);
//        FileStatus[] listStatus = fs.listStatus(getFirstJobInputPath());
//        StringBuilder sb = new StringBuilder();
//        for(FileStatus fileStatus : listStatus){
//            String path = fileStatus.getPath().toString();
//            if(path.contains(".")){
//                System.out.println(path);
//                continue;
//            }
//            sb.append(path).append("/cf,");
//
//        }
//        sb.deleteCharAt(sb.length() - 1);
//        System.out.println("-------------");
//        System.out.println("i nputpaths:" + sb.toString());
//        FileInputFormat.addInputPaths(job, sb.toString());


        // 设置输出目录
        Path outputDir = getOutputPath(getJobNameWithTaskId());
        FileOutputFormat.setOutputPath(job, outputDir);
        return job;
    }

    @Override
    public String getJobName() {
        return "TextAvro";
    }
}
