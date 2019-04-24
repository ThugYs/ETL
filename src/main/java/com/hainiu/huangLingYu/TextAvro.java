package com.hainiu.huangLingYu;

import com.hainiu.huangLingYu.util.LogParser;
import com.hainiu.util.base.BaseMR;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
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
            SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");

//            genericRecord.put("uptime", sdf.format((valueOut.get("uptime"))));
//          genericRecord.put("time",Long.parseLong(valueOut.get("time")));
//            genericRecord.put("idCountry",valueOut.get("idCountry"));
            genericRecord.put("id",valueOut.get("id"));
            genericRecord.put("country",valueOut.get("country"));
            genericRecord.put("ref",valueOut.get("ref"));
            genericRecord.put("browser",valueOut.get("browser"));
            genericRecord.put("type",valueOut.get("type"));
            genericRecord.put("version",valueOut.get("version"));

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
