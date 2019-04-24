package com.hainiu.qiaoChunYu.logETL;

import com.hainiu.util.IPParser;
import com.hainiu.util.base.BaseMR;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class LogETLTest extends BaseMR {

    private static class LogETLMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable> {

        public static Schema schema = null;

        public static Schema.Parser parser = new Schema.Parser();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if (schema == null) {
                schema = parser.parse(LogETLTest.class.getResourceAsStream("/qcy_schema/log_schema.txt"));
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] logs = value.toString().split("\001");
            // 数据源 Start
            String ip = "";
            long time = -1;
            String request_info = "";
            String ref = "";
            String userAgent = "";
            String country = "";
            String provite = "";
            String city = "";
            String year = "";
            String month = "";
            String day = "";
            String dayDate = "";
            // 数据源 end

            if (logs.length != 0 && logs[0] != null) {
                ip = logs[0];
                IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp(logs[0] != null ? logs[0] : null);
                if (regionInfo != null) {
                    country = regionInfo.getCountry();
                    provite = regionInfo.getProvince();
                    city = regionInfo.getCity();
                }
            }
            try {
                if (logs.length != 0 && logs[2] != null) {
                    String dateStirng = logs[2];
                    SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z", new Locale("ENGLISH", "CHINA"));
                    Date date = null;
                    try {
                        date = dateFormat.parse(dateStirng);
                    } catch (ParseException e) {
                        e.printStackTrace();
                    }
                    if (date != null) {
                        time = date.getTime();
                        dateFormat = new SimpleDateFormat("YYYY-MM-dd");
                        String dateTmp = dateFormat.format(date);
                        year = dateTmp.split("-")[0];
                        month = dateTmp.split("-")[1];
                        day = dateTmp.split("-")[2];
                        dateFormat = new SimpleDateFormat("HH:mm:ss");
                        dayDate = dateFormat.format(date);
                    }
                }
                if (logs.length != 0 && logs[8] != null) {
                    userAgent = logs[8];
                }
                if (logs.length != 0 && logs[7] != null) {
                    ref = logs[7];
                }
                if (logs.length != 0 && logs[3] != null) {
                    request_info = logs[3];
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }
            GenericRecord record = new GenericData.Record(schema);

            record.put("ip", ip);
            record.put("time", time);
            record.put("request_info", request_info);
            record.put("ref", ref);
            record.put("userAgent", userAgent);
            record.put("country", country);
            record.put("provite", provite);
            record.put("city", city);
            record.put("year", year);
            record.put("month", month);
            record.put("day", day);
            record.put("dayDate", dayDate);

            context.write(new AvroKey<>(record) , NullWritable.get());
        }
    }


    @Override
    public Job getJob() throws IOException {
        Configuration configuration = this.conf;
        Job job = Job.getInstance(configuration, getJobNameWithTaskId());

        job.setJarByClass(LogETLTest.class);
        job.setMapperClass(LogETLMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job, getOutputPath(getJobNameWithTaskId()));

        return job;
    }

    @Override
    public String getJobName() {
        return null;
    }
}
