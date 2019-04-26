package com.hainiu.qiaoChunYu.logETL;

import com.hainiu.util.IPParser;
import com.hainiu.util.base.BaseMR;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class LogETLTest extends BaseMR {

    public static Schema schema = null;

    public static Schema.Parser parser = new Schema.Parser();

    private static class LogETLMapper extends Mapper<LongWritable, Text, AvroKey<GenericRecord>, NullWritable> {

        IPParser ipParser = null;

        Logger logger = LoggerFactory.getLogger(LogETLTest.class);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            if (schema == null) {
                schema = parser.parse(LogETLTest.class.getResourceAsStream("/qcy_schema/log_schema.txt"));
            }

            logger.info("===========================");
//            logger.info(LogETLTest.class.getResource("/qqwry.dat").getPath());
            ipParser = new IPParser("hdfs:////ns1/user/qiaoChunYu/config/qqwry.dat");

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
                IPParser.RegionInfo regionInfo = null;
                try {
                    regionInfo = ipParser.analyseIp(logs[0] != null ? logs[0] : null);
                } catch (NullPointerException e) {
                    e.printStackTrace();
                }
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

            record.put("ip", ip == "" || ip == null ? "" : ip);
            record.put("time", time);
            record.put("request_info", request_info == "" || request_info == null ? "" : request_info);
            record.put("ref", ref == "" || ref == null ? "" : ref);
            record.put("userAgent", userAgent == "" || userAgent == null ? "" : userAgent);
            record.put("country", country == "" || country == null ? "" : country);
            record.put("provite", provite == "" || provite == null ? "" : provite);
            record.put("city", city == "" || city == null ? "" : city);
            record.put("year", year == "" || year == null ? "" : year);
            record.put("month", month == "" || month == null ? "" : month);
            record.put("day", day == "" || day == null ? "" : day);
            record.put("dayDate", dayDate == "" || dayDate == null ? "" : dayDate);

            context.write(new AvroKey<>(record), NullWritable.get());
        }
    }


    @Override
    public Job getJob() throws IOException {
        Configuration configuration = this.conf;
        Job job = Job.getInstance(configuration, getJobNameWithTaskId());

        job.setJarByClass(LogETLTest.class);
        job.setMapperClass(LogETLMapper.class);
        job.setNumReduceTasks(0);

        job.setMapOutputKeyClass(AvroKey.class);
        job.setMapOutputValueClass(NullWritable.class);

        schema = parser.parse(LogETLTest.class.getResourceAsStream("/qcy_schema/log_schema.txt"));

        AvroJob.setMapOutputKeySchema(job, schema);

        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        FileInputFormat.addInputPath(job, getFirstJobInputPath());
        FileOutputFormat.setOutputPath(job, getOutputPath(getJobNameWithTaskId()));

        return job;
    }

    @Override
    public String getJobName() {
        return "LogToAvro";
    }
}
