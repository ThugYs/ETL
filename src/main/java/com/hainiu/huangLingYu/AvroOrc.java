package com.hainiu.huangLingYu;

import com.hainiu.util.base.BaseMR;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcNewOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AvroOrc extends BaseMR {
        public static Schema schema = null;

        public static Schema.Parser parser = new Schema.Parser();

    public  static class AvroOrcMapper extends Mapper<AvroKey<GenericRecord>, NullWritable, NullWritable, Writable>{
         StructObjectInspector inspector = null;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String type ="struct<id:string,country:string,ref:string,browser:string,type:string,version:string>";
            TypeInfo info = TypeInfoUtils.getTypeInfoFromTypeString(type);
            inspector = (StructObjectInspector) TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(info);
        }

        @Override
        protected void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            GenericRecord datum = key.datum();
            String id = (String)datum.get("id");
            String country = (String)datum.get("country");
            String ref = (String)datum.get("ref");
            String browser = (String)datum.get("browser");
            String type = (String)datum.get("type");
            String version = (String)datum.get("version");

            OrcSerde orcSerde = new OrcSerde();
            List<Object> realRow= new ArrayList<Object>();
            realRow.add(id);
            realRow.add(country);
            realRow.add(ref);
            realRow.add(browser);
            realRow.add(type);
            realRow.add(version);

            Writable w = orcSerde.serialize(realRow,inspector);
            context.write(NullWritable.get(),w);
        }
    }

    @Override
    public Job getJob() throws IOException {
        Job job =Job.getInstance(conf,getJobNameWithTaskId());
        job.setJarByClass(AvroOrc.class);
        job.setMapperClass(AvroOrcMapper.class);
        job.setNumReduceTasks(0);
        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Writable.class);
        job.setOutputFormatClass(OrcNewOutputFormat.class);

        schema = parser.parse(AvroOrc.class.getResourceAsStream("/avro_schema.txt"));
        AvroJob.setInputKeySchema(job, schema);
        FileInputFormat.addInputPath(job,getFirstJobInputPath());
        Path outputDir = getOutputPath(getJobNameWithTaskId());
        FileOutputFormat.setOutputPath(job, outputDir);

        return job;
    }

    @Override
    public String getJobName() {
        return "AvroOrc";
    }
}
