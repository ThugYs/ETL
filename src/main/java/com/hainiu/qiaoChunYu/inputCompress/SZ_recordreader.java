package com.hainiu.qiaoChunYu.inputCompress;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class SZ_recordreader implements RecordReader<Text, Text> {
    FileSplit split;
    JobConf job_run;
    boolean processed = false;

    CompressionCodecFactory compressioncodec = null;   // A factory that will find the correct codec(.file) for a given filename.

    public SZ_recordreader(JobConf job_run, FileSplit split) {
        this.split = split;
        this.job_run = job_run;
    }

    @Override
    public void close() throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public Text createKey() {
        // TODO Auto-generated method stub
        return new Text();
    }

    @Override
    public Text createValue() {
        // TODO Auto-generated method stub
        return new Text();
    }

    @Override
    public long getPos() throws IOException {
        // TODO Auto-generated method stub
        return processed ? split.getLength() : 0;
    }

    @Override
    public float getProgress() throws IOException {
        // TODO Auto-generated method stub
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public boolean next(Text key, Text value) throws IOException {
        // TODO Auto-generated method stub
        FSDataInputStream in = null;
        if (!processed) {
            byte[] bytestream = new byte[(int) split.getLength()];
            Path path = split.getPath();
            compressioncodec = new CompressionCodecFactory(job_run);

            CompressionCodec code = compressioncodec.getCodec(path);
            // compressioncodec will find the correct codec by visiting the path of the file and store the result in code
            System.out.println(code);

            FileSystem fs = path.getFileSystem(job_run);

            try {
                in = fs.open(path);
                IOUtils.readFully(in, bytestream, 0, bytestream.length);
                System.out.println("the input is " + in + in.toString());
                key.set(path.getName());
                value.set(bytestream, 0, bytestream.length);
            } finally {
                IOUtils.closeStream(in);
            }

            processed = true;

            return true;


        }
        return false;
    }

}