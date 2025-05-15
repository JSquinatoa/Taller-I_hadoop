package com.example;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CSVRowMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text outputKey = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split(","); // Split the line by comma

        // Assuming you want to count the occurrences of the value in the second column (index 1)
        if (fields.length > 1) {
            String valueToCount = fields[1].trim();
            outputKey.set(valueToCount);
            context.write(outputKey, one);
        }
    }
}