package org.ifmo.app;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    
    private DoubleWritable revenueKey = new DoubleWritable();
    private Text outputValue = new Text();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString();
        String[] parts = line.split("\t");
        
        if (parts.length >= 3) {
            try {
                String category = parts[0];
                double revenue = Double.parseDouble(parts[1]);
                String quantity = parts[2];
                
                revenueKey.set(revenue);
                outputValue.set(category + "\t" + quantity);
                
                context.write(revenueKey, outputValue);
            } catch (NumberFormatException e) {
                System.err.println("Invalid revenue value: " + line);
            }
        }
    }
}
