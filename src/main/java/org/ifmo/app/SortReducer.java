package org.ifmo.app;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SortReducer extends Reducer<DoubleWritable, Text, Text, Text> {
    
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) 
            throws IOException, InterruptedException {
        
        double revenue = key.get();
        
        for (Text val : values) {
            String[] parts = val.toString().split("\t");
            String category = parts[0];
            String quantity = parts[1];
            
            context.write(
                new Text(category),
                new Text(String.format("%.2f\t%s", revenue, quantity))
            );
        }
    }
}
