package org.ifmo.app;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesReducer extends Reducer<Text, SalesRecordWritable, Text, SalesRecordWritable> {
    
    private SalesRecordWritable result = new SalesRecordWritable();
    
    @Override
    protected void reduce(Text key, Iterable<SalesRecordWritable> values, Context context) 
            throws IOException, InterruptedException {
        
        double totalRevenue = 0.0;
        long totalQuantity = 0;
        
        for (SalesRecordWritable val : values) {
            totalRevenue += val.getRevenue();
            totalQuantity += val.getQuantity();
        }
        
        result = new SalesRecordWritable(totalRevenue, totalQuantity);
        context.write(key, result);
    }
}
