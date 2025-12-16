package org.ifmo.app;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SalesMapper extends Mapper<LongWritable, Text, Text, SalesRecordWritable> {
    
    private Text category = new Text();
    private SalesRecordWritable record = new SalesRecordWritable();
    
    public static enum Counter {
        BAD_RECORDS,
        HEADER_SKIPPED
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) 
            throws IOException, InterruptedException {
        
        String line = value.toString();
        
        if (key.get() == 0 && line.startsWith("transaction_id")) {
            context.getCounter(Counter.HEADER_SKIPPED).increment(1);
            return;
        }
        
        String[] fields = line.split(",");
        
        if (fields.length >= 5) {
            try {
                String categoryStr = fields[2].trim();
                double price = Double.parseDouble(fields[3].trim());
                long quantity = Long.parseLong(fields[4].trim());
                double revenue = price * quantity;
                
                category.set(categoryStr);
                record = new SalesRecordWritable(revenue, quantity);
                
                context.write(category, record);
            } catch (NumberFormatException e) {
                context.getCounter(Counter.BAD_RECORDS).increment(1);
                System.err.println("Invalid row: " + line);
            }
        } else {
            context.getCounter(Counter.BAD_RECORDS).increment(1);
            System.err.println("Row does not have enough fields: " + line);
        }
    }
}
