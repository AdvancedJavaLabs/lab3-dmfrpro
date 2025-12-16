package org.ifmo.app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.net.URI;

public class SalesAnalysisDriver {
    
    public static class DescendingDoubleComparator extends WritableComparator {
        protected DescendingDoubleComparator() {
            super(DoubleWritable.class, true);
        }
        
        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            DoubleWritable key1 = (DoubleWritable) w1;
            DoubleWritable key2 = (DoubleWritable) w2;
            return -1 * key1.compareTo(key2);
        }
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("ERROR: Need exactly 3 arguments!");
            System.err.println("Usage: SalesAnalysisDriver <input> <intermediate> <final>");
            System.err.println("Example: SalesAnalysisDriver /input /output/temp /output/result");
            System.exit(-1);
        }
        
        String inputPath = args[1];
        String intermediatePath = args[2];
        String finalPath = args[3];
        
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        
        // Clean output directories if they exist
        System.out.println("Cleaning output directories...");
        fs.delete(new Path(intermediatePath), true);
        fs.delete(new Path(finalPath), true);
        
        System.out.println("\n=== Starting Job 1: Calculate revenue per category ===");
        
        Job job1 = Job.getInstance(conf, "Sales Analysis");
        job1.setJarByClass(SalesAnalysisDriver.class);
        
        job1.setMapperClass(SalesMapper.class);
        job1.setReducerClass(SalesReducer.class);
        
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(SalesRecordWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(SalesRecordWritable.class);
        
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(intermediatePath));
        
        job1.setNumReduceTasks(3);
        
        boolean success = job1.waitForCompletion(true);
        
        if (!success) {
            System.err.println("Job 1 failed!");
            System.exit(1);
        }
        
        System.out.println("\n=== Starting Job 2: Sort by revenue (descending) ===");
        
        Job job2 = Job.getInstance(conf, "Sort by Revenue");
        job2.setJarByClass(SalesAnalysisDriver.class);
        
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        
        job2.setMapOutputKeyClass(DoubleWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        job2.setSortComparatorClass(DescendingDoubleComparator.class);
        
        FileInputFormat.addInputPath(job2, new Path(intermediatePath));
        FileOutputFormat.setOutputPath(job2, new Path(finalPath));
        
         // Single reducer for global sort
        job2.setNumReduceTasks(1);
        
        success = job2.waitForCompletion(true);
        
        System.out.println("\n=== Job Complete ===");        
        System.exit(success ? 0 : 1);
    }
}
