package org.ifmo.app;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class SalesRecordWritable implements Writable {
    private double revenue;
    private long quantity;
    
    public SalesRecordWritable() {
        this.revenue = 0.0;
        this.quantity = 0;
    }
    
    public SalesRecordWritable(double revenue, long quantity) {
        this.revenue = revenue;
        this.quantity = quantity;
    }
    
    public void add(SalesRecordWritable other) {
        this.revenue += other.revenue;
        this.quantity += other.quantity;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(revenue);
        out.writeLong(quantity);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        revenue = in.readDouble();
        quantity = in.readLong();
    }
    
    public double getRevenue() {
        return revenue;
    }
    
    public long getQuantity() {
        return quantity;
    }
    
    @Override
    public String toString() {
        return String.format("%.2f\t%d", revenue, quantity);
    }
}
