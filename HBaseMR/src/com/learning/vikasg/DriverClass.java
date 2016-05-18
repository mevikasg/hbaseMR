package com.learning.vikasg;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;

public class DriverClass {
	public static void main(String[] args) throws Exception {
        Configuration conf = new HBaseConfiguration().create();
        Job job = new Job(conf, "HBase_MR_UserCount");
        job.setJarByClass(DriverClass.class);
        Scan scan = new Scan();
        String columns = "details"; // comma separated
        scan.addFamily(Bytes.toBytes(columns));
        scan.setFilter(new FirstKeyOnlyFilter());
        TableMapReduceUtil.initTableMapperJob("access_logs", scan, MapperClass.class, ImmutableBytesWritable.class,
                IntWritable.class, job);
        TableMapReduceUtil.initTableReducerJob("summary_user", ReducerClass.class, job);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
