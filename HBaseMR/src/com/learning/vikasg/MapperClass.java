package com.learning.vikasg;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;

/**
 * counts the number of userIDs
 * 
 * @author sujee ==at== sujee.net
 * 
 */

public class MapperClass extends TableMapper<ImmutableBytesWritable, IntWritable> {

	private int numRecords = 0;
	private static final IntWritable one = new IntWritable(1);

	@Override
	public void map(ImmutableBytesWritable row, Result values, Context context) throws IOException {
		// extract userKey from the compositeKey (userId + counter)
		ImmutableBytesWritable userKey = new ImmutableBytesWritable(row.get(), 0, Bytes.SIZEOF_INT);
		try {
			context.write(userKey, one);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		numRecords++;
		if ((numRecords % 10000) == 0) {
			context.setStatus("mapper processed " + numRecords + " records so far");
		}
	}

}