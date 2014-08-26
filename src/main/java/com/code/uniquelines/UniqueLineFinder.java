package com.code.uniquelines;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

class UniqueLineFinder {

	/**
	 * @param args
	 * 
	 */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
        String dir;
        String inputGZFile;
        String inputFile;
		
		Job job = new Job(conf, "UniqueLineFinder");
		job.setJarByClass(UniqueLineFinder.class);
		job.setMapperClass(UniqueLineMapper.class);
		job.setReducerClass(UniqueLineReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

        dir =  "/0/data/";
        inputGZFile = "foo.txt.gz";
        inputFile = "foo.txt";

//        dir =  "/Users/neha/workspace/";

		decompressGZToTextFile(dir+inputGZFile);

        FileInputFormat.addInputPath(job, new Path("file://"+dir+inputFile));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/uniquelines"));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
/*
*
* Mapper to take the lines and give each of them as count one for their occurrence.
*
* */
	public static class UniqueLineMapper extends
			Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable ONE = new IntWritable(1);

        @Override
		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			context.write(value, ONE);

		}
	}

/*
*
* Reducer to take the final output of only those lines where the count is less than one
* i.e. the unique lines.
*
* */
	public static class UniqueLineReducer extends
			Reducer<Text, IntWritable, Text, NullWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int countPerLine = 0;
			for (IntWritable count : values) {
				countPerLine += count.get();
			}

			if (countPerLine < 2) {
				result.set(countPerLine);
				context.write(key, NullWritable.get());
			}
		}
	}

    /*
    *
    * This method is a utility method to convert .gz to zip files at the same folder.
    *
    * */

 	public static void decompressGZToTextFile(String uri) throws IOException{

	    Configuration conf = new Configuration();
	    FileSystem fs = FileSystem.get(URI.create(uri), conf);
	    
	    Path inputPath = new Path(uri);
	    CompressionCodecFactory factory = new CompressionCodecFactory(conf);
	    CompressionCodec codec = factory.getCodec(inputPath);
	    if (codec == null) {
	      System.err.println("No codec found for " + uri);
	    }

	    String outputUri =
	      CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());

	    InputStream in = null;
	    OutputStream out = null;
	    try {
	      in = codec.createInputStream(fs.open(inputPath));
	      out = fs.create(new Path(outputUri));
	      IOUtils.copyBytes(in, out, conf);
	    } finally {
	      IOUtils.closeStream(in);
	      IOUtils.closeStream(out);
	    }
	  }
	}


