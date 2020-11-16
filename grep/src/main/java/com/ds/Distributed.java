package com.ds;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Distributed extends Configured implements Tool {

  // TODO: Handle folder preprocess
  public static String preprocess(String path, long pattern_length) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(path, "r");
    // TODO: Determing numSplit value
    long numSplits = 1;
    long sourceSize = raf.length();
    long bytesPerSplit = sourceSize / numSplits;
    long remainingBytes = sourceSize % numSplits;

    // TODO: max buffer size flushed at a time
    int maxReadBufferSize = 8 * 1024; //8KB
    BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(path + "1234"));
    for (int destIx = 1; destIx <= numSplits; destIx++) {
      if (bytesPerSplit > maxReadBufferSize) {
        long numReads = bytesPerSplit / maxReadBufferSize;
        long numRemainingRead = bytesPerSplit % maxReadBufferSize;
        for (int i = 0; i < numReads; i++) {
          readWrite(raf, bw, maxReadBufferSize, pattern_length);
        }
        if (numRemainingRead > 0) {
          readWrite(raf, bw, numRemainingRead, pattern_length);
        }
      } else {
        readWrite(raf, bw, bytesPerSplit, pattern_length);
      }
    }
    if (remainingBytes > 0) {
      readWrite(raf, bw, remainingBytes, pattern_length);
    }
    bw.close();
    raf.close();
    return path + "1234";
  }

  static void readWrite(RandomAccessFile raf, BufferedOutputStream bw, long numBytes,
      long pattern_length)
      throws IOException {
    int val;
    try {
      byte[] buf_pref = new byte[(int) pattern_length - 1];
      val = raf.read(buf_pref);
      if (val != -1) {
        bw.write(buf_pref);
        bw.write((byte) '\n');
        bw.write(buf_pref);
      }
    }catch (Exception e){

    }
    try {
      byte[] buf = new byte[(int) (numBytes - pattern_length + 1)];
      val = raf.read(buf);
      if (val != -1) {
        bw.write(buf);
      }
    }
    catch (Exception e){

    }
  }

  public static void main(String[] args) throws Exception {
    String new_path = preprocess(args[0], args[2].length());
    args[0] = new_path;
    int res = ToolRunner.run(new Configuration(), new Distributed(), args);
    System.exit(res);
  }

  @Override
  public int run(String[] args) throws Exception {
    Path tempDir =
        new Path("grep-temp-" +
            Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));

    Configuration conf = getConf();
    conf.set(RegexMapper.PATTERN, args[2]);
    if (args.length == 4) {
      conf.set(RegexMapper.GROUP, args[3]);
    }

    Job grepJob = Job.getInstance(conf);

    try {

      grepJob.setJobName("grep-search");
      grepJob.setJarByClass(Distributed.class);

      FileInputFormat.setInputPaths(grepJob, args[0]);

      grepJob.setMapperClass(RegexMapper.class);

      grepJob.setCombinerClass(LongSumReducer.class);
      grepJob.setReducerClass(LongSumReducer.class);

      FileOutputFormat.setOutputPath(grepJob, tempDir);
      grepJob.setOutputFormatClass(SequenceFileOutputFormat.class);
      grepJob.setOutputKeyClass(Text.class);
      grepJob.setOutputValueClass(LongWritable.class);

      grepJob.waitForCompletion(true);

      Job sortJob = Job.getInstance(conf);
      sortJob.setJobName("grep-sort");
      sortJob.setJarByClass(Distributed.class);

      FileInputFormat.setInputPaths(sortJob, tempDir);
      sortJob.setInputFormatClass(SequenceFileInputFormat.class);

      sortJob.setMapperClass(InverseMapper.class);

      sortJob.setNumReduceTasks(1);                 // write a single file
      FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));
      sortJob.setSortComparatorClass(          // sort by decreasing freq
          LongWritable.DecreasingComparator.class);

      sortJob.waitForCompletion(true);
    } finally {
      FileSystem.get(conf).delete(tempDir, true);
      File f = new File(args[0]);
      f.delete();
    }
    return 0;
  }
}
