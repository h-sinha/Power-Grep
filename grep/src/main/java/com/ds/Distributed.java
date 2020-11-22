package com.ds;

import static java.lang.StrictMath.max;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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

  static long numSplits, sourceSize, bytesPerSplit, remainingBytes;
  static FileWriter posWriter;

  protected static void preProcess_string(byte[] pattern, int[] lps) {
    int j = 0;
    lps[0] = 0;
    for (int i = 1; i < pattern.length; i++) {
      if (pattern[i] == pattern[j]) {
        j++;
        lps[i] = j;
      } else {
        if (j == 0) {
          lps[i] = 0;
        } else {
          if (j > 0) {
            j = lps[j - 1];
            i--;
          } else {
            lps[i] = 0;
          }
        }
      }
    }
  }

  public static class GMapper<K> extends
      Mapper<K, Text, Text, LongWritable> {

    public static String PATTERN = "mapreduce.mapper.regex";
    public static String GROUP = "mapreduce.mapper.regexmapper..group";
    private byte[] pattern;
    private int[] lps;
    String _pattern;

    public void setup(Context context) {
      Configuration conf = context.getConfiguration();
      pattern = conf.get(PATTERN).getBytes();
      _pattern = conf.get(PATTERN);
      lps = new int[pattern.length];
      preProcess_string(pattern, lps);
    }

    public void map(K key, Text value, Context context) throws IOException, InterruptedException {
      byte[] line = value.toString().getBytes();
      byte b;
      int patternIter = 0, idx = 0;
      long counter = 0;
      long offset = (Long.parseLong(key.toString()));
      long lineNumber = (Long.parseLong(key.toString())) / (bytesPerSplit + pattern.length);
      while (idx < line.length) {
        offset++;
        b = line[idx];
        idx++;
        if (b == pattern[patternIter]) {
          patternIter++;
        } else {
          while (patternIter != 0 && b != pattern[patternIter]) {
            patternIter = lps[patternIter - 1];
          }
          if (b == pattern[patternIter]) {
            patternIter++;
          }

        }
        if (patternIter == pattern.length) {
          counter++;
          posWriter.write(offset - pattern.length - (lineNumber + 1) * pattern.length + " ");
//          System.out.println(offset + " "+ pattern.length + " " +key + " " + value.toString().substring(0, 10) + " " + value.toString()
//              .substring((int) (offset - pattern.length - 10L), (int) (offset + 10L)));
          patternIter = lps[patternIter - 1];
        }
      }
      context.write(new Text(_pattern), new LongWritable(counter));
    }
  }

  // TODO: Handle folder preprocess
  public static String preprocess(String path, long pattern_length) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(path, "r");
    // TODO: Determing numSplit value
    numSplits = 1;
    sourceSize = raf.length();
    bytesPerSplit = sourceSize / numSplits;
    remainingBytes = sourceSize % numSplits;

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
    } catch (Exception e) {

    }
    try {
      byte[] buf = new byte[(int) (numBytes - pattern_length + 1)];
      val = raf.read(buf);
      if (val != -1) {
        bw.write(buf);
      }
    } catch (Exception e) {

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
    File pos = new File("dis-positions.txt");
    pos.createNewFile();
    posWriter = new FileWriter("dis-positions.txt");

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

      grepJob.setMapperClass(GMapper.class);

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

      sortJob.setNumReduceTasks(1);
      FileOutputFormat.setOutputPath(sortJob, new Path(args[1]));
      sortJob.setSortComparatorClass(
          LongWritable.DecreasingComparator.class);

      sortJob.waitForCompletion(true);
    } finally {
      FileSystem.get(conf).delete(tempDir, true);
      File f = new File(args[0]);
//      f.delete();
      posWriter.close();
    }
    return 0;
  }
}
