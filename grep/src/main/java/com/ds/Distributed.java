package com.ds;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
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
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.map.RegexMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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
      long offset = Long.parseLong(key.toString());
      long lineNumber =
          (Long.parseLong(key.toString()) - pattern.length - 1) / (bytesPerSplit + pattern.length);
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
          patternIter = lps[patternIter - 1];
        }
      }
      context.write(new Text(_pattern), new LongWritable(counter));
    }
  }

  public static class GReducer<K> extends Reducer<K, LongWritable, K, LongWritable> {

    private LongWritable result = new LongWritable();

    public void reduce(K key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  // TODO: Handle folder preprocess
  public static String preprocess(String path, long pattern_length) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(path, "r");
    // TODO: Determing numSplit value
    sourceSize = raf.length();
    numSplits = (sourceSize + 1000000 - 1) / 1000000;
    bytesPerSplit = sourceSize / numSplits;
    remainingBytes = sourceSize % numSplits;

    // TODO: max buffer size flushed at a time
    int maxReadBufferSize = 8 * 1024; //8KB
    int flag = 0;
    BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(path + "1234"));
    for (int destIx = 1; destIx <= numSplits; destIx++) {
      if (bytesPerSplit > maxReadBufferSize) {
        long numReads = bytesPerSplit / maxReadBufferSize;
        long numRemainingRead = bytesPerSplit % maxReadBufferSize;
        for (int i = 0; i < numReads; i++) {
          if (i == 0) {
            flag = 1;
          } else {
            flag = 0;
          }
          readWrite(raf, bw, maxReadBufferSize, pattern_length, flag);
        }
        if (numRemainingRead > 0) {
          if (numReads == 0) {
            flag = 1;
          } else {
            flag = 0;
          }
          readWrite(raf, bw, numRemainingRead, pattern_length, flag);
        }
      } else {
        flag = 1;
        readWrite(raf, bw, bytesPerSplit, pattern_length, flag);
      }
    }
    if (remainingBytes > 0) {
      flag = 1;
      readWrite(raf, bw, remainingBytes, pattern_length, flag);
    }
    bw.close();
    raf.close();
    return path + "1234";
  }

  static void readWrite(RandomAccessFile raf, BufferedOutputStream bw, long numBytes,
      long pattern_length, int flag)
      throws IOException {
    int val;
    if (flag > 0) {
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
    } else {
      try {
        byte[] buf_pref = new byte[(int) pattern_length - 1];
        val = raf.read(buf_pref);
        if (val != -1) {
          bw.write(buf_pref);
        }
      } catch (Exception e) {

      }
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

      grepJob.setCombinerClass(GReducer.class);
      grepJob.setReducerClass(GReducer.class);

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
      f.delete();
      posWriter.close();
    }
    return 0;
  }
}
