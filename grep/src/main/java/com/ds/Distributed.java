package com.ds;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Distributed extends Mapper<LongWritable, Text, Text, IntWritable> {

  public static String preprocess(String path, long pattern_length) throws IOException {
    RandomAccessFile raf = new RandomAccessFile(path, "r");
    // TODO: Determing numSplit value
    long numSplits = 3;
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
    byte[] buf = new byte[(int) (numBytes - pattern_length + 1)];
    byte[] buf_pref = new byte[(int) pattern_length - 1];

    int val = raf.read(buf_pref);
    if (val != -1) {
      bw.write(buf_pref);
      bw.write((byte) '\n');
      bw.write(buf_pref);
    }
    val = raf.read(buf);
    if (val != -1) {
      bw.write(buf);
    }
  }

  public static void main(String[] args) throws Exception {
    preprocess(args[0], args[2].length());
  }
}
