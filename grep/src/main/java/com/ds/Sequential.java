package com.ds;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;

public class Sequential {

  static FileWriter posWriter;

  /**
   * Searches for the occurrence of the pattern in the InputStream.
   *
   * @return ArrayList containing positions where pattern is found.
   * @throws IOException
   */
  public static long search(String pattern_, String path) throws IOException {
    InputStream stream = new FileInputStream(
        new File(path));
    byte[] pattern = pattern_.getBytes();
    int[] lps = new int[pattern_.length()];
    preProcess(pattern, lps);

    long offset = 0;

    int b;
    int patternIter = 0;
    long counter = 0;
    while ((b = stream.read()) != -1) {
      offset++;
      if ((byte) b == pattern[patternIter]) {
        patternIter++;
      } else {
        while (patternIter != 0 && (byte) b != pattern[patternIter]) {
          patternIter = lps[patternIter - 1];
        }
        if (b == pattern[patternIter]) {
          patternIter++;
        }

      }
      if (patternIter == pattern_.length()) {
        counter++;
        posWriter.write(offset - pattern_.length() + " ");
        patternIter = lps[patternIter - 1];
      }
    }
    return counter;
  }

  /**
   * Builds up a table of lps. This table is stored internally and aids in implementation of the
   * Knuth-Moore-Pratt string search.
   */
  protected static void preProcess(byte[] pattern, int[] lps) {
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

  public static void main(String[] args) throws Exception {
    File pos = new File("seq-positions.txt");
    pos.createNewFile();
    posWriter = new FileWriter("seq-positions.txt");
    System.out.println(args[2] + "\t" + search(args[2], args[0]));
    posWriter.close();
  }
}
