package com.ds;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;

public class Sequential {

  /**
   * Searches for the occurrence of the pattern in the InputStream.
   *
   * @return ArrayList containing positions where pattern is found.
   * @throws IOException
   */
  public static ArrayList<Long> search(byte[] pattern_, InputStream stream) throws IOException {
    byte[] pattern = Arrays.copyOf(pattern_, pattern_.length);
    int[] lps = new int[pattern_.length];
    preProcess(pattern, lps);

    long offset = 0;

    int b;
    int patternIter = 0;
    ArrayList<Long> positions = new java.util.ArrayList<Long>();
    while ((b = stream.read()) != -1) {
      offset++;
      if ((byte) b == pattern[patternIter]) {
        patternIter++;
      } else {
        while (patternIter != 0 && (byte) b != pattern_[patternIter]) {
          patternIter = lps[patternIter - 1];
        }
      }
      if (patternIter == pattern_.length) {
        positions.add(offset - pattern_.length);
        patternIter = lps[patternIter - 1];
      }
    }
    return positions;
  }

  /**
   * Builds up a table of longest "borders" for each prefix of the pattern to find. This table is
   * stored internally and aids in implementation of the Knuth-Moore-Pratt string search.
   * <p>
   * For more information, see: https://www.inf.fh-flensburg.de/lang/algorithmen/pattern/kmpen.htm.
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
          while (j > 0 && pattern[i] != pattern[j]) {
            j = lps[j - 1];
          }
          if (pattern[i] == pattern[j]) {
            j++;
          }
          lps[i] = j;
        }
      }
    }
  }
}
