package com.ds;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Test;

/**
 * Unit test for Grep.
 */
public class StringSearchTest {

  /**
   * Rigorous Test :-)
   */
  @Test
  public void sequentialTest() throws IOException {

    InputStream initialStream = new FileInputStream(
        new File("src/test/java/com/ds/test"));
  }
}
