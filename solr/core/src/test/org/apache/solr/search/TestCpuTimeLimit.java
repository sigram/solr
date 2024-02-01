package org.apache.solr.search;

import org.apache.solr.SolrTestCaseJ4;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class TestCpuTimeLimit extends SolrTestCaseJ4 {

  @Test
  public void testCompareToWallClock() throws Exception {
    CpuTimeLimit limit = CpuTimeLimit.getInstance();
    long cpuLimit = 100;
    CpuTimeLimit.set(cpuLimit);
    int[] randoms = new int[100];
    try {
      long startNs = System.nanoTime();
      while (!limit.shouldExit()) {
        Thread.sleep(10);
        // do some busywork
        for (int i = 0; i < randoms.length; i++) {
          randoms[i] = random().nextInt();
        }
      }
      long endNs = System.nanoTime();
      long wallTimeDeltaMs = TimeUnit.MILLISECONDS.convert(endNs - startNs, TimeUnit.NANOSECONDS);
      assertTrue("Elapsed wall-clock time expected much larger than 100ms but was " +
          wallTimeDeltaMs, cpuLimit < wallTimeDeltaMs);
    } finally {
      CpuTimeLimit.reset();
    }
  }
}
