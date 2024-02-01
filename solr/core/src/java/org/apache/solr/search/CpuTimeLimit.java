package org.apache.solr.search;

import org.apache.lucene.index.QueryTimeout;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.request.SolrQueryRequest;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.TimeUnit;

public class CpuTimeLimit implements QueryTimeout {
  /** The ThreadLocal variable to store the resource usage beyond which the processing should exit. */
  private static final ThreadLocal<Long> maxLimits = new ThreadLocal<>();
  private static final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();

  private static final CpuTimeLimit instance = new CpuTimeLimit();

  private CpuTimeLimit() {}

  /** Return singleton instance */
  public static CpuTimeLimit getInstance() {
    return instance;
  }

  /** The maximum usage for the thread at which the request should be terminated. */
  public static Long getMaxLimit() {
    return maxLimits.get();
  }

  public boolean isLimitEnabled() {
    return getMaxLimit() != null;
  }

  /** Return true if a max limit value is set and the current usage has exceeded the limit. */
  @Override
  public boolean shouldExit() {
    Long maxLimit = getMaxLimit();
    if (maxLimit == null) {
      // not set
      return false;
    }
    return maxLimit - threadMXBean.getCurrentThreadCpuTime() < 0L;
  }

  /**
   * Sets or clears the time allowed based on how much time remains from the start of the request
   * plus the configured {@link CommonParams#CPU_ALLOWED}.
   */
  public static void set(SolrQueryRequest req) {
    long cpuAllowed = req.getParams().getLong(CommonParams.CPU_ALLOWED, -1L);
    if (cpuAllowed >= 0L) {
      set(cpuAllowed);
    } else {
      reset();
    }
  }

  /**
   * Sets the time allowed (milliseconds), assuming we start a timer immediately. You should
   * probably invoke {@link #set(SolrQueryRequest)} instead.
   */
  public static void set(long timeAllowed) {
    long time = threadMXBean.getCurrentThreadCpuTime() + TimeUnit.NANOSECONDS.convert(timeAllowed, TimeUnit.MILLISECONDS);
    maxLimits.set(time);
  }

  /** Cleanup the ThreadLocal timeout value. */
  public static void reset() {
    maxLimits.remove();
  }

  @Override
  public String toString() {
    return "maxLimit: " + getMaxLimit() + " (current cpuTime: " + threadMXBean.getCurrentThreadCpuTime() + ")";
  }

  /** Internal impl for speed only used when we know there's a limit enabled. */
  QueryTimeout makeLocalImpl() {
    assert isLimitEnabled();
    return new QueryTimeout() {
      final long maxLimit = getMaxLimit();

      @Override
      public boolean shouldExit() {
        return maxLimit - threadMXBean.getCurrentThreadCpuTime() < 0L;
      }
    };
  }
}
