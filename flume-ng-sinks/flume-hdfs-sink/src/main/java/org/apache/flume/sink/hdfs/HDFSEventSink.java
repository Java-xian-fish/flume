/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.sink.hdfs;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flume.Channel;
import org.apache.flume.Clock;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.SystemClock;
import org.apache.flume.Transaction;
import org.apache.flume.auth.FlumeAuthenticationUtil;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.formatter.output.BucketPath;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class HDFSEventSink extends AbstractSink implements Configurable, BatchSizeSupported {

  private static final Logger LOG = LoggerFactory.getLogger(HDFSEventSink.class);

  private static String DIRECTORY_DELIMITER = System.getProperty("file.separator");
  private static final long defaultRollInterval = 30;
  private static final long defaultRollSize = 1024;
  private static final long defaultRollCount = 10;
  private static final String defaultFileName = "FlumeData";
  private static final String defaultSuffix = "";
  private static final String defaultInUsePrefix = "";
  private static final String defaultInUseSuffix = ".tmp";
  private static final long defaultBatchSize = 100;
  private static final String defaultFileType = HDFSWriterFactory.SequenceFileType;
  private static final int defaultMaxOpenFiles = 5000;
  // Time between close retries, in seconds
  private static final long defaultRetryInterval = 180;
  // Retry forever.
  private static final int defaultTryCount = Integer.MAX_VALUE;
  public static final String IN_USE_SUFFIX_PARAM_NAME = "hdfs.inUseSuffix";

  /**
   * Default length of time we wait for blocking BucketWriter calls
   * before timing out the operation. Intended to prevent server hangs.
   */
  private static final long defaultCallTimeout = 30000;
  /**
   * Default number of threads available for tasks
   * such as append/open/close/flush with hdfs.
   * These tasks are done in a separate thread in
   * the case that they take too long. In which
   * case we create a new file and move on.
   */
  private static final int defaultThreadPoolSize = 10;
  private static final int defaultRollTimerPoolSize = 1;
  private long batchSize;
  private int threadsPoolSize;
  private int rollTimerPoolSize;
  private TimeZone timeZone;
  private boolean needRounding = false;
  private int roundUnit = Calendar.SECOND;
  private int roundValue = 1;
  //Object for sink parameter which all threads will use
  private volatile SinkParameter parameter;
  private volatile String day;
  private volatile ShiftController controller;
  private AbstractDispatcher dispatcher;

  public HDFSEventSink() {
    this(new HDFSWriterFactory());
  }

  public HDFSEventSink(HDFSWriterFactory writerFactory) {
    parameter = new SinkParameter(writerFactory);
    LOG.info("ParallelSink-->HDFSEventSink-->SinkParameter has been created!");
  }

  // read configuration and setup thresholds
  @Override
  public void configure(Context context) {
    LOG.info("ParallelSink-->HDFSEventSink-->configure! Context ClassLoader-->" + Context.class.getClassLoader());
    this.parameter.setContext(context);
    parameter.setFilePath( Preconditions.checkNotNull(
            context.getString("hdfs.path"), "hdfs.path is required"));
    parameter.setFileName(context.getString("hdfs.filePrefix", defaultFileName));
    parameter.setSuffix(context.getString("hdfs.fileSuffix", defaultSuffix));
    parameter.setInUsePrefix(context.getString("hdfs.inUsePrefix", defaultInUsePrefix));
    boolean emptyInUseSuffix = context.getBoolean("hdfs.emptyInUseSuffix", false);
    if (emptyInUseSuffix) {
      String tmpInUseSuffix = context.getString(IN_USE_SUFFIX_PARAM_NAME);
      if (tmpInUseSuffix != null) {
        LOG.warn("Ignoring parameter " + IN_USE_SUFFIX_PARAM_NAME + " for hdfs sink: " + getName());
      }
    } else {
      parameter.setInUseSuffix(context.getString(IN_USE_SUFFIX_PARAM_NAME, defaultInUseSuffix));
    }
    String tzName = context.getString("hdfs.timeZone");
    timeZone = tzName == null ? null : TimeZone.getTimeZone(tzName);
    parameter.setRollInterval(context.getLong("hdfs.rollInterval", defaultRollInterval));
    parameter.setRollSize(context.getLong("hdfs.rollSize", defaultRollSize));
    parameter.setRollCount(context.getLong("hdfs.rollCount", defaultRollCount));
    batchSize = context.getLong("hdfs.batchSize", defaultBatchSize);
    parameter.setBatchSize(batchSize);
    parameter.setIdleTimeout(context.getInteger("hdfs.idleTimeout", 0));
    String codecName = context.getString("hdfs.codeC");
    parameter.setFileType(context.getString("hdfs.fileType", defaultFileType));
    parameter.setMaxOpenFiles(context.getInteger("hdfs.maxOpenFiles", defaultMaxOpenFiles));
    parameter.setCallTimeout(context.getLong("hdfs.callTimeout", defaultCallTimeout));
    threadsPoolSize = context.getInteger("hdfs.threadsPoolSize",
            defaultThreadPoolSize);
    rollTimerPoolSize = context.getInteger("hdfs.rollTimerPoolSize",
            defaultRollTimerPoolSize);
    String kerbConfPrincipal = context.getString("hdfs.kerberosPrincipal");
    String kerbKeytab = context.getString("hdfs.kerberosKeytab");
    String proxyUser = context.getString("hdfs.proxyUser");
    parameter.setTryCount(context.getInteger("hdfs.closeTries", defaultTryCount));
    if (parameter.getTryCount() <= 0) {
      LOG.warn("Retry count value : " + parameter.getTryCount() + " is not " +
              "valid. The sink will try to close the file until the file " +
              "is eventually closed.");
      parameter.setTryCount(defaultTryCount);
    }
    parameter.setRetryInterval(context.getLong("hdfs.retryInterval", defaultRetryInterval));
    if (parameter.getRetryInterval() <= 0) {
      LOG.warn("Retry Interval value: " + parameter.getRetryInterval() + " is not " +
              "valid. If the first close of a file fails, " +
              "it may remain open and will not be renamed.");
      parameter.setTryCount(1);
    }

    Preconditions.checkArgument(batchSize > 0, "batchSize must be greater than 0");
    if (codecName == null) {
      parameter.setCompType(CompressionType.NONE);
    } else {
      parameter.setCodeC(getCodec(codecName));
      // TODO : set proper compression type
      parameter.setCompType(CompressionType.BLOCK);
    }

    // Do not allow user to set fileType DataStream with codeC together
    // To prevent output file with compress extension (like .snappy)
    if (parameter.getFileType().equalsIgnoreCase(HDFSWriterFactory.DataStreamType) && codecName != null) {
      throw new IllegalArgumentException("fileType: " + parameter.getFileType() +
              " which does NOT support compressed output. Please don't set codeC" +
              " or change the fileType if compressed output is desired.");
    }

    if (parameter.getFileType().equalsIgnoreCase(HDFSWriterFactory.CompStreamType)) {
      Preconditions.checkNotNull(parameter.getCodeC(), "It's essential to set compress codec"
              + " when fileType is: " + parameter.getFileType());
    }

    // get the appropriate executor
    parameter.setPrivExecutor(FlumeAuthenticationUtil.getAuthenticator(
            kerbConfPrincipal, kerbKeytab).proxyAs(proxyUser));
    needRounding = context.getBoolean("hdfs.round", false);
    if (needRounding) {
      String unit = context.getString("hdfs.roundUnit", "second");
      if (unit.equalsIgnoreCase("hour")) {
        this.roundUnit = Calendar.HOUR_OF_DAY;
      } else if (unit.equalsIgnoreCase("minute")) {
        this.roundUnit = Calendar.MINUTE;
      } else if (unit.equalsIgnoreCase("second")) {
        this.roundUnit = Calendar.SECOND;
      } else {
        LOG.warn("Rounding unit is not valid, please set one of" +
                "minute, hour, or second. Rounding will be disabled");
        needRounding = false;
      }
      this.roundValue = context.getInteger("hdfs.roundValue", 1);
      if (roundUnit == Calendar.SECOND || roundUnit == Calendar.MINUTE) {
        Preconditions.checkArgument(roundValue > 0 && roundValue <= 60,
                "Round value" +
                        "must be > 0 and <= 60");
      } else if (roundUnit == Calendar.HOUR_OF_DAY) {
        Preconditions.checkArgument(roundValue > 0 && roundValue <= 24,
                "Round value" +
                        "must be > 0 and <= 24");
      }
    }

    if (parameter.getSinkCounter() == null) {
      parameter.setSinkCounter(new SinkCounter(getName()));
    }
  }

  private static boolean codecMatches(Class<? extends CompressionCodec> cls, String codecName) {
    String simpleName = cls.getSimpleName();
    if (cls.getName().equals(codecName) || simpleName.equalsIgnoreCase(codecName)) {
      return true;
    }
    if (simpleName.endsWith("Codec")) {
      String prefix = simpleName.substring(0, simpleName.length() - "Codec".length());
      if (prefix.equalsIgnoreCase(codecName)) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  static CompressionCodec getCodec(String codecName) {
    Configuration conf = new Configuration();
    List<Class<? extends CompressionCodec>> codecs = CompressionCodecFactory.getCodecClasses(conf);
    // Wish we could base this on DefaultCodec but appears not all codec's
    // extend DefaultCodec(Lzo)
    CompressionCodec codec = null;
    ArrayList<String> codecStrs = new ArrayList<String>();
    codecStrs.add("None");
    for (Class<? extends CompressionCodec> cls : codecs) {
      codecStrs.add(cls.getSimpleName());
      if (codecMatches(cls, codecName)) {
        try {
          codec = cls.newInstance();
        } catch (InstantiationException e) {
          LOG.error("Unable to instantiate " + cls + " class");
        } catch (IllegalAccessException e) {
          LOG.error("Unable to access " + cls + " class");
        }
      }
    }

    if (codec == null) {
      if (!codecName.equalsIgnoreCase("None")) {
        throw new IllegalArgumentException("Unsupported compression codec "
                + codecName + ".  Please choose from: " + codecStrs);
      }
    } else if (codec instanceof org.apache.hadoop.conf.Configurable) {
      // Must check instanceof codec as BZip2Codec doesn't inherit Configurable
      // Must set the configuration for Configurable objects that may or do use
      // native libs
      ((org.apache.hadoop.conf.Configurable) codec).setConf(conf);
    }
    return codec;
  }

  /**
   * Pull events out of channel and send it to HDFS. Take at most batchSize
   * events per Transaction. Find the corresponding bucket for the event.
   * Ensure the file is open. Serialize the data and write it to the file on
   * HDFS. <br/>
   * This method is not thread safe.
   */
  public Status process() throws EventDeliveryException {
    Channel channel = getChannel();
    Transaction transaction = channel.getTransaction();
    SinkCounter sinkCounter = parameter.getSinkCounter();
    transaction.begin();
    try {
      int txnEventCount = 0;
      for (txnEventCount = 0; txnEventCount < batchSize; txnEventCount++) {
        Event event = channel.take();
        if (event == null) {
          break;
        }
        dispatcher.dispatch(event);
        //check if its the next day.When it's 00:00,it will turn to else module.
      }
      transaction.commit();
      if (txnEventCount < 1) {
        return Status.BACKOFF;
      } else {
        parameter.setCount(txnEventCount);
        sinkCounter.addToEventDrainSuccessCount(txnEventCount);
        return Status.READY;
      }
    } catch (Exception eIO) {
      transaction.rollback();
      LOG.warn("HDFS IO error", eIO);
      sinkCounter.incrementEventWriteFail();
      return Status.BACKOFF;
    } catch (Throwable th) {
      transaction.rollback();
      LOG.error("process failed", th);
      sinkCounter.incrementEventWriteOrChannelFail(th);
      if (th instanceof Error) {
        throw (Error) th;
      } else {
        throw new EventDeliveryException(th);
      }
    } finally {
      transaction.close();
    }
  }

  @Override
  public void stop() {
    // do not constrain close() calls with a timeout

    // shut down all our thread pools
    ExecutorService[] toShutdown = { parameter.getCallTimeoutPool(), parameter.getTimedRollerPool() };
    for (ExecutorService execService : toShutdown) {
      execService.shutdown();
      try {
        while (execService.isTerminated() == false) {
          execService.awaitTermination(
                  Math.max(defaultCallTimeout, parameter.getCallTimeout()), TimeUnit.MILLISECONDS);
        }
      } catch (InterruptedException ex) {
        LOG.warn("shutdown interrupted on " + execService, ex);
      }
    }

    parameter.setCallTimeoutPool(null);
    parameter.setTimedRollerPool(null);
    parameter.getSinkCounter().stop();
    dispatcher.stop();
    controller.close();
    super.stop();
  }

  @Override
  public void start() {
    String timeoutName = "hdfs-" + getName() + "-call-runner-%d";
    parameter.setCallTimeoutPool(Executors.newFixedThreadPool(threadsPoolSize,
            new ThreadFactoryBuilder().setNameFormat(timeoutName).build()));

    String rollerName = "hdfs-" + getName() + "-roll-timer-%d";
    parameter.setTimedRollerPool(Executors.newScheduledThreadPool(rollTimerPoolSize,
            new ThreadFactoryBuilder().setNameFormat(rollerName).build()));
    parameter.getSinkCounter().start();
    dispatcher = new ShiftDispatcher(parameter);
    controller = new ShiftController(dispatcher,parameter);
    parameter.setController(controller);
    dispatcher.start();
    super.start();
  }

  @Override
  public String toString() {
    return "{ Sink type:" + getClass().getSimpleName() + ", name:" + getName() +
            " }";
  }

  @VisibleForTesting
  void setBucketClock(Clock clock) {
    BucketPath.setClock(clock);
  }

  @VisibleForTesting
  void setMockFs(FileSystem mockFs) {
    this.parameter.setMockFs(mockFs);
  }

  @VisibleForTesting
  void setMockWriter(HDFSWriter writer) {
    this.parameter.setMockWriter(writer);
  }

  @VisibleForTesting
  int getTryCount() {
    return parameter.getTryCount();
  }

  @Override
  public long getBatchSize() {
    return batchSize;
  }

}
