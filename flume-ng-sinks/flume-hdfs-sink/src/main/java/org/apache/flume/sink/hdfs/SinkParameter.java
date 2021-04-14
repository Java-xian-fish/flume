package org.apache.flume.sink.hdfs;

import org.apache.flume.Context;
import org.apache.flume.auth.PrivilegedExecutor;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

public class SinkParameter {

    private volatile FileSystem mockFs;
    private volatile HDFSWriter mockWriter;
    private volatile long rollInterval;
    private volatile long rollSize;
    private volatile long rollCount;
    private volatile Context context;
    private volatile CompressionCodec codeC = null;
    private volatile SequenceFile.CompressionType compType;
    private volatile String fileType;
    private volatile String filePath;
    private volatile String fileName;
    private volatile String suffix;
    private volatile String inUsePrefix;
    private volatile String inUseSuffix = "";
    private volatile ScheduledExecutorService timedRollerPool;
    private volatile PrivilegedExecutor privExecutor;
    private volatile SinkCounter sinkCounter;
    private volatile int idleTimeout;
    private volatile long callTimeout;
    private volatile ExecutorService callTimeoutPool;
    private volatile long retryInterval;
    private volatile int maxOpenFiles;
    private volatile long batchSize;
    private final HDFSWriterFactory writerFactory;
    private volatile ShiftController controller;
    private volatile int tryCount;
    private volatile int ratioCount = 0;
    private volatile int ratioSumCount = 0;
    private volatile double ratio = 1.0;
    private volatile long count = 0;
    private volatile boolean nextDay = false;

    public static final int SUM_NUM = 200000;
    public static final double FIRST_RATIO = 0.65;
    public static final double SECOND_RATIO = 0.4;
    public static final double LAST_RATIO = 0.1;
    public static final double END_RATIO = 0.005;
    public static final int DATA_SIZE = 2048;
    public static final int HANDLE_BATCH  = ( DATA_SIZE / 100 ) * 100;
    public static final int THREAD_SIZE = 20;
    public static final int TIME_COUNT = 600 * 1000;
    public static final int STEPS = 5;
    public static final int NUM_THREAD_SHIFT_ONE_TIME = THREAD_SIZE / STEPS;

    private static volatile int step = 1;

    public SinkParameter(HDFSWriterFactory _writerFactory){
        this.writerFactory = _writerFactory;
    }

    public long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(long batchSize) {
        this.batchSize = batchSize;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count += count;
    }

    public boolean isNextDay() {
        return nextDay;
    }

    public void setNextDay(boolean nextDay) {
        this.nextDay = nextDay;
    }

    public ShiftController getController() {
        return controller;
    }

    public void setController(ShiftController controller) {
        this.controller = controller;
    }

    public int getRatioCount() {
        return ratioCount;
    }

    public void setRatioCount(int ratioCount) {
        this.ratioCount = ratioCount;
    }

    public int getRatioSumCount() {
        return ratioSumCount;
    }

    public void setRatioSumCount(int ratioSumCount) {
        this.ratioSumCount = ratioSumCount;
    }

    public double getRatio() {
        return ratio;
    }

    public void setRatio(double ratio) {
        this.ratio = ratio;
    }

    public static int getStep() {
        return step;
    }

    public static void setStep(int step) {
        SinkParameter.step = step;
    }

    public HDFSWriterFactory getWriterFactory() {
        return writerFactory;
    }

    public int getMaxOpenFiles() {
        return maxOpenFiles;
    }

    public void setMaxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
    }

    public long getRetryInterval() {
        return retryInterval;
    }

    public void setRetryInterval(long retryInterval) {
        this.retryInterval = retryInterval;
    }

    public int getTryCount() {
        return tryCount;
    }

    public void setTryCount(int tryCount) {
        this.tryCount = tryCount;
    }

    public long getCallTimeout() {
        return callTimeout;
    }

    public void setCallTimeout(long callTimeout) {
        this.callTimeout = callTimeout;
    }

    public ExecutorService getCallTimeoutPool() {
        return callTimeoutPool;
    }

    public void setCallTimeoutPool(ExecutorService callTimeoutPool) {
        this.callTimeoutPool = callTimeoutPool;
    }

    public SinkCounter getSinkCounter() {
        return sinkCounter;
    }

    public void setSinkCounter(SinkCounter sinkCounter) {
        this.sinkCounter = sinkCounter;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    public PrivilegedExecutor getPrivExecutor() {
        return privExecutor;
    }

    public void setPrivExecutor(PrivilegedExecutor privExecutor) {
        this.privExecutor = privExecutor;
    }

    public ScheduledExecutorService getTimedRollerPool() {
        return timedRollerPool;
    }

    public void setTimedRollerPool(ScheduledExecutorService timedRollerPool) {
        this.timedRollerPool = timedRollerPool;
    }

    public CompressionCodec getCodeC() {
        return codeC;
    }

    public void setCodeC(CompressionCodec codeC) {
        this.codeC = codeC;
    }

    public SequenceFile.CompressionType getCompType() {
        return compType;
    }

    public void setCompType(SequenceFile.CompressionType compType) {
        this.compType = compType;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    public String getInUsePrefix() {
        return inUsePrefix;
    }

    public void setInUsePrefix(String inUsePrefix) {
        this.inUsePrefix = inUsePrefix;
    }

    public String getInUseSuffix() {
        return inUseSuffix;
    }

    public void setInUseSuffix(String inUseSuffix) {
        this.inUseSuffix = inUseSuffix;
    }

    public Context getContext() {
        return context;
    }

    public void setContext(Context context) {
        this.context = context;
    }

    public long getRollSize() {
        return rollSize;
    }

    public void setRollSize(long rollSize) {
        this.rollSize = rollSize;
    }

    public long getRollCount() {
        return rollCount;
    }

    public void setRollCount(long rollCount) {
        this.rollCount = rollCount;
    }

    public long getRollInterval() {
        return rollInterval;
    }

    public void setRollInterval(long rollInterval) {
        this.rollInterval = rollInterval;
    }

    public HDFSWriter getMockWriter() {
        return mockWriter;
    }

    public void setMockWriter(HDFSWriter mockWriter) {
        this.mockWriter = mockWriter;
    }

    public FileSystem getMockFs() {
        return mockFs;
    }

    public void setMockFs(FileSystem mockFs) {
        this.mockFs = mockFs;
    }
}
