package org.apache.flume.sink.hdfs;

import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class ParallelSinkThread implements Runnable{

    public interface WriterCallback {
        public void run(String filePath);
    }

    private static final Logger LOG = LoggerFactory.getLogger(ParallelSinkThread.class);
    //thread buffer size
    private final int DATA_SIZE = 2048;
    //thread buffer
    private volatile Event[] data = new Event[DATA_SIZE];
    //queue buffer tail
    private volatile int rear = 0;
    //queue buffer head
    private volatile int front = 0;
    //decision for whether ending the thread
    public volatile boolean closed = false;
    //thread id for manage threads and log out
    private int id;
    //symbol of which day's data the thread will handle.true:the last day.false:the current day.
    private volatile boolean shiftDate = false;
    //symbol of the thread will shift to handle the current day's data.The process happens in run method.
    private volatile boolean shifting = false;
    //Symbol of updating BucketWriter's shiftDate var which is very important--it determines the file belongs to which day.
    private boolean updateShiftDate = false;
    private long batchSize;
    private WriterLinkedHashMap sfWriters;
    private final Object sfWritersLock = new Object();
    private HDFSWriter hdfsWriter;
    private SinkParameter parameter;
    private volatile long count;
    private volatile long oldCount;

    public ParallelSinkThread(int _id, SinkParameter _parameter){
        this.id = _id;
        this.batchSize = _parameter.getBatchSize();
        this.parameter = _parameter;
        try {
            HDFSWriterFactory writerFactory = parameter.getWriterFactory();
            this.hdfsWriter = writerFactory.getWriter(parameter.getFileType());
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.sfWriters = new WriterLinkedHashMap(parameter.getMaxOpenFiles());
    }

    public int getId(){
        return this.id;
    }

    public boolean isClosed() {
        return closed;
    }

    private static class WriterLinkedHashMap
            extends LinkedHashMap<String, BucketWriter> {

        private final int maxOpenFiles;

        public WriterLinkedHashMap(int maxOpenFiles) {
            super(16, 0.75f, true); // stock initial capacity/load, access ordering
            this.maxOpenFiles = maxOpenFiles;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, BucketWriter> eldest) {
            if (size() > maxOpenFiles) {
                // If we have more that max open files, then close the last one and
                // return true
                try {
                    eldest.getValue().close();
                } catch (InterruptedException e) {
                    LOG.warn(eldest.getKey().toString(), e);
                    Thread.currentThread().interrupt();
                }
                return true;
            } else {
                return false;
            }
        }
    }

    public void setShiftDate(boolean bool){
        shiftDate = bool;
    }

    public boolean isShiftDate() {
        return shiftDate;
    }

    public void setShifting(boolean shifting) {
        this.shifting = shifting;
    }

    public long getCount(){
        return count;
    }

    //only for controller
    public long getBatchCount(){
        long batchCount = count - oldCount;
        oldCount = count;
        return batchCount;
    }

    public  int  size(){
        return ( rear - front + DATA_SIZE ) % DATA_SIZE;
    }

    public void add(Event event){
        if (rear == DATA_SIZE){
            rear = 0;
        }
        data[rear++] = event;
    }

    public BucketWriter TimeToShift(BucketWriter _bucketWriter, String _lookupPath){
        if ( shifting && _bucketWriter != null) {
            //full the buffer,then the dispatcher will not add event in it,which can avoid parallel problem.
            while ( size() < SinkParameter.HANDLE_BATCH){}
            //the old buffer will be moved,create a new one for this thread.
            Event[] _data = new Event[DATA_SIZE];
            //the only steps need lock,because some last day threads may request normal data or bucket.
            synchronized (ShiftDispatcher.normal) {
                ShiftDispatcher.normal.offer(data);
                ShiftDispatcher.normalBucket.offer(_bucketWriter);
            }
            sfWriters.remove(_lookupPath);
            data = _data;
            //rear set first.
            rear = 0;
            front = 0;
            shiftDate = false;
            shifting = false;
            updateShiftDate = false;
            LOG.info("ParallelSink-->thread-->" + id + "**has shifted !" );
            return null;
        }else{
            return _bucketWriter;
        }
    }

    public BucketWriter getNormalBucket(String lookupPath){
        BucketWriter bucketWriter = null;
        if (!ShiftDispatcher.normalBucket.isEmpty()){
            synchronized (ShiftDispatcher.normal) {
                bucketWriter = ShiftDispatcher.normalBucket.poll();
                sfWriters.put(lookupPath, bucketWriter);
                bucketWriter.setShiftDate(shiftDate);
            }
            LOG.info("ParallelSink-->thread-->" + id + "**has handle old bucketWriter!" + bucketWriter.getBucketPath());
        }
        return bucketWriter;
    }

    private BucketWriter initializeBucketWriter(String realPath,
                                                String realName, String lookupPath, HDFSWriter hdfsWriter, WriterCallback closeCallback, int id) {
        HDFSWriter actualHdfsWriter = parameter.getMockFs() == null ? hdfsWriter : parameter.getMockWriter();
        BucketWriter bucketWriter = new BucketWriter(parameter.getRollInterval(),
                parameter.getRollSize(), parameter.getRollCount(),
                batchSize, parameter.getContext(), realPath, realName, parameter.getInUsePrefix(), parameter.getInUseSuffix(),
                parameter.getSuffix(), parameter.getCodeC(), parameter.getCompType(), actualHdfsWriter, parameter.getTimedRollerPool(),
                parameter.getPrivExecutor(), parameter.getSinkCounter(), parameter.getIdleTimeout(), closeCallback,
                lookupPath, parameter.getCallTimeout(), parameter.getCallTimeoutPool(), parameter.getRetryInterval(),
                parameter.getTryCount());
        if (parameter.getMockFs() != null) {
            bucketWriter.setFileSystem(parameter.getMockFs());
        }
        return bucketWriter;
    }

    @Override
    public void run() {
        int wrong = 0;
        closed = false;
        while (!closed){
            try {
                int txnEventCount = 0;
                while (txnEventCount < batchSize) {
                    String lookupPath = "BucketWriter-" + this.id;
                    BucketWriter bucketWriter;
                    //HDFSWriter hdfsWriter = null;
                    // Callback to remove the reference to the bucket writer from the
                    // sfWriters map so that all buffers used by the HDFS file
                    // handles are garbage collected.
                    WriterCallback closeCallback = new WriterCallback() {
                        @Override
                        public void run(String bucketPath) {
                            LOG.info("Writer callback called.");
                            synchronized (sfWritersLock) {
                                sfWriters.remove(bucketPath);
                            }
                        }
                    };
                    bucketWriter = sfWriters.get(lookupPath);
                    bucketWriter = TimeToShift(bucketWriter, lookupPath);
                    if (shiftDate) {
                        if (!updateShiftDate) {
                            if (bucketWriter != null) {
                                if ( bucketWriter.getShiftedDate() != shiftDate) {
                                    bucketWriter.setShiftDate(shiftDate);
                                    LOG.info("ParallelSink-->thread-->" + id + "**has reset shiftDate**thread's shiftDate-->" + shiftDate + "** BucketWriter's shiftDate-->" + bucketWriter.getShiftedDate());
                                }
                                updateShiftDate = true;
                            }
                        }
                        //the first way which can get a bucketWriter.
                        if (bucketWriter == null) {
                            bucketWriter = getNormalBucket(lookupPath);
                        }
                    }
                    //the second way which can get a bucketWriter.
                    if (bucketWriter == null) {
                        bucketWriter = initializeBucketWriter(parameter.getFilePath(), "" + id, lookupPath, hdfsWriter, closeCallback, id);
                        bucketWriter.setShiftDate(shiftDate);
                        sfWriters.put(lookupPath, bucketWriter);
                        LOG.info("ParallelSink-->thread-->" + id + " create a bucketWriter by normal initializeBucketWriter!");
                    }
                    // Write the data to HDFS
                    Event e = null;
                    try {
                        for (int c = 0; c < SinkParameter.HANDLE_BATCH && txnEventCount < batchSize; c++) {
                            while (size() < 1) {
                            }
                            while ( e == null ) {
                                if ( front == DATA_SIZE ) {
                                    front = 0;
                                }
                                e = data[front++];
                            }
                            bucketWriter.append(e);
                            txnEventCount++;
                            count++;
                        }
                        wrong = 0;
                    } catch (BucketClosedException ex) {
                        LOG.info("ParallelSink-->thread-->" + this.id + " Bucket was closed while trying to append, " +
                                "reinitializing bucket and writing event.");
                        if (bucketWriter == null || bucketWriter.closed.get()) {
                            bucketWriter = getNormalBucket(lookupPath);
                        }
                        if (bucketWriter == null) {
                            bucketWriter = initializeBucketWriter(parameter.getFilePath(), "" + id,
                                    lookupPath, hdfsWriter, closeCallback, id);
                            bucketWriter.setShiftDate(shiftDate);
                        }
                        sfWriters.put(lookupPath, bucketWriter);
                        bucketWriter.append(e);
                        count++;
                        txnEventCount++;
                    } catch (IOException ee) {
                        LOG.info("ParallelSink-->thread-->" + this.id + "**has encounter an IOException-->" + ee);
                        ee.printStackTrace();
                        BucketWriter _bucketWriter = sfWriters.get(lookupPath);
                        wrong++;
                        if (wrong > 10 && _bucketWriter != null) {
                            try {
                                _bucketWriter.close();
                            } catch (InterruptedException interruptedException) {
                                interruptedException.printStackTrace();
                            }finally {
                                _bucketWriter = null;
                                bucketWriter = null;
                                sfWriters.remove(lookupPath);
                            }
                        }
                    }
                }
            }catch (InterruptedException e) {
                LOG.info("ParallelSink-->thread-->" + this.id + "**has encounter an InterruptedException-->" + e);
                e.printStackTrace();
            } catch (IOException e) {
                LOG.info("ParallelSink-->thread-->" + this.id + "**has encounter an IOException-->" + e);
                e.printStackTrace();
            }catch (Exception e){
                LOG.info("ParallelSink-->thread-->" + this.id + "**has encounter an Exception-->" + e);
                e.printStackTrace();
            }
        }
        for (BucketWriter bucketWriter : sfWriters.values()){
            try {
                String info = bucketWriter.getBucketPath();
                bucketWriter.close();
                LOG.info("ParallelSink-->thread-->" + this.id + " BucketWriter has been closed-->" + info);
            }catch (Exception e){
                e.printStackTrace();
            }
        }
        sfWriters.clear();
    }

    public String BucketWriterInfo(){
        BucketWriter bucketWriter = sfWriters.get("BucketWriter-" + this.id);
        return "shiftDate-->" + bucketWriter.getShiftedDate() + "**fileName-->" + bucketWriter.getBucketPath();
    }

    public void close(){
        this.closed = true;
    }
}
