package org.apache.flume.sink.hdfs;

import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedList;

public class ShiftDispatcher extends AbstractDispatcher {

    //the data of last day,which is the shifted thread's buffer
    public static final LinkedList<Event[]> normal = new LinkedList<>();
    //the BucketWriter of last day,which will be handled by the thread of last day.
    public volatile static LinkedList<BucketWriter> normalBucket = new LinkedList<>();
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDispatcher.class);
    private volatile String day;
    private volatile ParallelSinkThread[] parallelSinks;
    private volatile Thread[] threads;
    private volatile Thread[] shiftThreads;
    private volatile int lastDayThreadBeginIndex = 20;
    private int lastIndex;
    private volatile int currentIndex;
    private int step;

    public ShiftDispatcher(SinkParameter _parameter) {
        super(_parameter);
        parallelSinks = new ParallelSinkThread[SinkParameter.THREAD_SIZE + 2];
        threads = new Thread[SinkParameter.THREAD_SIZE];
        shiftThreads = new Thread[3];
    }

    @Override
    public ParallelSinkThread[] getShiftSink() {
        ParallelSinkThread[] shiftSink = new ParallelSinkThread[2];
        shiftSink[0] = parallelSinks[SinkParameter.THREAD_SIZE];
        shiftSink[0] = parallelSinks[SinkParameter.THREAD_SIZE + 1];
        return shiftSink;
    }

    @Override
    public ParallelSinkThread[] getParallelSink() {
        return parallelSinks;
    }

    @Override
    public void setCurrentDay() {
        SinkParameter.setStep(1);
        step = 1;
        //parallelSinks[SinkParameter.THREAD_SIZE] = null;
        //shiftThreads[0] = null;
        //parallelSinks[SinkParameter.THREAD_SIZE + 1] = null;
        //shiftThreads[1] = null;
        lastDayThreadBeginIndex = 20;
        super.setCurrentDay();
    }

    @Override
    public void setNextDay(boolean _nextDay) {
        LOG.info("ParallelSink-->setNextDay");
        //parallelSinks[SinkParameter.THREAD_SIZE] = new ParallelSinkThread(SinkParameter.THREAD_SIZE,parameter);
        //parallelSinks[SinkParameter.THREAD_SIZE + 1] = new ParallelSinkThread(SinkParameter.THREAD_SIZE + 1,parameter);
        parallelSinks[SinkParameter.THREAD_SIZE].setShiftDate(true);
        parallelSinks[SinkParameter.THREAD_SIZE + 1].setShiftDate(true);
        if ( !parameter.isNextDay() ) {
            //shiftThreads[0] = new Thread(parallelSinks[SinkParameter.THREAD_SIZE]);
            shiftThreads[0].start();
            //shiftThreads[1] = new Thread(parallelSinks[SinkParameter.THREAD_SIZE + 1]);
            shiftThreads[1].start();
        }
        Date now = new Date();
        while ( now.getHours() == 23 && now.getMinutes() == 59){
            try {
                Thread.sleep(1000);
            }catch (Exception e){
                e.printStackTrace();
            }
            now = new Date();
        }
        int d = now.getDate();
        if(d<10){
            day = "0" + d;
        }else {
            day = "" + d;
        }
        parameter.setNextDay(_nextDay);
        for (int i=0;i<SinkParameter.THREAD_SIZE;i++){
            parallelSinks[i].setShiftDate(true);
        }
        for (int i=0;i<SinkParameter.NUM_THREAD_SHIFT_ONE_TIME;i++){
            parallelSinks[i].setShifting(true);
        }
        lastDayThreadBeginIndex = SinkParameter.NUM_THREAD_SHIFT_ONE_TIME;
        lastIndex = lastDayThreadBeginIndex;
        currentIndex = 0;
        LOG.info("ParallelSink--> end setNextDay");
    }

    @Override
    public ParallelSinkThread getBuffer(boolean _nextDay) {
        if ( parameter.isNextDay() && step < SinkParameter.getStep()){
            int i;
            for ( i=lastDayThreadBeginIndex;i<lastDayThreadBeginIndex+(SinkParameter.getStep() - step) * SinkParameter.NUM_THREAD_SHIFT_ONE_TIME;i++){
                parallelSinks[i].setShifting(true);
            }
            step = SinkParameter.getStep();
            lastDayThreadBeginIndex = i ;
            lastIndex = lastDayThreadBeginIndex;
        }
        ParallelSinkThread parallelSinkThread = null;
        if ( !_nextDay ) {
            while ( parallelSinkThread == null || parallelSinkThread.size() >= SinkParameter.HANDLE_BATCH ){
                parallelSinkThread = parallelSinks[currentIndex++];
                if (currentIndex == lastDayThreadBeginIndex){
                    currentIndex = 0;
                }
            }
        } else {
            while ( parallelSinkThread == null || parallelSinkThread.size() >= SinkParameter.HANDLE_BATCH ){
                parallelSinkThread = parallelSinks[lastIndex++];
                if ( lastIndex == ( SinkParameter.THREAD_SIZE + 2 ) ){
                    lastIndex = lastDayThreadBeginIndex;
                }
            }
        }
        return parallelSinkThread;
    }

    @Override
    public boolean checkIfShift(Event event) {
        try {
            String data = new String(event.getBody(), 0, event.getBody().length);
            String da = data.substring(data.indexOf('-')+4).substring(0,2);
            if (!da.equals(day)) {
                return true;
            }
        }catch (Exception e){
            LOG.error(e.getLocalizedMessage());
        }
        return false;
    }

    @Override
    public void stop() {
        for (int i=0;i< SinkParameter.THREAD_SIZE+2;i++){
            if (parallelSinks[1] != null){
                parallelSinks[i].close();
            }
        }
    }

    @Override
    public void start() {
        //start sink threads
        for(int i=0;i<SinkParameter.THREAD_SIZE;i++){
            parallelSinks[i] = new ParallelSinkThread(i,parameter);
            threads[i] = new Thread(parallelSinks[i]);
            threads[i].start();
        }
        LOG.info("ParallelSink-->sink threads started!");
        //start controller
        parallelSinks[SinkParameter.THREAD_SIZE] = new ParallelSinkThread(SinkParameter.THREAD_SIZE,parameter);
        shiftThreads[0] = new Thread(parallelSinks[SinkParameter.THREAD_SIZE]);
        parallelSinks[SinkParameter.THREAD_SIZE + 1] = new ParallelSinkThread(SinkParameter.THREAD_SIZE + 1,parameter);
        shiftThreads[1] = new Thread(parallelSinks[SinkParameter.THREAD_SIZE + 1]);
        shiftThreads[2] = new Thread(parameter.getController());
        shiftThreads[2].start();
        LOG.info("ParallelSink-->controller threads started!");
    }

    @Override
    public void printThreadStatus(){
        for (int i=0;i<SinkParameter.THREAD_SIZE;i++ ) {
            if ( parallelSinks[i] != null && threads[i] != null ){
                LOG.info("ParallelSink-->ThreadStatus-->thread-->" + parallelSinks[i].getId() + "**is running-->" + threads[i].getState()
                + "**ParallelSinkThread is closed-->" + parallelSinks[i].isClosed() + "**shiftDate-->" + parallelSinks[i].isShiftDate()
                        + "**BucketWriter info-->" + parallelSinks[i].BucketWriterInfo());
            }
        }
        if (parallelSinks[SinkParameter.THREAD_SIZE] != null && shiftThreads[0] != null){
            LOG.info("ParallelSink-->ThreadStatus-->thread-->" + parallelSinks[SinkParameter.THREAD_SIZE].getId() + "**is running-->" + shiftThreads[0].getState()
                    + "**ParallelSinkThread is closed-->" + parallelSinks[SinkParameter.THREAD_SIZE].isClosed() + "**shiftDate-->" + parallelSinks[SinkParameter.THREAD_SIZE].isShiftDate()
                    + "**BucketWriter info-->" + parallelSinks[SinkParameter.THREAD_SIZE].BucketWriterInfo());
        }
        if (parallelSinks[SinkParameter.THREAD_SIZE+1] != null && shiftThreads[1] != null){
            LOG.info("ParallelSink-->ThreadStatus-->thread-->" + parallelSinks[SinkParameter.THREAD_SIZE+1].getId() + "**is running-->" + shiftThreads[1].getState()
                    + "**ParallelSinkThread is closed-->" + parallelSinks[SinkParameter.THREAD_SIZE+1].isClosed() + "**shiftDate-->" + parallelSinks[SinkParameter.THREAD_SIZE+1].isShiftDate()
                    + "**BucketWriter info-->" + parallelSinks[SinkParameter.THREAD_SIZE+1].BucketWriterInfo());
        }
    }
}
