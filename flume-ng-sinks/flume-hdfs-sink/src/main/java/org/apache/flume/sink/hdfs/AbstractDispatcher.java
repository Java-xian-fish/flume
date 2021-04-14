package org.apache.flume.sink.hdfs;

import org.apache.flume.Event;

public abstract class AbstractDispatcher {

    protected volatile SinkParameter parameter;
    private ParallelSinkThread currentDayBuffer;
    private ParallelSinkThread lastDayBuffer;
    private int ratioCount = 0;
    private int ratioSumCount = 0;
    private volatile double ratio = 1.0;

    public AbstractDispatcher(SinkParameter _parameter){
        this.parameter = _parameter;
    }

    public abstract ParallelSinkThread[] getShiftSink();

    public abstract ParallelSinkThread[] getParallelSink();

    public void setCurrentDay(){
        this.ratioCount = 0;
        this.ratioSumCount = 0;
        this.ratio = 1.0;
        parameter.setNextDay(false);
    }

    public abstract void setNextDay(boolean _nextDay);

    /*
     *This method must be override.
     * _nextDay is standard for getting which day's buffer
     * false: the current day;true:the last day;
     * if the nextDay is false, the _nextDay is always false;
     */
    public abstract ParallelSinkThread getBuffer(boolean _nextDay);

    public void dispatch(Event e){
        if (!parameter.isNextDay()){
            while( currentDayBuffer == null || currentDayBuffer.size() >= SinkParameter.HANDLE_BATCH) {
                currentDayBuffer = getBuffer(false);
            }
            currentDayBuffer.add(e);
        }else {
            boolean checkResult = checkIfShift(e);
            if (!checkResult){
                while( currentDayBuffer == null || currentDayBuffer.size() >= SinkParameter.HANDLE_BATCH) {
                    currentDayBuffer = getBuffer(false);
                }
                currentDayBuffer.add(e);
            }else{
                while( lastDayBuffer == null || lastDayBuffer.size() >= SinkParameter.HANDLE_BATCH) {
                    lastDayBuffer = getBuffer(true);
                }
                lastDayBuffer.add(e);
                ratioCount++;
            }
            ratioSumCount++;
            /*
             *The dispatcher is responsible for computing the ratio.
             *The controller determines which time to shift step;
             */
            if ( ratioSumCount == SinkParameter.SUM_NUM ){
                ratio = ratioCount / (double) ratioSumCount;
                parameter.getController().add(ratio);
                parameter.setRatio(ratio);
                parameter.setRatioCount(ratioCount);
                parameter.setRatioSumCount(ratioSumCount);
                ratioSumCount = 0;
                ratioCount = 0;
            }
        }

    }

    /*
     *This method must be override.
     * True: the data belongs to last day.False:it's today's data.
     */
    public abstract boolean checkIfShift(Event event);

    public abstract void stop();

    public abstract  void start();

    public abstract void printThreadStatus();
}
