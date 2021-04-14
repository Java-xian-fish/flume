package org.apache.flume.sink.hdfs;

import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.LinkedList;

public class ShiftController implements Runnable{

    private boolean close = false;
    private volatile LinkedList<Double> ratios = new LinkedList<>();
    private static final Logger LOG = LoggerFactory.getLogger(ShiftController.class);
    private static final long LOG_COUNT = 100_000_000;
    private AbstractDispatcher dispatcher;
    private SinkParameter parameter;
    private double ratio = 1.0;
    private int makeSure = 0;
    private long lastTimeStamp;
    private long count = 0l;
    private long timeCount = 0l;

    public ShiftController(AbstractDispatcher _dispatcher, SinkParameter _parameter){
        this.dispatcher = _dispatcher;
        this.parameter = _parameter;
        this.lastTimeStamp = System.currentTimeMillis();
        this.timeCount = System.currentTimeMillis() / SinkParameter.TIME_COUNT;
    }

    public void add(Double _double){
        ratios.offer(_double);
    }

    @Override
    public void run() {
        LOG.info("ParallelSink-->ShiftController is started!" );
        boolean nextDay = false;
        while(!close){
            try {
                //1、main task,check if it's time to shift.
                Date date = new Date();
                if ( date.getHours() == 23 && date.getMinutes() == 59){
                    //there is a condition that the process already is having fault,the data is late.So we have to shift the threads back;
                    dispatcher.setNextDay(true);
                    nextDay = true;
                    LOG.info("ParallelSink-->ShiftController shift handle is started!" );
                    count++;
                }

                //2、the shifting rate is lower than what is enough to end,it's dispatcher to end first.
                if ( nextDay && !parameter.isNextDay()){
                    ParallelSinkThread[] shiftSinks = dispatcher.getShiftSink();
                    while (!ShiftDispatcher.normal.isEmpty()) {
                        Event[] events = ShiftDispatcher.normal.poll();
                        for (int i=0;i<events.length;i++){
                            if (events[i] != null){
                                shiftSinks[i%2].add(events[i]);
                            }
                        }
                    }
                    shiftSinks[0].close();
                    shiftSinks[1].close();
                    while (!ShiftDispatcher.normalBucket.isEmpty()){
                        try {
                            ShiftDispatcher.normalBucket.poll().close();
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }
                    dispatcher.setCurrentDay();
                    nextDay = false;
                    ratio = 1.0;
                    ratios.clear();
                    LOG.info("ParallelSink-->ShiftController shift handle is ended!" );
                }

                //3、count the ratio,and determines which time to update step,which control thread handle last day's data or current day's.
                while ( nextDay && !ratios.isEmpty()){
                    double _ratio = ratios.poll();
                    double baseRatio = 1.0 - SinkParameter.getStep() * ( 1.0 / SinkParameter.STEPS );
                    if ( ratio > baseRatio && _ratio < baseRatio ){
                        if ( makeSure > 10 ){
                            SinkParameter.setStep( SinkParameter.getStep() + 1 );
                            ratio = Math.max(_ratio, ( 1.0 - ( SinkParameter.getStep() - 0.5 ) * ( 1.0 / SinkParameter.STEPS )));
                            LOG.info("ParallelSink-->shift step-->" + SinkParameter.getStep() + "**ratio is-->"
                            + ratio + "**_ratio is-->" + _ratio + "**ratioCount/ratioSumCount-->" + parameter.getRatioCount() +
                            "/" + parameter.getRatioSumCount());
                        }
                        makeSure++;
                    }else {
                        ratio = _ratio;
                        makeSure = 0;
                    }
                    if( ratio > SinkParameter.END_RATIO && _ratio < SinkParameter.END_RATIO && SinkParameter.getStep() == SinkParameter.STEPS) {
                        if (makeSure < 10) {
                            makeSure++;
                        }else {
                            nextDay = false;
                            LOG.info("ParallelSink-->process END_RATIO is over,shift handle is over!-->ratio is-->"
                                    + ratio + "**_ratio is-->" + _ratio + "**ratioCount/ratioSumCount-->" + parameter.getRatioCount() +
                                    "/" + parameter.getRatioSumCount());
                            ratios.clear();
                            break;
                        }
                    }else {
                        ratio = _ratio;
                        makeSure = 0;
                    }
                }

                //4、logging out data has been handled,and count the speed of the sink.This happens every 100_000_000
                if ( parameter.getCount() % LOG_COUNT < parameter.getBatchSize() && parameter.getCount() > parameter.getBatchSize() ){
                    ParallelSinkThread[] parallelSink = dispatcher.getParallelSink();
                    long sum = 0;
                    for (int i=0;i<SinkParameter.THREAD_SIZE+2;i++){
                        if ( parallelSink[i] != null ){
                            sum = sum + parallelSink[i].getCount();
                            LOG.info("ParallelSink-->ShiftController-->this batch thread-->" + parallelSink[i].getId() + "**has get data-->" + parallelSink[i].getCount() + "**this batch-->" + parallelSink[i].getBatchCount());
                        }
                    }
                    long time = ( System.currentTimeMillis() - lastTimeStamp ) / 1000;
                    lastTimeStamp = System.currentTimeMillis();
                    long thisBatchCount = sum - count;
                    LOG.info("ParallelSink-->ShiftController-->Dispatch sum is-->" + parameter.getCount());
                    LOG.info("ParallelSink-->ShiftController-->The Sum Of Threads count is-->" + sum );
                    LOG.info("ParallelSink-->ShiftController-->This Flume handle-->" + thisBatchCount + "**take time-->" + time + "**speed-->" + (thisBatchCount/time) + "**条/秒");
                    count = sum;
                }

                //5、log thread info
                if ( System.currentTimeMillis() / SinkParameter.TIME_COUNT > timeCount ){
                    timeCount = System.currentTimeMillis() / SinkParameter.TIME_COUNT;
                    dispatcher.printThreadStatus();
                }
            }catch (Exception e){
                LOG.error("ParallelSink-->ShiftController" +e);
                e.printStackTrace();
            }
        }
        LOG.info("ParallelSink-->ShiftController is ended!" );
    }

    public void close(){
        this.close = true;
    }
}
