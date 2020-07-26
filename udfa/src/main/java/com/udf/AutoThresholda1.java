package com.udf;



import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AutoThresholda1 extends UDF {

    static Logger LOGGER = LoggerFactory.getLogger(AutoThresholda1.class.getName());
    private ArrayList<Float> diffList = new ArrayList(50);
    private Integer group = 0;
    private String begin_time0 = "";

    public AutoThresholda1() {
    }

    public String evaluate(String begin_time, String _thresholdperdiff) {
        int thresholdperdiff = Integer.parseInt(_thresholdperdiff);
        if (this.begin_time0.length() == 0) {
            this.begin_time0 = begin_time;
        }

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        try {
            Date format_bt = format.parse(begin_time.substring(0, 23));
            Date format_bt0 = format.parse(this.begin_time0.substring(0, 23));
            long diff = format_bt.getTime() - format_bt0.getTime();
            if (diff > (long)thresholdperdiff) {
                this.group = this.group + 1;
                this.begin_time0 = begin_time;
            }
        } catch (Exception e) {
            LOGGER.error("parse filed");
        }

        return this.group.toString();
    }
}
