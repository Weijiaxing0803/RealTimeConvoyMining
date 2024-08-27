package MineOnline.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class ConvoyTimeUtils {
    //Convert to timestamp based on time string
    public static long getTimeStamp(String time){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return date.getTime();
    }


    //Convert to time string based on timestamps
    public static String getTimeDate(long timeStamp){
        Date date = new Date(timeStamp);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sdf.format(date);
    }
}
