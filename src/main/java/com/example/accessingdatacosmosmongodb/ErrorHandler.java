package com.example.accessingdatacosmosmongodb;

import com.mongodb.MongoCommandException;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ErrorHandler {
    public static boolean IsThrottle(MongoCommandException ex)
    {
        if(ex.getCode() == 16500)
        {
            return true;
        }
        return false;
    }

    public static boolean IsThrottle(String strException)
    {
        return strException.toLowerCase().contains("Request rate is large".toLowerCase());
    }

    // TODO : Use this method to know exact time to wait in 3.6 version
    public int extractRetryDuration(String message) throws Exception {

        Pattern pattern = Pattern.compile("RetryAfterMs=([0-9]+)");

        Matcher matcher = pattern.matcher(message);



        int retryAfter = 0;



        if (matcher.find()) {

            retryAfter = Integer.parseInt(matcher.group(1));

        }



        if (retryAfter <= 0) {

            throw new Exception("Invalid retryAfterMs from the cosmos db error message: " + message);

        }



        return retryAfter;

    }

}
