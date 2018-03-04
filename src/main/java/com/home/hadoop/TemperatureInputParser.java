package com.home.hadoop;


import org.apache.hadoop.io.Text;

public class TemperatureInputParser {

    private static final int MISSING = 9999;

    private String year;
    private int airTemperature;
    private String quality;

    public void parse(String value) {

        year = value.substring(15, 19);
        if (value.charAt(87) == '+') {
            airTemperature = Integer.parseInt(value.substring(88, 92));
        } else {
            airTemperature = Integer.parseInt(value.substring(87, 92));
        }
        quality = value.substring(92, 93);

    }

    public void parse(Text record) {
        parse(record.toString());
    }

    public boolean isValidTemperature() {
        return airTemperature != MISSING && quality.matches("[01459]");
    }

    public String getYear() {
        return year;
    }

    public int getAirTemperature() {
        return airTemperature;
    }
}
