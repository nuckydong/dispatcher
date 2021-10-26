package com.ebike;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;

/**
 * @author NiKo
 * @date 2021-10-26
 */
public class Config {
    private static Logger Log = LoggerFactory.getLogger(EbikeMqttClient.class);

    private static Properties properties = new Properties();

    static {
        InputStream inputStream = null;
        try {
            final String external_file = System.getProperty("user.dir") + File.separator + "config.properties";
            try {
                inputStream = new FileInputStream(external_file);
            } catch (FileNotFoundException e) {
                Log.info("The external config file : {} doesn't exists using inner package config", external_file);
                inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties");
            }
            if (inputStream != null) {
                Log.info("Properties config load success");
                properties.load(inputStream);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Log.error(e.getMessage(), e);
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                    Log.error(e.getMessage(), e);
                }
            }
        }
    }

    private Config() {

    }

    public static Properties getConfig() {
        return properties;
    }
}
