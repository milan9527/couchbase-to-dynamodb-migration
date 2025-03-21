package com.migration.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Utility class to load configuration from properties file
 */
public class ConfigLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);
    private static final String CONFIG_FILE = "application.properties";
    private static Properties properties;

    static {
        properties = new Properties();
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                LOG.error("Unable to find {}", CONFIG_FILE);
                throw new RuntimeException("Unable to find " + CONFIG_FILE);
            }
            properties.load(input);
            LOG.info("Configuration loaded successfully");
        } catch (IOException ex) {
            LOG.error("Error loading configuration", ex);
            throw new RuntimeException("Error loading configuration", ex);
        }
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue) {
        return properties.getProperty(key, defaultValue);
    }

    public static int getIntProperty(String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            LOG.warn("Invalid integer value for {}: {}, using default: {}", key, value, defaultValue);
            return defaultValue;
        }
    }
}
