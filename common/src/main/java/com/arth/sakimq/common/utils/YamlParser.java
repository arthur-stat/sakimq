package com.arth.sakimq.common.utils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple YAML parser for loading configuration files
 */
public class YamlParser {

    /**
     * Parse a YAML file into a map of key-value pairs
     *
     * @param filePath Path to the YAML file
     * @return Map containing configuration key-value pairs
     * @throws IOException If an error occurs while reading the file
     */
    public static Map<String, String> parseYaml(String filePath) throws IOException {
        Map<String, String> configMap = new HashMap<>();
        parseYamlInternal(filePath, configMap);
        return configMap;
    }

    private static void parseYamlInternal(String filePath, Map<String, String> configMap) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            java.util.Deque<String> keyStack = new java.util.ArrayDeque<>();
            java.util.Deque<Integer> indentStack = new java.util.ArrayDeque<>();
            indentStack.push(-1);

            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty() || line.trim().startsWith("#")) {
                    continue;
                }

                int indent = 0;
                while (indent < line.length() && line.charAt(indent) == ' ') {
                    indent++;
                }

                String trimmedLine = line.trim();
                int colonIndex = trimmedLine.indexOf(':');
                
                if (colonIndex > 0) {
                    String key = trimmedLine.substring(0, colonIndex).trim();
                    String value = trimmedLine.substring(colonIndex + 1).trim();

                    while (indent <= indentStack.peek()) {
                        indentStack.pop();
                        if (!keyStack.isEmpty()) keyStack.pop();
                    }

                    String fullKey = key;
                    if (!keyStack.isEmpty()) {
                        fullKey = String.join(".", keyStack) + "." + key;
                    }

                    if (value.isEmpty()) {
                        // It's a parent key
                        keyStack.push(key);
                        indentStack.push(indent);
                    } else {
                         // Remove quotes if present
                        if ((value.startsWith("'") && value.endsWith("'")) ||
                                (value.startsWith("\"") && value.endsWith("\""))) {
                            value = value.substring(1, value.length() - 1);
                        }
                        configMap.put(fullKey, value);
                    }
                }
            }
        }
    }

    /**
     * Get an integer value from the config map, with a default fallback
     *
     * @param configMap    Config map
     * @param key          Config key
     * @param defaultValue Default value if key not found
     * @return Integer value or default
     */
    public static int getIntValue(Map<String, String> configMap, String key, int defaultValue) {
        String value = configMap.get(key);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                // Ignore invalid number format, return default
            }
        }
        return defaultValue;
    }

    /**
     * Get a string value from the config map, with a default fallback
     *
     * @param configMap    Config map
     * @param key          Config key
     * @param defaultValue Default value if key not found
     * @return String value or default
     */
    public static String getStringValue(Map<String, String> configMap, String key, String defaultValue) {
        String value = configMap.get(key);
        return value != null ? value : defaultValue;
    }

    /**
     * Get a boolean value from the config map, with a default fallback
     *
     * @param configMap    Config map
     * @param key          Config key
     * @param defaultValue Default value if key not found
     * @return Boolean value or default
     */
    public static boolean getBooleanValue(Map<String, String> configMap, String key, boolean defaultValue) {
        String value = configMap.get(key);
        if (value != null) {
            return Boolean.parseBoolean(value);
        }
        return defaultValue;
    }

    /**
     * Get a long value from the config map, with a default fallback
     *
     * @param configMap    Config map
     * @param key          Config key
     * @param defaultValue Default value if key not found or invalid
     * @return Long value or default
     */
    public static long getLongValue(Map<String, String> configMap, String key, long defaultValue) {
        String value = configMap.get(key);
        if (value != null) {
            try {
                return Long.parseLong(value);
            } catch (NumberFormatException e) {
                // Ignore invalid number format, return default
            }
        }
        return defaultValue;
    }
}
