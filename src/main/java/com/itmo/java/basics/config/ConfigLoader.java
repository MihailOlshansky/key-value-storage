    package com.itmo.java.basics.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {

    public static String DEFAULT_FILE_NAME = "server.properties";
    
    private final String configFilePath;

    private final String kvsWorkingPathKey = "kvs.workingPath";
    private final String kvsHostKey = "kvs.host";
    private final String kvsPortKey = "kvs.port";

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        configFilePath = DEFAULT_FILE_NAME;
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        configFilePath = name;
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     * @throws FileNotFoundException
     */
    public DatabaseServerConfig readConfig() {
        Properties properties = new Properties();
        try (InputStream is = this.getClass().getClassLoader().getResourceAsStream(configFilePath)) {
            properties.load(is);
        } catch (Exception ext) {
            ext.printStackTrace();
        }
        
        try (InputStream is = new FileInputStream(configFilePath)) {
            properties.load(is);
        } catch (Exception ext) {
            ext.printStackTrace();
        }

        String workingPath = properties.getProperty(kvsWorkingPathKey, DatabaseConfig.DEFAULT_WORKING_PATH);
        String host = properties.getProperty(kvsHostKey, ServerConfig.DEFAULT_HOST);
        String port = properties.getProperty(kvsPortKey, String.valueOf(ServerConfig.DEFAULT_PORT));

        return
        DatabaseServerConfig
            .builder()
            .dbConfig(new DatabaseConfig(workingPath))
            .serverConfig(new ServerConfig(host, Integer.parseInt(port)))
            .build();
    }

}
