package com.itmo.java.basics.initialization.impl;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;

public class DatabaseServerInitializer implements Initializer {

    private final DatabaseInitializer databaseInitializer;

    public DatabaseServerInitializer(DatabaseInitializer databaseInitializer) {
        this.databaseInitializer = databaseInitializer;
    }

    /**
     * Если заданная в окружении директория не существует - создает ее
     * Добавляет информацию о существующих в директории базах, нацинает их инициалиализацию
     *
     * @param context контекст, содержащий информацию об окружении
     * @throws DatabaseException если произошла ошибка при создании директории, ее обходе или ошибка инициализации бд
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        var env = context.executionEnvironment();
        Path envPath = env.getWorkingPath();

        if(!Files.exists(envPath)) {
            try {
                Files.createDirectory(envPath);
            } catch (IOException ioext) {
                throw new DatabaseException("Can't find or create " + envPath.toString(), ioext);
            }
        }
        
        File[] dbDirList = envPath.toFile().listFiles();
        if(dbDirList == null) {
            throw new DatabaseException(envPath.toString() + " not a directory");
        }
  
        for(File dbDir : dbDirList) {
            var curContext =
                new DatabaseInitializationContextImpl(
                    dbDir.getName(),
                    envPath);
            this.databaseInitializer.perform(
                InitializationContextImpl
                .builder()
                .executionEnvironment(env)
                .currentDatabaseContext(curContext)
                .build()
                );
        }
    }
}
