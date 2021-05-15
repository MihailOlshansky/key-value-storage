package com.itmo.java.basics.initialization.impl;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.impl.DatabaseImpl;

public class DatabaseInitializer implements Initializer {

    private final TableInitializer tableInitializer;

    public DatabaseInitializer(TableInitializer tableInitializer) {
        this.tableInitializer = tableInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой бд.
     * Запускает инициализацию всех таблиц это базы
     *
     * @param initialContext контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к базе, невозможно прочитать содержимого папки,
     *  или если возникла ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext initialContext) throws DatabaseException {
        var curDBContext = initialContext.currentDbContext();
        String databaseName = curDBContext.getDbName();
        Path databasePath = curDBContext.getDatabasePath();

        if(!Files.exists(databasePath)) {
            throw new DatabaseException("Database " + databaseName + "(" + databasePath.toString() + ") doesn't exsist");
        }
        
        File[] tableDirList = databasePath.toFile().listFiles();
        
        if(tableDirList == null) {
            throw new DatabaseException(databasePath.toString() + " not a directory");
        }

        for(File tableDir : tableDirList) {
            var curContext =
                new TableInitializationContextImpl(
                    tableDir.getName(),
                    databasePath,
                    new TableIndex());
            this.tableInitializer.perform(
                InitializationContextImpl
                .builder()
                .currentDatabaseContext(curDBContext)
                .currentTableContext(curContext)
                .build()
            );
        }

        initialContext.executionEnvironment().addDatabase(DatabaseImpl.initializeFromContext(curDBContext));
    }
}
