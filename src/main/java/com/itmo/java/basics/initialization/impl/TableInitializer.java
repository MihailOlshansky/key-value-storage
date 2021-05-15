package com.itmo.java.basics.initialization.impl;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.impl.TableImpl;

public class TableInitializer implements Initializer {
    private final SegmentInitializer segmentInitializer;

    public TableInitializer(SegmentInitializer segmentInitializer) {
        this.segmentInitializer = segmentInitializer;
    }

    /**
     * Добавляет в контекст информацию об инициализируемой таблице.
     * Запускает инициализацию всех сегментов в порядке их создания (из имени)
     *
     * @param context контекст с информацией об инициализируемой бд, окружении, таблицы
     * @throws DatabaseException если в контексте лежит неправильный путь к таблице, невозможно прочитать содержимого папки,
     *  или если возникла ошибка ошибка дочерних инициализаторов
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        TableInitializationContext curTableContext = context.currentTableContext();
        String tableName = curTableContext.getTableName();
        Path tablePath = curTableContext.getTablePath();

        if(!Files.exists(tablePath)) {
            throw new DatabaseException("Table " + tableName + "(" + tablePath.toString() + ") doesn't exsist");
        }
        
        var segmentFileList = tablePath.toFile().listFiles();

        if(segmentFileList == null) {
            throw new DatabaseException(tablePath + " not a directory");
        }

        Arrays.sort(segmentFileList);

        for(File segmentFile : segmentFileList) {
            var curContext =
                new SegmentInitializationContextImpl(
                    segmentFile.getName(),
                    tablePath,
                    0);

            this.segmentInitializer.perform(
                InitializationContextImpl
                .builder()
                .currentTableContext(curTableContext)
                .currentSegmentContext(curContext)
                .build()
            );
        }


        context.currentDbContext().addTable(TableImpl.initializeFromContext(curTableContext));
    }
}
