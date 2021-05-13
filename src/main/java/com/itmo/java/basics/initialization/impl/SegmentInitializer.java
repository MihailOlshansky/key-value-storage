package com.itmo.java.basics.initialization.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;
import com.itmo.java.basics.initialization.InitializationContext;
import com.itmo.java.basics.initialization.Initializer;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.impl.SegmentImpl;
import com.itmo.java.basics.logic.io.DatabaseInputStream;


public class SegmentInitializer implements Initializer {

    /**
     * Добавляет в контекст информацию об инициализируемом сегменте.
     * Составляет индекс сегмента
     * Обновляет инфу в индексе таблицы
     *
     * @param context контекст с информацией об инициализируемой бд и об окружении
     * @throws DatabaseException если в контексте лежит неправильный путь к сегменту, невозможно прочитать содержимое. Ошибка в содержании
     */
    @Override
    public void perform(InitializationContext context) throws DatabaseException {
        var curContext = context.currentSegmentContext();
        String segmentName = curContext.getSegmentName();
        Path segmentPath = curContext.getSegmentPath();
        int currentSize = 0;
        SegmentIndex index = curContext.getIndex();

        if(!Files.exists(segmentPath)) {
            throw new DatabaseException("Segment " + segmentName + "(" + segmentPath.toString() + ") doesn't exsist");
        }

        List<String> keyList = new LinkedList<>();

        try (DatabaseInputStream dbis = new DatabaseInputStream(new FileInputStream(segmentPath.toString()))) {
            Optional<DatabaseRecord> odbr = dbis.readDbUnit();
            while(odbr.isPresent()) {
                index.onIndexedEntityUpdated(new String(odbr.get().getKey()), new SegmentOffsetInfoImpl(currentSize));
                keyList.add(new String(odbr.get().getKey()));
                currentSize += odbr.get().size();
                odbr = dbis.readDbUnit();
            }
        } catch (IOException ioext) {
            throw new DatabaseException("Problems with reading file " + segmentPath.toString(), ioext);
        }

        var currentSegment = SegmentImpl.initializeFromContext(
            new SegmentInitializationContextImpl(
                curContext.getSegmentName(),
                curContext.getSegmentPath(),
                currentSize,
                index));
        
        for(var key : keyList) {
            context.currentTableContext().getTableIndex().onIndexedEntityUpdated(
                key,
                currentSegment);
        }
        context.currentTableContext().updateCurrentSegment(currentSegment);
    }
}
