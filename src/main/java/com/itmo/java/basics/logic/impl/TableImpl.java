package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.initialization.TableInitializationContext;
import com.itmo.java.basics.logic.Table;
import com.itmo.java.basics.logic.Segment;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Таблица - логическая сущность, представляющая собой набор файлов-сегментов, которые объединены одним
 * именем и используются для хранения однотипных данных (данных, представляющих собой одну и ту же сущность,
 * например, таблица "Пользователи")
 * <p>
 * - имеет единый размер сегмента
 * - представляет из себя директорию в файловой системе, именованную как таблица
 * и хранящую файлы-сегменты данной таблицы
 */
public class TableImpl implements Table {
    private final String tableName;
    private final Path pathToTable;
    private final TableIndex tableIndex;
    private Segment actualSegment = null;

    private TableImpl(String tableName, Path path, TableIndex tableIndex) throws DatabaseException {
        this.tableName = tableName;
        this.pathToTable = path;
        this.tableIndex = tableIndex;
        this.actualSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), path);
    }

    static Table create(String tableName, Path pathToDatabaseRoot, TableIndex tableIndex) throws DatabaseException {
        Path path = Paths.get(pathToDatabaseRoot.toString(), tableName);
        
        if (Files.exists(path)) {
            throw new DatabaseException("Table " + tableName + " already exists");
        }
        try {
            Files.createDirectory(path);
        } catch (IOException ioext) {
            throw new DatabaseException("Can't create directory " + path.toString(), ioext);
        }

        return new TableImpl(tableName, path, tableIndex);
    }

    public static Table initializeFromContext(TableInitializationContext context) {
        return null;
    }
    
    @Override
    public String getName() {
        return this.tableName;
    }

    @Override
    public void write(String objectKey, byte[] objectValue) throws DatabaseException {
        try {
            if (actualSegment.isReadOnly()) {
                actualSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), pathToTable);
            }
            actualSegment.write(objectKey, objectValue);
            tableIndex.onIndexedEntityUpdated(objectKey, actualSegment);
        } catch (IOException ioext) {
            throw new DatabaseException("Can't write pair to file in folder " + pathToTable.toString(), ioext);
        }
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws DatabaseException {
        Optional<Segment> os = tableIndex.searchForKey(objectKey);
        if (os.isPresent()) {
            try {
                return os.get().read(objectKey);
            } catch (IOException ioext) {
                throw new DatabaseException("Can't read value by key " + objectKey, ioext);
            }
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void delete(String objectKey) throws DatabaseException {
        try {
            if (actualSegment.isReadOnly()) {
                actualSegment = SegmentImpl.create(SegmentImpl.createSegmentName(tableName), pathToTable);
            }
            actualSegment.delete(objectKey);
            tableIndex.onIndexedEntityUpdated(objectKey, null);
        } catch (IOException ioext) {
            throw new DatabaseException("Can't delete pair in file in folder " + pathToTable.toString(), ioext);
        }
    }
}
