package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.SegmentInitializationContext;
import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.Segment;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.io.DatabaseInputStream;
import com.itmo.java.basics.logic.io.DatabaseOutputStream;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.index.SegmentOffsetInfo;
import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.index.impl.SegmentOffsetInfoImpl;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

/**
 * Сегмент - append-only файл, хранящий пары ключ-значение, разделенные специальным символом.
 * - имеет ограниченный размер, большие значения (>100000) записываются в последний сегмент, если он не read-only
 * - при превышении размера сегмента создается новый сегмент и дальнейшие операции записи производятся в него
 * - именование файла-сегмента должно позволять установить очередность их появления
 * - является неизменяемым после появления более нового сегмента
 */
public class SegmentImpl implements Segment {

    private final String segmentName;
    private final Path segmentPath;
    private SegmentOffsetInfoImpl actualOffset;
    private final SegmentIndex segmentIndex;
    private final long maxOffset = 100_000;

    private SegmentImpl(String segmentName, Path path) throws DatabaseException {
        this.segmentName = segmentName;
        this.segmentPath = path;
        this.actualOffset = new SegmentOffsetInfoImpl(0);
        this.segmentIndex = new SegmentIndex();
    }

    public static Segment create(String segmentName, Path tableRootPath) throws DatabaseException {
        Path path = Paths.get(tableRootPath.toString(), segmentName);
        
        if (Files.exists(path)) {
            throw new DatabaseException("Segment " + segmentName + " already exists");
        }
        try {
            Files.createFile(path);
        } catch (IOException ioext) {
            throw new DatabaseException("Can't create file " + path.toString(), ioext);
        }

        return new SegmentImpl(segmentName, path); // todo implement
    }

    public static Segment initializeFromContext(SegmentInitializationContext context) {
        return null;
    }

    static String createSegmentName(String tableName) {
        return tableName + "_" + System.currentTimeMillis();
    }

    @Override
    public String getName() {
        return this.segmentName;
    }

    @Override
    public boolean write(String objectKey, byte[] objectValue) throws IOException {
        if (objectValue == null) {
            return delete(objectKey);
        }

        return writeInfoToFile(new SetDatabaseRecord(objectKey.getBytes(), objectValue), actualOffset);
    }

    @Override
    public Optional<byte[]> read(String objectKey) throws IOException {
        Optional<SegmentOffsetInfo> osoi = segmentIndex.searchForKey(objectKey);
        
        if (!osoi.isPresent()) {
            return Optional.empty();
        }

        try (DatabaseInputStream dbis = new DatabaseInputStream(new FileInputStream(segmentPath.toString()))) {
            dbis.skip(osoi.get().getOffset());
            Optional<DatabaseRecord> odbr = dbis.readDbUnit();
            return odbr.map(DatabaseRecord::getValue);
        }
    }

    @Override
    public boolean isReadOnly() {
        return actualOffset.getOffset() >= maxOffset ;
    }

    @Override
    public boolean delete(String objectKey) throws IOException {
        return writeInfoToFile(new RemoveDatabaseRecord(objectKey.getBytes()), null);
    }

    private boolean writeInfoToFile(WritableDatabaseRecord wdbr, SegmentOffsetInfo soi) throws IOException {
        if (isReadOnly()) {
            return false;
        }

        try(DatabaseOutputStream dbos = new DatabaseOutputStream(new FileOutputStream(segmentPath.toString(), true))) {
            dbos.write(wdbr);
        }
        segmentIndex.onIndexedEntityUpdated(new String(wdbr.getKey()), soi);
        actualOffset = new SegmentOffsetInfoImpl(actualOffset.getOffset() + wdbr.size());

        return true;
    }
}
