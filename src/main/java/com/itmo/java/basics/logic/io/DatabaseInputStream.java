package com.itmo.java.basics.logic.io;

import com.itmo.java.basics.logic.DatabaseRecord;
import com.itmo.java.basics.logic.WritableDatabaseRecord;
import com.itmo.java.basics.logic.impl.SetDatabaseRecord;
import com.itmo.java.basics.logic.impl.RemoveDatabaseRecord;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

/**
 * Класс, отвечающий за чтение данных из БД
 */
public class DatabaseInputStream extends DataInputStream {
    private static final int REMOVED_OBJECT_SIZE = -1;

    public DatabaseInputStream(InputStream inputStream) {
        super(inputStream);
    }

    /**
     * Читает следующую запись (см {@link DatabaseOutputStream#write(WritableDatabaseRecord)})
     * @return следующую запись, если она существует. {@link Optional#empty()} - если конец файла достигнут
     */
    public Optional<DatabaseRecord> readDbUnit() throws IOException {
        try {
            int keySize = readInt();
            byte[] key = new byte[keySize];
            read(key);

            int valueSize = readInt();
            if (valueSize == REMOVED_OBJECT_SIZE) {
                return Optional.of(new RemoveDatabaseRecord(key));
            }
            byte[] value = new byte[valueSize];
            read(value);


            return Optional.of(new SetDatabaseRecord(key, value));
        } catch (EOFException ext) {
            return Optional.empty();
        }
    }
}
