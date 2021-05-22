package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

/**
 * Команда для создания бд
 */
public class CreateDatabaseKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_DATABASE";

    private final String dbName;
    /**
     * Создает объект
     *
     * @param databaseName имя базы данных
     */
    public CreateDatabaseKvsCommand(String databaseName) {
        this.dbName = databaseName;
    }

    /**
     * Возвращает RESP объект. {@link RespArray} с {@link RespCommandId}, именем команды, аргументами в виде {@link RespBulkString}
     *
     * @return объект
     */
    @Override
    public RespArray serialize() {
        return new RespArray(
            new RespCommandId(idGen.intValue()),
            new RespBulkString(COMMAND_NAME.getBytes()),
            new RespBulkString(dbName.getBytes())
        );
    }

    @Override
    public int getCommandId() {
        return idGen.intValue();
    }
}
