package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

/**
 * Команда для создания таблицы
 */
public class CreateTableKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "CREATE_TABLE";

    private final String dbName;
    private final String tableName;

    public CreateTableKvsCommand(String databaseName, String tableName) {
        this.dbName = databaseName;
        this.tableName = tableName;
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
            new RespBulkString(dbName.getBytes()),
            new RespBulkString(tableName.getBytes())
        );
    }

    @Override
    public int getCommandId() {
        return idGen.intValue();
    }
}
