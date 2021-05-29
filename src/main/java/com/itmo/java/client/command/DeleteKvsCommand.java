package com.itmo.java.client.command;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;

public class DeleteKvsCommand implements KvsCommand {
    private static final String COMMAND_NAME = "DELETE_KEY";

    private final String dbName;
    private final String tableName;
    private final String key;

    public DeleteKvsCommand(String databaseName, String tableName, String key) {
        this.dbName = databaseName;
        this.tableName = tableName;
        this.key = key;
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
            new RespBulkString(tableName.getBytes()),
            new RespBulkString(key.getBytes())
        );
    }

    @Override
    public int getCommandId() {
        return idGen.intValue();
    }
}
