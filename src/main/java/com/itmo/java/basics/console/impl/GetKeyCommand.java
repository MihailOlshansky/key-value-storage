package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;
import java.util.Optional;

/**
 * Команда для чтения данных по ключу
 */
public class GetKeyCommand implements DatabaseCommand {
    private static final int NUM_OF_ARGS = 5;

    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public GetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.size() < NUM_OF_ARGS) {
            throw new IllegalArgumentException("Not enough arguments to get value by key");
        }
        if (commandArgs.size() > NUM_OF_ARGS) {
            throw new IllegalArgumentException("Too much arguments to get value by key");
        }

        for (var object : commandArgs) {
            if (object == null) {
                throw new IllegalArgumentException("Some arguments are null");
            }
        }

        this.env = env;
        this.commandArgs = commandArgs;
    }

    /**
     * Читает значение по ключу
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с прочитанным значением. Например, "previous". Null, если такого нет
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            String tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            String objectKey = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();

            Optional<Database> database = env.getDatabase(dbName);
            if (database.isEmpty()) {
                throw new DatabaseException("No database with name " + dbName);
            }
            Optional<byte[]> value = database.get().read(tableName, objectKey);
            if (value.isEmpty()) {
                return DatabaseCommandResult.success(null);
            }
            return DatabaseCommandResult.success(value.get());
        } catch (Exception ext) {
            return DatabaseCommandResult.error("Can't get key's value, because" + ext.getMessage());
        }
    }
}
