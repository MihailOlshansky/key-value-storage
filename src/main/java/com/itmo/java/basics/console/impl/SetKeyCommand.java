package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;

/**
 * Команда для создания записи значения
 */
public class SetKeyCommand implements DatabaseCommand {
    private static final int NUM_OF_ARGS = 6;

    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;

    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, таблицы, ключ, значение
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public SetKeyCommand(ExecutionEnvironment env, List<RespObject> commandArgs) {
        if (commandArgs.size() < NUM_OF_ARGS) {
            throw new IllegalArgumentException("Not enough arguments to set key");
        }
        if (commandArgs.size() > NUM_OF_ARGS) {
            throw new IllegalArgumentException("Too much arguments to set key");
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
     * Записывает значение
     *
     * @return {@link DatabaseCommandResult#success(byte[])} c предыдущим значением. Например, "previous" или null, если такого не было
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            String tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            String objectKey = commandArgs.get(DatabaseCommandArgPositions.KEY.getPositionIndex()).asString();
            byte[] value = commandArgs.get(DatabaseCommandArgPositions.VALUE.getPositionIndex()).asString().getBytes();
            var prevValue = env.getDatabase(dbName).get().read(tableName, objectKey).get();
            env.getDatabase(dbName).get().write(tableName, objectKey, value);
            return DatabaseCommandResult.success(prevValue);
        } catch (DatabaseException dbext) {
            return DatabaseCommandResult.success(null);
        } catch (Exception ext) {
            return DatabaseCommandResult.error("Can't set key's value");
        }
    }
}
