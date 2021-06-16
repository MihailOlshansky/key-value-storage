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
 * Команда для создания базы таблицы
 */
public class CreateTableCommand implements DatabaseCommand {

    private static final int NUM_OF_ARGS = 4;

    private final ExecutionEnvironment env;
    private final List<RespObject> commandArgs;

    /**
     * Создает команду
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду. Только формальные признаки (например, количество переданных значений или ненуловость объектов
     *
     * @param env         env
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя бд, имя таблицы
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateTableCommand(ExecutionEnvironment env, List<RespObject> commandArgs) throws IllegalArgumentException {
        if (commandArgs.size() < NUM_OF_ARGS) {
            throw new IllegalArgumentException("Not enough arguments to create table");
        }
        if (commandArgs.size() > NUM_OF_ARGS) {
            throw new IllegalArgumentException("Too much arguments to create table");
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
     * Создает таблицу в нужной бд
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная таблица была создана. Например, "Table table1 in database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            String tableName = commandArgs.get(DatabaseCommandArgPositions.TABLE_NAME.getPositionIndex()).asString();
            Optional<Database> database = env.getDatabase(dbName);
            if (database.isEmpty()) {
                throw new DatabaseException("No database with name " + dbName);
            }
            database.get().createTableIfNotExists(tableName);
            return DatabaseCommandResult.success(("Table " + tableName + " created successfully").getBytes());
        } catch (Exception ext) {
            return DatabaseCommandResult.error("Can't create table because " + ext.getMessage());
        }
    }
}
