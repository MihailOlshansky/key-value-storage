package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.logic.DatabaseFactory;
import com.itmo.java.protocol.model.RespObject;

import java.util.List;

/**
 * Команда для создания базы данных
 */
public class CreateDatabaseCommand implements DatabaseCommand {

    private static final int NUM_OF_ARGS = 3;

    private final ExecutionEnvironment env;
    private final DatabaseFactory factory;
    private final List<RespObject> commandArgs;
    /**
     * Создает команду.
     * <br/>
     * Обратите внимание, что в конструкторе нет логики проверки валидности данных. Не проверяется, можно ли исполнить команду.
     * Только формальные признаки (например, количество переданных значений или ненуловость объектов)
     *
     * @param env         env
     * @param factory     функция создания базы данных (пример: DatabaseImpl::create)
     * @param commandArgs аргументы для создания (порядок - {@link DatabaseCommandArgPositions}.
     *                    Id команды, имя команды, имя создаваемой бд
     * @throws IllegalArgumentException если передано неправильное количество аргументов
     */
    public CreateDatabaseCommand(ExecutionEnvironment env, DatabaseFactory factory, List<RespObject> commandArgs) throws IllegalArgumentException {
        if (commandArgs.size() < NUM_OF_ARGS) {
            throw new IllegalArgumentException("Not enough arguments to create database");
        }
        if (commandArgs.size() > NUM_OF_ARGS) {
            throw new IllegalArgumentException("Too much arguments to create database");
        }

        for (var object : commandArgs) {
            if (object == null) {
                throw new IllegalArgumentException("Some arguments are null");
            }
        }

        this.env = env;
        this.factory = factory;
        this.commandArgs = commandArgs;
    }

    /**
     * Создает бд в нужном env
     *
     * @return {@link DatabaseCommandResult#success(byte[])} с сообщением о том, что заданная база была создана. Например, "Database db1 created"
     */
    @Override
    public DatabaseCommandResult execute() {
        try {
            String dbName = commandArgs.get(DatabaseCommandArgPositions.DATABASE_NAME.getPositionIndex()).asString();
            env.addDatabase(factory.createNonExistent(dbName, env.getWorkingPath()));
            return DatabaseCommandResult.success(("Database " + dbName + " created successfully").getBytes());
        } catch (Exception ext) {
            return DatabaseCommandResult.error("Can't create database, because " + ext.getMessage());
        }
    }
}
