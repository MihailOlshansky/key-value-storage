package com.itmo.java.client.client;

import com.itmo.java.client.command.CreateDatabaseKvsCommand;
import com.itmo.java.client.command.CreateTableKvsCommand;
import com.itmo.java.client.command.DeleteKvsCommand;
import com.itmo.java.client.command.GetKvsCommand;
import com.itmo.java.client.command.KvsCommand;
import com.itmo.java.client.command.SetKvsCommand;
import com.itmo.java.client.connection.KvsConnection;
import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.client.exception.DatabaseExecutionException;

import java.util.function.Supplier;

public class SimpleKvsClient implements KvsClient {
    private final String dbName;
    private final Supplier<KvsConnection> connectionSupplier;

    /**
     * Конструктор
     *
     * @param databaseName       имя базы, с которой работает
     * @param connectionSupplier метод создания подключения к базе
     */
    public SimpleKvsClient(String databaseName, Supplier<KvsConnection> connectionSupplier) {
        this.dbName = databaseName;
        this.connectionSupplier = connectionSupplier;
    }

    @Override
    public String createDatabase() throws DatabaseExecutionException {
        try{
            KvsCommand createDatabaseCommand = new CreateDatabaseKvsCommand(dbName);
            var result = 
            connectionSupplier.get()
                .send(createDatabaseCommand.getCommandId(), createDatabaseCommand.serialize());
            if(result.isError()){
                throw new DatabaseExecutionException(result.asString());
            }
            return result.asString();
        } catch (ConnectionException cntext) {
            throw new DatabaseExecutionException("Cannot create database " + dbName, cntext);
        }
    }

    @Override
    public String createTable(String tableName) throws DatabaseExecutionException {
        try{
            KvsCommand createTableCommand = new CreateTableKvsCommand(dbName, tableName);
            var result = 
            connectionSupplier.get()
                .send(createTableCommand.getCommandId(), createTableCommand.serialize());
            if(result.isError()){
                throw new DatabaseExecutionException(result.asString());
            }
            return result.asString();
        } catch (ConnectionException cntext) {
            throw new DatabaseExecutionException("Cannot create table " + tableName + " in database" + dbName, cntext);
        }
    }

    @Override
    public String get(String tableName, String key) throws DatabaseExecutionException {
        try{
            KvsCommand getCommand = new GetKvsCommand(dbName, tableName, key);
            var result = 
            connectionSupplier.get()
                .send(getCommand.getCommandId(), getCommand.serialize());
            if(result.isError()){
                throw new DatabaseExecutionException(result.asString());
            }
            return result.asString();
        } catch (ConnectionException cntext) {
            throw new DatabaseExecutionException("Cannot get value by key " + key + " in table" + tableName + " in database" + dbName, cntext);
        }
    }

    @Override
    public String set(String tableName, String key, String value) throws DatabaseExecutionException {
        try{
            KvsCommand setCommand = new SetKvsCommand(dbName, tableName, key, value);
            var result = 
            connectionSupplier.get()
                .send(setCommand.getCommandId(), setCommand.serialize());
            if(result.isError()){
                throw new DatabaseExecutionException(result.asString());
            }
            return result.asString();
        } catch (ConnectionException cntext) {
            throw new DatabaseExecutionException("Cannot set value " + value + " by key " + key + " in table" + tableName + " in database" + dbName, cntext);
        }
    }

    @Override
    public String delete(String tableName, String key) throws DatabaseExecutionException {
        try{
            KvsCommand deleteCommand = new DeleteKvsCommand(dbName, tableName, key);
            var result = 
            connectionSupplier.get()
                .send(deleteCommand.getCommandId(), deleteCommand.serialize());
            if(result.isError()){
                throw new DatabaseExecutionException(result.asString());
            }
            return result.asString();
        } catch (ConnectionException cntext) {
            throw new DatabaseExecutionException("Cannot delete key " + key + " in table" + tableName + " in database" + dbName, cntext);
        }
    }
}
