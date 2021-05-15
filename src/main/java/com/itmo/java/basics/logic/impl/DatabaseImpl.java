package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.exceptions.DatabaseException;
import com.itmo.java.basics.initialization.DatabaseInitializationContext;
import com.itmo.java.basics.index.impl.TableIndex;
import com.itmo.java.basics.logic.Database;
import com.itmo.java.basics.logic.Table;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;


public class DatabaseImpl implements Database {
    private final String dbName;
    private final Path pathToDataBase;
    private final Map<String, Table> databaseTables;

    private DatabaseImpl(String dbName, Path path) {
        this.dbName = dbName;
        this.pathToDataBase = path;
        databaseTables = new HashMap<>();
    }

    private DatabaseImpl(DatabaseInitializationContext context) {
        this.dbName = context.getDbName();
        this.pathToDataBase = context.getDatabasePath();
        databaseTables = context.getTables();
    }

    /**
     * @param databaseRoot путь к директории, которая может содержать несколько БД,
     *                     поэтому при создании БД необходимо создать директорию внутри databaseRoot.
     */
    public static Database create(String dbName, Path databaseRoot) throws DatabaseException {
        if (dbName == null || dbName.isEmpty()) {
            throw new DatabaseException("Name of database is null or empty");
        }

        Path path = Paths.get(databaseRoot.toString(), dbName);
        
        if (Files.exists(path)) {
            throw new DatabaseException("Database " + dbName + " already exists");
        }

        try {
            Files.createDirectory(path);
        } catch (IOException ioext) {
            throw new DatabaseException("Can't create directory " + path.toString(), ioext);
        }        

        return new DatabaseImpl(dbName, path);
    }

    public static Database initializeFromContext(DatabaseInitializationContext context) {
        return new DatabaseImpl(context);
    }

    @Override
    public String getName() {
        return dbName;
    }

    @Override
    public void createTableIfNotExists(String tableName) throws DatabaseException {
        if (tableName == null) {
            throw new DatabaseException("Invalid table name");
        }

        if (containsTable(tableName)) {
            throw new DatabaseException("Table " + tableName + " already exists");
        }
        databaseTables.put(tableName, TableImpl.create(tableName, pathToDataBase, new TableIndex()));
    }

    @Override
    public void write(String tableName, String objectKey, byte[] objectValue) throws DatabaseException {        
        getTable(tableName).write(objectKey, objectValue);
    }

    @Override
    public Optional<byte[]> read(String tableName, String objectKey) throws DatabaseException {
        return getTable(tableName).read(objectKey);
    }

    @Override
    public void delete(String tableName, String objectKey) throws DatabaseException {
        getTable(tableName).delete(objectKey);
    }

    private boolean containsTable(String tableName) {
        return databaseTables.containsKey(tableName);
    }

    private Table getTable(String tableName) throws DatabaseException {
        if (!containsTable(tableName)) {
            throw new DatabaseException("No table " + tableName + " in database");
        }

        return databaseTables.get(tableName);
    }
}
