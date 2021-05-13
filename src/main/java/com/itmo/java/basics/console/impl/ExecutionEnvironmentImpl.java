package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.config.DatabaseConfig;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.basics.logic.Database;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ExecutionEnvironmentImpl implements ExecutionEnvironment {
    private final DatabaseConfig dbConfig;
    private final Map<String, Database> databases = new HashMap<>();

    public ExecutionEnvironmentImpl(DatabaseConfig config) {
        this.dbConfig = config;
    }

    @Override
    public Optional<Database> getDatabase(String name) {
        return Optional.ofNullable(databases.get(name));
    }

    @Override
    public void addDatabase(Database db) {
        databases.put(db.getName(), db);
    }

    @Override
    public Path getWorkingPath() {
        return Paths.get(dbConfig.getWorkingPath());
    }
}
