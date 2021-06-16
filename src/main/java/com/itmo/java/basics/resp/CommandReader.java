package com.itmo.java.basics.resp;

import com.itmo.java.basics.console.DatabaseCommand;
import com.itmo.java.basics.console.DatabaseCommandArgPositions;
import com.itmo.java.basics.console.DatabaseCommands;
import com.itmo.java.basics.console.ExecutionEnvironment;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespObject;

import java.io.IOException;
import java.util.List;

public class CommandReader implements AutoCloseable {

    private final RespReader reader;
    private final ExecutionEnvironment env;

    public CommandReader(RespReader reader, ExecutionEnvironment env) {
        this.reader = reader;
        this.env = env;
    }

    /**
     * Есть ли следующая команда в ридере?
     */
    public boolean hasNextCommand() throws IOException {
        return reader.hasArray();
    }

    /**
     * Считывает комманду с помощью ридера и возвращает ее
     *
     * @throws IllegalArgumentException если нет имени команды и id
     */
    public DatabaseCommand readCommand() throws IllegalArgumentException, IOException {
        List<RespObject> objects = getObjectsIfCanRead();
        return DatabaseCommands
                .valueOf(
                    objects.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex()).asString())
                .getCommand(env, objects);
    }

    @Override
    public void close() throws Exception {
        reader.close();
    }

    private List<RespObject> getObjectsIfCanRead() throws IllegalArgumentException, IOException {
        List<RespObject> objects = reader.readArray().getObjects();

        if (objects.size() < 3) {
            throw new IllegalArgumentException("Not enough arguments to read a command");
        }
        if (!(objects.get(DatabaseCommandArgPositions.COMMAND_ID.getPositionIndex())
            instanceof RespCommandId)) {
            throw new IllegalArgumentException("No command id found");
        }
        if (!(objects.get(DatabaseCommandArgPositions.COMMAND_NAME.getPositionIndex())
            instanceof RespBulkString)) {
            throw new IllegalArgumentException("No command name found");
        }

        return objects;
    }
}
