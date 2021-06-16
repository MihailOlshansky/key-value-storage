package com.itmo.java.client.connection;

import java.io.IOException;
import java.net.Socket;

import com.itmo.java.client.exception.ConnectionException;
import com.itmo.java.protocol.RespReader;
import com.itmo.java.protocol.RespWriter;
import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespObject;

/**
 * С помощью {@link RespWriter} и {@link RespReader} читает/пишет в сокет
 */
public class SocketKvsConnection implements KvsConnection {
    private final Socket socket;
    private final RespWriter writer;
    private final RespReader reader;

    public SocketKvsConnection(ConnectionConfig config) {
        try {
            socket = new Socket(config.getHost(), config.getPort());
            reader = new RespReader(socket.getInputStream());
            writer = new RespWriter(socket.getOutputStream());
        } catch (IOException ioext) {
            throw new RuntimeException("Connecting error", ioext);
        }
    }

    /**
     * Отправляет с помощью сокета команду и получает результат.
     *
     * @param commandId id команды (номер)
     * @param command   команда
     * @throws ConnectionException если сокет закрыт или если произошла другая ошибка соединения
     */
    @Override
    public synchronized RespObject send(int commandId, RespArray command) throws ConnectionException {
        try {
            writer.write(command);
            return reader.readObject();
        } catch (Exception ext) {
            throw new ConnectionException("Sending error", ext);
        }
    }

    /**
     * Закрывает сокет (и другие использованные ресурсы)
     */
    @Override
    public void close() {
        try {
            socket.close();
            reader.close();
            writer.close();
        } catch (IOException ioext) {
            throw new RuntimeException("Cannot close resources", ioext);
        }
    }

}
