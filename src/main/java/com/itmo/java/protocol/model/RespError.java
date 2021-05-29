package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Сообщение об ошибке в RESP протоколе
 */
public class RespError implements RespObject {

    /**
     * Код объекта
     */
    public static final byte CODE = '-';

    private final byte[] byteMessage;

    public RespError(byte[] message) {
        this.byteMessage = message;
    }

    /**
     * Ошибка ли это? Ответ - да
     *
     * @return true
     */
    @Override
    public boolean isError() {
        return true;
    }

    @Override
    public String asString() {
        return new String(byteMessage);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        os.write(byteMessage);
        os.write(CRLF);
    }
}
