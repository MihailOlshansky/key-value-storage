package com.itmo.java.protocol.model;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Строка
 */
public class RespBulkString implements RespObject {
    /**
     * Код объекта
     */
    public static final byte CODE = '$';
    public static final int NULL_STRING_SIZE = -1;

    public static final RespBulkString NULL_STRING = new RespBulkString(null);
    private final byte[] byteString;

    public RespBulkString(byte[] data) {
        this.byteString = data;
    }

    /**
     * Ошибка ли это? Ответ - нет
     *
     * @return false
     */
    @Override
    public boolean isError() {
        return false;
    }

    /**
     * Строковое представление
     *
     * @return строку, если данные есть. Если нет - null
     */
    @Override
    public String asString() {
        if(byteString == null) {
            return null;
        }
        return new String(byteString);
    }

    @Override
    public void write(OutputStream os) throws IOException {
        os.write(CODE);
        if(byteString != null) {
            os.write(String.valueOf(byteString.length).getBytes());
            os.write(CRLF);
            os.write(byteString);
            os.write(CRLF);
        } else {
            os.write(String.valueOf(NULL_STRING_SIZE).getBytes());
            os.write(CRLF);
        }
    }
}
