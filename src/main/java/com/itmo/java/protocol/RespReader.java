package com.itmo.java.protocol;

import com.itmo.java.protocol.model.RespArray;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespCommandId;
import com.itmo.java.protocol.model.RespError;
import com.itmo.java.protocol.model.RespObject;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.ByteBuffer;

public class RespReader implements AutoCloseable {

    /**
     * Специальные символы окончания элемента
     */
    private static final byte CR = '\r';
    private static final byte LF = '\n';
    private final PushbackInputStream pbis;

    public RespReader(InputStream is) {
        this.pbis = new PushbackInputStream(is);
    }

    /**
     * Есть ли следующий массив в стриме?
     */
    public boolean hasArray() throws IOException {
        try{
            return getType() == RespArray.CODE;
        } catch (EOFException eofext) {
            return false;
        }
    }

    /**
     * Считывает из input stream следующий объект. Может прочитать любой объект, сам определит его тип на основе кода объекта.
     * Например, если первый элемент "-", то вернет ошибку. Если "$" - bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespObject readObject() throws IOException {
        byte type = getType();
        switch (type) {
            case RespArray.CODE: 
                return readArray();
            case RespBulkString.CODE:
                return readBulkString();
            case RespCommandId.CODE:
                return readCommandId();
            case RespError.CODE:
                return readError();
            default:
                throw new IOException("Wrong RESP type");
        }

    }

    /**
     * Считывает объект ошибки
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespError readError() throws IOException {
        checkType(RespError.CODE);
        return new RespError(readUntilCRLF().getBytes());
    }

    /**
     * Читает bulk строку
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespBulkString readBulkString() throws IOException {
        checkType(RespBulkString.CODE);
        int stringSize;
        try {
            stringSize = Integer.parseInt(readUntilCRLF());
        } catch (NumberFormatException nfext) {
            throw new IOException("Wrong lenght of bulk string format", nfext);
        }

        if (stringSize == RespBulkString.NULL_STRING_SIZE) {
            return RespBulkString.NULL_STRING;
        }

        byte[] result = pbis.readNBytes(stringSize);
        
        if (!checkIfNextCRLF() ||
            result.length != stringSize) {
            throw new IOException("Wrong size of string");
        }

        return new RespBulkString(result);
    }

    /**
     * Считывает массив RESP элементов
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespArray readArray() throws IOException {
        checkType(RespArray.CODE);
        int arraySize;
        try {
            arraySize = Integer.parseInt(readUntilCRLF());
        } catch (NumberFormatException nfext) {
            throw new IOException("Wrong lenght of bulk string format", nfext);
        }

        RespObject[] result = new RespObject[arraySize];
        for (int i = 0; i < arraySize; i++) {
            result[i] = readObject();
        }

        return new RespArray(result);
    }

    /**
     * Считывает id команды
     *
     * @throws EOFException если stream пустой
     * @throws IOException  при ошибке чтения
     */
    public RespCommandId readCommandId() throws IOException {
        checkType(RespCommandId.CODE);
        byte[] result = pbis.readNBytes(4);
        if (!checkIfNextCRLF() ||
            result.length != 4) {
                throw new IOException("Wrong command id format");
            }
        return new RespCommandId(ByteBuffer.wrap(result).getInt());
    }


    @Override
    public void close() throws IOException {
        pbis.close();
    }

    private byte getType() throws IOException {
        int type = pbis.read();
        pbis.unread(type);
        if (type == -1) {
            throw new EOFException("eof");
        }
        return (byte)type;
    }

    private void checkType(byte expectedType) throws IOException {
        int type = pbis.read();
        if (type == -1){
            throw new EOFException("eof");
        }
        if (type != (int)expectedType){
            throw new IOException("Wrong RESP type. Expected: " + (char)expectedType);
        }
    }

    private String readUntilCRLF() throws IOException {
        StringBuilder builder = new StringBuilder("");
        int cur = pbis.read();
        if (cur == -1) {
            throw new EOFException("eof");
        }
        while (true) {
            int next = pbis.read();
            if (next == -1) {
                throw new EOFException("eof");
            }
            if (cur == CR && next == LF) {
                break;
            }
            builder.append((char)cur);
            cur = next;
        }
        return builder.toString();
    }

    private boolean checkIfNextCRLF() throws IOException {
        int first = pbis.read();
        int second = pbis.read();
        return first == CR && second == LF;
    }
}
