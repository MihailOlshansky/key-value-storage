package com.itmo.java.basics.console.impl;

import com.itmo.java.basics.console.DatabaseCommandResult;
import com.itmo.java.protocol.model.RespBulkString;
import com.itmo.java.protocol.model.RespObject;

/**
 * Результат успешной команды
 */
public class SuccessDatabaseCommandResult implements DatabaseCommandResult {
    private final String payLoad;

    public SuccessDatabaseCommandResult(byte[] payload) {
        if(payload == null) {
            this.payLoad = null;
        }
        else {
            this.payLoad = new String(payload);
        }
    }

    @Override
    public String getPayLoad() {
        return payLoad;
    }

    @Override
    public boolean isSuccess() {
        return true;
    }

    /**
     * Сериализуется в {@link RespBulkString}
     */
    @Override
    public RespObject serialize() {
        if (payLoad == null) {
            return RespBulkString.NULL_STRING;
        }
        return new RespBulkString(payLoad.getBytes());
    }
}
