package com.itmo.java.basics.logic.impl;

import com.itmo.java.basics.logic.WritableDatabaseRecord;

public class SetDatabaseRecord implements WritableDatabaseRecord {

    private final byte[] key;
    private final byte[] value;

    public SetDatabaseRecord(byte[] key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public byte[] getKey() {
        return this.key;
    }

    @Override
    public byte[] getValue() {
        return this.value;
    }

    @Override
    public long size() {
        return 4 + (long)getKeySize() + 4 + (long)getValueSize();
    }

    @Override
    public boolean isValuePresented() {
        return true;
    }

    @Override
    public int getKeySize() {
        return key.length;
    }

    @Override
    public int getValueSize() {
        return value.length;
    }
}
