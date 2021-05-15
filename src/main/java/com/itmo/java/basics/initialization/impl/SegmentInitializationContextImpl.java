package com.itmo.java.basics.initialization.impl;

import com.itmo.java.basics.index.impl.SegmentIndex;
import com.itmo.java.basics.initialization.SegmentInitializationContext;

import java.nio.file.Path;
import java.nio.file.Paths;

public class SegmentInitializationContextImpl implements SegmentInitializationContext {
<<<<<<< HEAD
    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, long currentSize, SegmentIndex index) {
    }

    /**
     * Не используйте этот конструктор. Оставлен для совместимости со старыми тестами.
     */
    public SegmentInitializationContextImpl(String segmentName, Path tablePath, long currentSize) {
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath) {
        this(segmentName, tablePath.resolve(segmentName), 0, new SegmentIndex());
=======

    private final String segmentName;
    private final Path segmentPath;
    private final int currentSize;
    private final SegmentIndex index;

    public SegmentInitializationContextImpl(String segmentName, Path segmentPath, int currentSize, SegmentIndex index) {
        this.segmentName = segmentName;
        this.segmentPath = segmentPath;
        this.currentSize = currentSize;
        this.index = index;
    }

    public SegmentInitializationContextImpl(String segmentName, Path tablePath, int currentSize) {
        this.segmentName = segmentName;
        this.segmentPath = Paths.get(tablePath.toString(), segmentName);
        this.currentSize = currentSize;
        this.index = new SegmentIndex();
>>>>>>> 2c4f880 (Lab2 (#2))
    }

    @Override
    public String getSegmentName() {
        return segmentName;
    }

    @Override
    public Path getSegmentPath() {
        return segmentPath;
    }

    @Override
    public SegmentIndex getIndex() {
        return index;
    }

    @Override
    public long getCurrentSize() {
        return currentSize;
    }
}
