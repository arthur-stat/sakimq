package com.arth.sakimq.broker.store;

import com.arth.sakimq.common.constant.LoggerName;
import com.arth.sakimq.protocol.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.BiConsumer;

public class FileMessageStore implements MessageStore {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.LOGGING);
    private final Path storeFile;
    private FileOutputStream fileOutputStream;
    private BufferedOutputStream outputStream;
    private long currentOffset = 0;

    public FileMessageStore(String storeDir) {
        this.storeFile = Paths.get(storeDir, "messages.data");
    }

    @Override
    public synchronized void start() throws IOException {
        Files.createDirectories(storeFile.getParent());
        boolean exists = Files.exists(storeFile);
        if (exists) {
            currentOffset = Files.size(storeFile);
        } else {
            Files.createFile(storeFile);
            currentOffset = 0;
        }
        
        // Append mode
        fileOutputStream = new FileOutputStream(storeFile.toFile(), true);
        outputStream = new BufferedOutputStream(fileOutputStream);
        log.info("FileMessageStore started at {}, current size: {}", storeFile, currentOffset);
    }

    @Override
    public synchronized long append(MessagePack messagePack) throws IOException {
        if (outputStream == null) {
            throw new IOException("Store not started");
        }
        
        // Calculate length to update offset correctly
        // writeDelimitedTo writes a varint32 length followed by the message
        // To track offset accurately without re-calculating varint size, we can use a CountingOutputStream or just rely on file position if we flush.
        // However, for simplicity and performance, we can just write and assume appending works.
        // But we need to return the START offset of this message for indexing if needed.
        // For now, let's just return the offset BEFORE writing.
        
        long writeOffset = currentOffset;
        
        // We need to know how many bytes were written to update currentOffset.
        // Since BufferedOutputStream doesn't tell us easily, and we want to be safe:
        // Let's flush every time for durability (since it's a prototype MQ).
        
        // To get exact byte count, let's serialize to byte array first (memory overhead but safe).
        // Or use a wrapper stream.
        
        int serializedSize = messagePack.getSerializedSize();
        int varintSize = com.google.protobuf.CodedOutputStream.computeUInt32SizeNoTag(serializedSize);
        int totalSize = varintSize + serializedSize;
        
        messagePack.writeDelimitedTo(outputStream);
        outputStream.flush();
        // fsync is expensive, maybe we can skip it for prototype or do it periodically.
        // fileOutputStream.getFD().sync(); 
        
        currentOffset += totalSize;
        return writeOffset;
    }

    @Override
    public synchronized void replay(long fromOffset, BiConsumer<Long, MessagePack> messageConsumer) throws IOException {
        if (!Files.exists(storeFile)) {
            return;
        }

        try (FileInputStream fis = new FileInputStream(storeFile.toFile());
             BufferedInputStream bis = new BufferedInputStream(fis)) {
            
            if (fromOffset > 0) {
                long skipped = bis.skip(fromOffset);
                if (skipped != fromOffset) {
                    log.warn("Could not skip to offset {}, only skipped {}", fromOffset, skipped);
                }
            }

            long readOffset = fromOffset;
            while (bis.available() > 0) {
                try {
                    // mark position to calculate bytes read? No, available() is not reliable for that.
                    // But writeDelimitedFrom consumes from stream.
                    // We need to track offset manually if we want to pass it to consumer.
                    // Since PB parseFrom consumes exact bytes, we can try to estimate or just pass -1 if not strictly needed.
                    // Actually, for replay, we just need to load them back.
                    
                    // To be precise: 
                    // We can peek the first byte to read varint... complex.
                    // Let's trust the stream.
                    
                    MessagePack pack = MessagePack.parseDelimitedFrom(bis);
                    if (pack == null) break;
                    
                    // Re-calculate size to update readOffset
                    int size = pack.getSerializedSize();
                    int varint = com.google.protobuf.CodedOutputStream.computeUInt32SizeNoTag(size);
                    
                    messageConsumer.accept(readOffset, pack);
                    readOffset += (size + varint);
                    
                } catch (IOException e) {
                    log.error("Error reading message at offset {}", readOffset, e);
                    break;
                }
            }
        }
    }

    @Override
    public synchronized void shutdown() throws IOException {
        if (outputStream != null) {
            outputStream.flush();
            outputStream.close();
        }
    }
}
