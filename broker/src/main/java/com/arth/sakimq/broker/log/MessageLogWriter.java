package com.arth.sakimq.broker.log;

import com.arth.sakimq.protocol.Message;
import com.arth.sakimq.protocol.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Very simple message log writer with size-based rolling.
 * Currently only writes metadata and body size; body content is intentionally omitted for the prototype.
 */
public class MessageLogWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(MessageLogWriter.class);
    private static final String FILE_PREFIX = "message-log";
    private static final String FILE_SUFFIX = ".log";
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd-HHmmss");

    private final Path logDir;
    private final long maxBytes;
    private final boolean includeBody;
    private final AtomicInteger rollingIndex = new AtomicInteger(0);

    private BufferedOutputStream outputStream;
    private long currentSize = 0L;

    public MessageLogWriter(String logDir, long maxBytes, boolean includeBody) throws IOException {
        this.logDir = Paths.get(logDir);
        this.maxBytes = maxBytes;
        this.includeBody = includeBody;
        init();
    }

    private void init() throws IOException {
        Files.createDirectories(logDir);
        rollFile();
    }

    private void rollFile() throws IOException {
        closeStreamQuietly();
        String fileName = FILE_PREFIX + "-" + DATE_FORMAT.format(new Date())
                + "-" + rollingIndex.getAndIncrement() + FILE_SUFFIX;
        Path logFile = logDir.resolve(fileName);
        outputStream = new BufferedOutputStream(new FileOutputStream(logFile.toFile(), false));
        currentSize = 0L;
        log.info("Message log rolling to new file: {}", logFile.toAbsolutePath());
    }

    public synchronized void append(MessagePack pack, String clientId, long seq) throws IOException {
        if (pack == null || !pack.hasMessage()) {
            return;
        }

        Message message = pack.getMessage();
        List<String> topics = pack.getTopicsList();
        String topicsJoined = String.join(",", topics);

        // For prototype: only metadata and body length; body content is intentionally skipped even if includeBody=true.
        String line = String.format(
                "ts=%d|clientId=%s|seq=%d|msgId=%d|topics=%s|headers=%d|bodySize=%d%n",
                System.currentTimeMillis(),
                clientId,
                seq,
                message.getMessageId(),
                topicsJoined,
                message.getHeadersCount(),
                message.getBody().size()
        );

        byte[] data = line.getBytes(StandardCharsets.UTF_8);
        ensureCapacity(data.length);
        outputStream.write(data);
        currentSize += data.length;
        outputStream.flush();
    }

    private void ensureCapacity(long upcomingBytes) throws IOException {
        if (currentSize + upcomingBytes > maxBytes) {
            rollFile();
        }
    }

    private void closeStreamQuietly() {
        if (outputStream != null) {
            try {
                outputStream.flush();
                outputStream.close();
            } catch (IOException ignored) {
            }
        }
    }

    @Override
    public void close() throws IOException {
        closeStreamQuietly();
    }
}
