//package com.arth.sakimq.common.utils;
//
//import java.util.concurrent.atomic.AtomicInteger;
//
//public class IdGenerator {
//
//    private static final AtomicInteger sequence = new AtomicInteger(0);
//    private static volatile int lastTimestamp = 0;
//
//    public static int generateId() {
//        int currentSecond = (int) (System.currentTimeMillis() / 1000);
//        int seq = sequence.getAndUpdate(prevSeq -> {
//            if (prevSeq == Integer.MAX_VALUE) return 0;
//            return prevSeq + 1;
//        });
//
//        if (currentSecond != lastTimestamp) {
//            synchronized (IdGenerator.class) {
//                if (currentSecond != lastTimestamp) {
//                    sequence.set(0);
//                    lastTimestamp = currentSecond;
//                    seq = 0;
//                }
//            }
//        }
//
//        return (currentSecond << 1) | (seq & 0x1);
//    }
//}
