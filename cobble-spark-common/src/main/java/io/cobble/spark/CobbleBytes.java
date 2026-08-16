package io.cobble.spark;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;

/** Little-endian byte primitives and key framing shared by the row encoder and decoder. */
final class CobbleBytes {

    private CobbleBytes() {}

    static byte[] putShort(byte[] target, short value) {
        target[0] = (byte) (value & 0xFF);
        target[1] = (byte) ((value >> 8) & 0xFF);
        return target;
    }

    static byte[] putInt(byte[] target, int value) {
        target[0] = (byte) (value & 0xFF);
        target[1] = (byte) ((value >> 8) & 0xFF);
        target[2] = (byte) ((value >> 16) & 0xFF);
        target[3] = (byte) ((value >> 24) & 0xFF);
        return target;
    }

    static byte[] putLong(byte[] target, long value) {
        for (int i = 0; i < 8; i++) {
            target[i] = (byte) ((value >> (8 * i)) & 0xFF);
        }
        return target;
    }

    static short getShort(byte[] source, int offset) {
        return (short) ((source[offset] & 0xFF) | ((source[offset + 1] & 0xFF) << 8));
    }

    static int getInt(byte[] source, int offset) {
        return (source[offset] & 0xFF)
                | ((source[offset + 1] & 0xFF) << 8)
                | ((source[offset + 2] & 0xFF) << 16)
                | ((source[offset + 3] & 0xFF) << 24);
    }

    static long getLong(byte[] source, int offset) {
        long value = 0L;
        for (int i = 7; i >= 0; i--) {
            value = (value << 8) | (source[offset + i] & 0xFFL);
        }
        return value;
    }

    static BigDecimal decimal(BigInteger unscaled, int scale) {
        return new BigDecimal(unscaled, scale);
    }

    /**
     * Frames already encoded key fields: 4-byte big-endian length + bytes per field, in one
     * allocation.
     */
    static byte[] frameKeyParts(byte[][] parts) {
        int total = 0;
        for (byte[] part : parts) {
            total += Integer.BYTES + part.length;
        }
        byte[] framed = new byte[total];
        int offset = 0;
        for (byte[] part : parts) {
            framed[offset] = (byte) ((part.length >> 24) & 0xFF);
            framed[offset + 1] = (byte) ((part.length >> 16) & 0xFF);
            framed[offset + 2] = (byte) ((part.length >> 8) & 0xFF);
            framed[offset + 3] = (byte) (part.length & 0xFF);
            offset += Integer.BYTES;
            System.arraycopy(part, 0, framed, offset, part.length);
            offset += part.length;
        }
        return framed;
    }

    /** Inverse of {@link #frameKeyParts} for {@code count} fields; rejects corrupt framing. */
    static byte[][] splitKeyParts(byte[] framed, int count) {
        byte[][] parts = new byte[count][];
        int offset = 0;
        for (int i = 0; i < count; i++) {
            if (offset + Integer.BYTES > framed.length) {
                throw corruptKey();
            }
            int length =
                    ((framed[offset] & 0xFF) << 24)
                            | ((framed[offset + 1] & 0xFF) << 16)
                            | ((framed[offset + 2] & 0xFF) << 8)
                            | (framed[offset + 3] & 0xFF);
            offset += Integer.BYTES;
            if (length < 0 || offset + length > framed.length) {
                throw corruptKey();
            }
            parts[i] = Arrays.copyOfRange(framed, offset, offset + length);
            offset += length;
        }
        if (offset != framed.length) {
            throw corruptKey();
        }
        return parts;
    }

    private static IllegalArgumentException corruptKey() {
        return new IllegalArgumentException("Corrupt Cobble key: invalid length framing.");
    }
}
