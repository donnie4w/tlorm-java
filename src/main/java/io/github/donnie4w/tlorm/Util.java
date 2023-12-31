/*
 * Copyright 2023 tldb Author. All Rights Reserved.
 * email: donnie4w@gmail.com
 * https://githuc.com/donnie4w/tldb
 * https://githuc.com/donnie4w/tlorm-java
 */
package io.github.donnie4w.tlorm;

import io.github.donnie4w.tldb.tlcli.ColumnType;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Util {
    protected static long bytes2Long(byte[] bs) {
        ByteBuffer buf = ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN);
        buf.put(bs, 0, bs.length);
        buf.flip();
        return buf.getLong();
    }

    protected static byte[] long2Bytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).order(ByteOrder.BIG_ENDIAN).putLong(0, value).array();
    }

    protected static int bytes2int(byte[] bs) {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
        buf.put(bs, 0, bs.length);
        buf.flip();
        return buf.getInt();
    }

    protected static byte[] int2Bytes(int value) {
        return ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(0, value).array();
    }

    protected static short bytes2short(byte[] bs) {
        ByteBuffer buf = ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN);
        buf.put(bs, 0, bs.length);
        buf.flip();
        return buf.getShort();
    }

    protected static byte[] short2Bytes(short value) {
        return ByteBuffer.allocate(Short.BYTES).order(ByteOrder.BIG_ENDIAN).putShort(0, value).array();
    }

    protected static float bytes2Float(byte[] bs) {
        ByteBuffer buf = ByteBuffer.allocate(Float.BYTES).order(ByteOrder.BIG_ENDIAN);
        buf.put(bs, 0, bs.length);
        buf.flip();
        return buf.getFloat();
    }

    protected static byte[] float2Bytes(float value) {
        return ByteBuffer.allocate(Float.BYTES).order(ByteOrder.BIG_ENDIAN).putFloat(0, value).array();
    }

    protected static double bytes2Double(byte[] bs) {
        ByteBuffer buf = ByteBuffer.allocate(Double.BYTES).order(ByteOrder.BIG_ENDIAN);
        buf.put(bs, 0, bs.length);
        buf.flip();
        return buf.getDouble();
    }

    protected static byte[] double2Bytes(double value) {
        return ByteBuffer.allocate(Double.BYTES).order(ByteOrder.BIG_ENDIAN).putDouble(0, value).array();
    }

    protected static String bytes2String(byte[] bs) {
        return new String(bs, StandardCharsets.UTF_8);
    }

    protected static byte[] string2Bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    protected static boolean bytes2boolean(byte[] bs) {
        return bs == null ? false : bs[0] > 0;
    }

    protected static byte[] boolean2Bytes(boolean value) {
        return new byte[]{value ? (byte) 1 : (byte) 0};
    }

    protected static char bytes2char(byte[] bs) {
        return (char) bytes2short(bs);
    }

    protected static byte[] char2Bytes(char value) {
        return short2Bytes((short) value);
    }


    protected static byte[] prase(java.lang.reflect.Field c, Object o, boolean isNonzero) throws IllegalAccessException {
        byte[] bs = null;
        switch (c.getType().getSimpleName().toLowerCase()) {
            case "long":
                long l = c.getLong(o);
                if (isNonzero && l == 0) {
                    return bs;
                }
                bs = Util.long2Bytes(l);
                break;
            case "int":
                int i = c.getInt(o);
                if (isNonzero && i == 0) {
                    return bs;
                }
                bs = Util.int2Bytes(i);
                break;
            case "short":
                short s = c.getShort(o);
                if (isNonzero && s == 0) {
                    return bs;
                }
                bs = Util.short2Bytes(s);
                break;
            case "float":
                float f = c.getFloat(o);
                if (isNonzero && f == 0) {
                    return bs;
                }
                bs = Util.float2Bytes(f);
                break;
            case "boolean":
                bs = Util.boolean2Bytes(c.getBoolean(o));
                break;
            case "byte":
                byte bt = c.getByte(o);
                if (isNonzero && bt == 0) {
                    return bs;
                }
                bs = new byte[]{bt};
                break;
            case "double":
                double d = c.getDouble(o);
                if (isNonzero && d == 0) {
                    return bs;
                }
                bs = Util.double2Bytes(d);
                break;
            case "char":
                char cr = c.getChar(o);
                if (isNonzero && cr == 0) {
                    return bs;
                }
                bs = Util.char2Bytes(cr);
                break;
            case "string":
                if (c.get(o) == null) {
                    return bs;
                }
                bs = Util.string2Bytes(c.get(o).toString());
                break;
            case "byte[]":
                bs = (byte[]) c.get(o);
                break;
            default:
                bs = Util.string2Bytes(c.get(o).toString());
        }
        return bs;
    }

    protected static ColumnType praseColumnType(String fieldName)  {
        ColumnType s = null;
        try{
            switch (fieldName) {
                case "long":
                    s = ColumnType.INT64;
                    break;
                case "int":
                    s = ColumnType.INT32;
                    break;
                case "short":
                    s = ColumnType.INT16;
                    break;
                case "float":
                    s = ColumnType.FLOAT32;
                    break;
                case "boolean":
                    s = ColumnType.INT8;
                    break;
                case "byte":
                    s = ColumnType.BYTE;
                    break;
                case "double":
                    s = ColumnType.FLOAT64;
                    break;
                case "string":
                    s = ColumnType.STRING;
                    break;
                case "byte[]":
                case "char":
                default:
                    s = ColumnType.BINARY;
            }
        }catch (Exception e){
        }
        if (s == null) {
            s = ColumnType.BINARY;
        }
        return s;
    }


    protected static void prase4set(java.lang.reflect.Field c, Object o, java.nio.ByteBuffer bb) throws IllegalAccessException {
        switch (c.getType().getSimpleName().toLowerCase()) {
            case "long":
                c.setLong(o, Util.bytes2Long(bb.array()));
                break;
            case "int":
                c.setInt(o, Util.bytes2int(bb.array()));
                break;
            case "short":
                c.setShort(o, Util.bytes2short(bb.array()));
                break;
            case "float":
                c.setFloat(o, Util.bytes2Float(bb.array()));
                break;
            case "boolean":
                c.setBoolean(o, Util.bytes2boolean(bb.array()));
                break;
            case "byte":
                c.setByte(o, bb == null ? 0 : bb.array()[0]);
                break;
            case "double":
                c.setBoolean(o, Util.bytes2boolean(bb.array()));
                break;
            case "char":
                c.setChar(o, Util.bytes2char(bb.array()));
                break;
            case "string":
                c.set(o, Util.bytes2String(bb.array()));
                break;
            case "byte[]":
                c.set(o, bb.array());
                break;
            default:
                c.set(o, bb.array());
        }
    }

    protected static byte[] praseValue(Field c, Object o) throws IllegalAccessException {
        byte[] bs = null;
        switch (c.getType().getSimpleName().toLowerCase()) {
            case "long":
                bs = Util.long2Bytes((Long) o);
                break;
            case "int":
                bs = Util.int2Bytes((Integer) o);
                break;
            case "short":
                bs = Util.short2Bytes((Short) o);
                break;
            case "float":
                bs = Util.float2Bytes((Float) o);
                break;
            case "boolean":
                bs = Util.boolean2Bytes((Boolean) o);
                break;
            case "byte":
                bs = new byte[]{c.getByte(o)};
                break;
            case "double":
                bs = Util.double2Bytes((Double) o);
                break;
            case "char":
                bs = Util.char2Bytes((Character) o);
                break;
            case "string":
                bs = Util.string2Bytes((String) o);
                break;
            case "byte[]":
                bs = (byte[]) o;
                break;
            default:
                bs = null;
        }
        return bs;
    }
}
