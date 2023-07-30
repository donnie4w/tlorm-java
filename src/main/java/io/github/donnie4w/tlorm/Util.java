/*
 * Copyright 2023 tldb Author. All Rights Reserved.
 * email: donnie4w@gmail.com
 * https://githuc.com/donnie4w/tldb
 * https://githuc.com/donnie4w/tlorm-java
 */
package io.github.donnie4w.tlorm;

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
        return  value.getBytes(StandardCharsets.UTF_8);
    }

    protected static boolean bytes2boolean(byte[] bs) {
        return bs==null?false:bs[0]>0?true:false;
    }

    protected static byte[] boolean2Bytes(boolean value) {
        return new byte[]{value?(byte)1:(byte)0};
    }

    protected static char bytes2char(byte[] bs) {
        return (char) bytes2short(bs);
    }

    protected static byte[] char2Bytes(char value) {
        return short2Bytes((short)value );
    }


    protected static byte[] prase(java.lang.reflect.Field c, Object o) throws IllegalAccessException {
        byte[] bs = null;
        switch (c.getType().getSimpleName()) {
            case "long":
                bs = Util.long2Bytes(c.getLong(o));
                break;
            case "int":
                bs = Util.int2Bytes(c.getInt(o));
                break;
            case "short":
                bs = Util.short2Bytes(c.getShort(o));
                break;
            case "float":
                bs = Util.float2Bytes(c.getFloat(o));
                break;
            case "boolean":
                bs = Util.boolean2Bytes(c.getBoolean(o));
                break;
            case "byte":
                bs = new byte[]{c.getByte(o)};
                break;
            case "double":
                bs = Util.double2Bytes(c.getDouble(o));
                break;
            case "char":
                bs = Util.char2Bytes(c.getChar(o));
                break;
            case "String":
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


    protected static void prase4set(java.lang.reflect.Field c, Object o, java.nio.ByteBuffer bb) throws IllegalAccessException {
        byte[] bs = null;
        switch (c.getType().getSimpleName()) {
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
            case "String":
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
        switch (c.getType().getSimpleName()) {
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
                bs = new byte[]{c.getByte((Byte) o)};
                break;
            case "double":
                bs = Util.double2Bytes((Double) o);
                break;
            case "char":
                bs = Util.char2Bytes((Character) o);
                break;
            case "String":
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
