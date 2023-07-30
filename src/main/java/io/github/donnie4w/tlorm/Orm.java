/*
 * Copyright 2023 tldb Author. All Rights Reserved.
 * email: donnie4w@gmail.com
 * https://githuc.com/donnie4w/tldb
 * https://githuc.com/donnie4w/tlorm-java
 */

package io.github.donnie4w.tlorm;

import io.github.donnie4w.tldb.tlcli.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Orm<T> {
    private static Map<Class, Map<String, Object[]>> cm = new ConcurrentHashMap<Class, Map<String, Object[]>>();
    private static Client defaultClient = null;
    private Client client = null;
    private String name;

    public static void registerDefaultResource(boolean tls, String host, int port, String auth) throws TlException {
        defaultClient = new Client();
        defaultClient.newConnect(tls, host, port, auth);
    }

    public static Client newClient(boolean tls, String host, int port, String auth) throws TlException {
        Client c = new Client();
        c.newConnect(tls, host, port, auth);
        return c;
    }

    public Orm() {
        praseValue(false);
        this.name = this.getClass().getSimpleName().toLowerCase();
        this.setClient(defaultClient);
    }

    public void setClient(Client client) {
        this.client = client;
    }

    private void praseValue(boolean force) {
        if (!cm.containsKey(this) || force) {
            Map<String, Object[]> fm = new HashMap<>();
            for (Field f : this.getClass().getFields()) {
                if (f.getName().toLowerCase() != "id") {
                    String fieldname = null;
                    if (f.isAnnotationPresent(io.github.donnie4w.tlorm.Field.class)) {
                        io.github.donnie4w.tlorm.Field field = f.getAnnotation(io.github.donnie4w.tlorm.Field.class);
                        fieldname = field.name();
                    }
                    fm.put(f.getName(), new Object[]{fieldname, f.isAnnotationPresent(Index.class)});
                } else if (f.getType().getSimpleName() != "long") {
                    throw new TlRunTimeException("not found \"id\" in long type");
                }
            }
            cm.put(this.getClass(), fm);
        }
    }

    public void createTable() throws TlException {
        Map<String, Object[]> pm = cm.get(this.getClass());
        String[] fs = new String[pm.size()];
        List<String> idxlist = new ArrayList<>();
        int i = 0;
        for (String key : pm.keySet()) {
            if (key.toLowerCase() != "id") {
                String name = (String) pm.get(key)[0];
                if (name != null) {
                    fs[i++] = name;
                } else {
                    fs[i++] = key;
                }
                if ((Boolean) pm.get(key)[1]) {
                    idxlist.add(key);
                }
            }
        }
        this.client.createTable(this.name, fs, idxlist.toArray(new String[]{}));
    }

    public long insert(T o) throws TlException {
        Map<String, byte[]> m = new HashMap<>();
        try {
            Map<String, Object[]> pm = cm.get(this.getClass());
            for (String key : pm.keySet()) {
                String name = (String) pm.get(key)[0];
                if (name == null) {
                    name = key;
                }
                if (key.toLowerCase() != "id") {
                    byte[] bs = Util.prase(o.getClass().getField(key), o);
                    m.put(name, bs);
                }
            }
            AckBean ab = defaultClient.insert(this.name, m);
            return ab.seq;
        } catch (Exception e) {
            throw new TlException(e);
        }
    }

    public long update(T o) throws TlException {
        Map<String, byte[]> m = new HashMap<>();
        try {
            Map<String, Object[]> pm = cm.get(this.getClass());
            long id = o.getClass().getField("id").getLong(o);
            for (String key : pm.keySet()) {
                String name = (String) pm.get(key)[0];
                if (name == null) {
                    name = key;
                }
                if (key.toLowerCase() != "id") {
                    byte[] bs = Util.prase(o.getClass().getField(key), o);
                    m.put(name, bs);
                }
            }
            AckBean ab = this.client.update(this.name, id, m);
            return ab.seq;
        } catch (Exception e) {
            throw new TlException(e);
        }
    }

    public void delete(long id) throws TlException {
        this.client.delete(this.name, id);
    }

    public void drop() throws TlException {
        this.client.drop(this.name);
    }

    public void alterTable() throws TlException {
        praseValue(true);
        Map<String, Object[]> pm = cm.get(this.getClass());
        String[] fs = new String[pm.size()];
        List<String> idxlist = new ArrayList<>();
        int i = 0;
        for (String key : pm.keySet()) {
            if (key.toLowerCase() != "id") {
                fs[i++] = key;
                if ((Boolean) pm.get(key)[1]) {
                    idxlist.add(key);
                }
            }
        }
        this.client.alterTable(this.name, fs, idxlist.toArray(new String[]{}));
    }

    public long selectId() throws TlException {
        return this.client.selectId(this.name);
    }

    public T selectById(long id) throws TlException {
        DataBean db = this.client.selectById(this.name, id);
        Map<String, java.nio.ByteBuffer> m = db.getTBean();
        Map<String, Object[]> pm = cm.get(this.getClass());
        T t = null;
        try {
            if (m != null) {
                t = (T) this.getClass().getDeclaredConstructor().newInstance();
                t.getClass().getField("id").setLong(t, db.getId());
                for (String key : pm.keySet()) {
                    String name = (String) pm.get(key)[0];
                    if (name == null) {
                        name = key;
                    }
                    if (m.containsKey(name)) {
                        Util.prase4set(t.getClass().getField(key), t, m.get(name));
                    }
                }
            }
            return t;
        } catch (Exception e) {
            throw new TlException(e);
        }
    }

    public List<T> selectsByIdLimit(long startId, long limit) throws TlException {
        List<DataBean> dblist = this.client.selectsByIdLimit(this.name, startId, limit);
        List<T> rlist = null;
        if (dblist != null) {
            rlist = new ArrayList<>();
            Map<String, Object[]> pm = cm.get(this.getClass());
            for (DataBean db : dblist) {
                Map<String, java.nio.ByteBuffer> m = db.getTBean();
                try {
                    T t = (T) this.getClass().getDeclaredConstructor().newInstance();
                    t.getClass().getField("id").setLong(t, db.getId());
                    for (String key : pm.keySet()) {
                        String name = (String) pm.get(key)[0];
                        if (name == null) {
                            name = key;
                        }
                        if (m.containsKey(name)) {
                            Util.prase4set(t.getClass().getField(key), t, m.get(name));
                        }
                    }
                    rlist.add(t);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return rlist;
    }

    public T selectByIdx(String columnName, Object columnValue) throws TlException {
        Map<String, Object[]> pm = cm.get(this.getClass());
        try {
            T t = null;
            byte[] bs = Util.praseValue(this.getClass().getField(columnName), columnValue);
            if (bs != null) {
                DataBean db = this.client.selectByIdx(this.name, columnName, bs);
                if (db != null) {
                    Map<String, java.nio.ByteBuffer> m = db.getTBean();
                    if (m != null) {
                        t = (T) this.getClass().getDeclaredConstructor().newInstance();
                        t.getClass().getField("id").setLong(t, db.getId());
                        for (String key : pm.keySet()) {
                            String name = (String) pm.get(key)[0];
                            if (name == null) {
                                name = key;
                            }
                            if (m.containsKey(name)) {
                                Util.prase4set(t.getClass().getField(key), t, m.get(name));
                            }
                        }
                    }
                }
            }
            return t;
        } catch (Exception e) {
            throw new TlException(e);
        }
    }

    public List<T> selectAllByIdx(String columnName, Object columnValue) throws TlException {
        List<T> rlist = null;
        try {
            byte[] bs = Util.praseValue(this.getClass().getField(columnName), columnValue);
            List<DataBean> dblist = this.client.selectAllByIdx(this.name, columnName, bs);
            if (dblist != null && dblist.size() > 0) {
                rlist = new ArrayList<>();
                Map<String, Object[]> pm = cm.get(this.getClass());
                for (DataBean db : dblist) {
                    Map<String, java.nio.ByteBuffer> m = db.getTBean();
                    try {
                        T t = (T) this.getClass().getDeclaredConstructor().newInstance();
                        t.getClass().getField("id").setLong(t, db.getId());
                        for (String key : pm.keySet()) {
                            String name = (String) pm.get(key)[0];
                            if (name == null) {
                                name = key;
                            }
                            if (m.containsKey(name)) {
                                Util.prase4set(t.getClass().getField(key), t, m.get(name));
                            }
                        }
                        rlist.add(t);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } catch (Exception e) {
            throw new TlException(e);
        }
        return rlist;
    }

    public List<T> selectByIdxLimit(long startId, long limit, String columnName, Object... columnValue) throws TlException {
        List<T> rlist = null;
        try {
            List<byte[]> bslist = new ArrayList<>();
            for (Object o : columnValue) {
                byte[] bs = Util.praseValue(this.getClass().getField(columnName), o);
                if (bs != null) {
                    bslist.add(bs);
                }
            }
            List<DataBean> dblist = this.client.selectByIdxLimit(this.name, columnName, bslist, startId, limit);
            if (dblist != null && dblist.size() > 0) {
                rlist = new ArrayList<>();
                Map<String, Object[]> pm = cm.get(this.getClass());
                for (DataBean db : dblist) {
                    Map<String, java.nio.ByteBuffer> m = db.getTBean();
                    try {
                        T t = (T) this.getClass().getDeclaredConstructor().newInstance();
                        t.getClass().getField("id").setLong(t, db.getId());
                        for (String key : pm.keySet()) {
                            String name = (String) pm.get(key)[0];
                            if (name == null) {
                                name = key;
                            }
                            if (m.containsKey(name)) {
                                Util.prase4set(t.getClass().getField(key), t, m.get(name));
                            }
                        }
                        rlist.add(t);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        } catch (Exception e) {
            throw new TlException(e);
        }
        return rlist;
    }
}
