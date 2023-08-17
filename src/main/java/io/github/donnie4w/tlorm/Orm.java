/**
 * Copyright 2023 tldb Author. All Rights Reserved.
 * email: donnie4w@gmail.com
 * https://githuc.com/donnie4w/tldb
 * https://githuc.com/donnie4w/tlorm-java
 */

package io.github.donnie4w.tlorm;

import io.github.donnie4w.tldb.tlcli.*;

import static io.github.donnie4w.tlorm.Util.*;

import java.lang.reflect.Field;
import java.util.*;
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
        prase(false);
        if (this.getClass().isAnnotationPresent(DefName.class)) {
            this.name = this.getClass().getAnnotation(DefName.class).name();
        } else {
            this.name = this.getClass().getSimpleName().toLowerCase();
        }
        this.setClient(defaultClient);
    }

    public void setClient(Client client) {
        this.client = client;
    }

    private void prase(boolean force) {
        if (!cm.containsKey(this.getClass()) || force) {
            Map<String, Object[]> fm = new HashMap<>();
            for (Field f : this.getClass().getFields()) {
                if (!"id".equals(f.getName().toLowerCase())) {
                    String fieldname = null;
                    if (f.isAnnotationPresent(DefName.class)) {
                        DefName field = f.getAnnotation(DefName.class);
                        fieldname = field.name();
                    }
                    fm.put(f.getName(), new Object[]{fieldname, f.isAnnotationPresent(Index.class), f.getType().getSimpleName().toLowerCase()});
                } else if (!"long".equals(f.getType().getSimpleName())) {
                    throw new TlRunTimeException("not found \"id\" in long type");
                }
            }
            cm.put(this.getClass(), fm);
        }
    }

    public void createTable() throws TlException {
        Map<String, Object[]> pm = cm.get(this.getClass());
        Map<String, ColumnType> fm = new HashMap<>();
        List<String> idxlist = new ArrayList<>();
        for (Map.Entry<String, Object[]> me : pm.entrySet()) {
            if (!(me.getKey().length() == 2 && "id".equals(me.getKey().toLowerCase()))) {
                String name0 = (String) me.getValue()[0];
                ColumnType ct = praseColumnType((String) me.getValue()[2]);
                if (name0 == null) {
                    name0 = me.getKey();
                }
                fm.put(name0, ct);
                if ((Boolean) me.getValue()[1]) {
                    idxlist.add(name0);
                }
            }
        }
        this.client.createTable(this.name, fm, idxlist.toArray(new String[]{}));
    }

    public long insert() throws TlException {
        return this.insert((T) this);
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
                if (!(key.length() == 2 && "id".equals(key.toLowerCase()))) {
                    byte[] bs = Util.prase(o.getClass().getField(key), o, false);
                    if (bs != null) {
                        m.put(name, bs);
                    }
                }
            }
            AckBean ab = defaultClient.insert(this.name, m);
            return ab.seq;
        } catch (Exception e) {
            throw new TlException(e);
        }
    }

    public long update() throws TlException {
        return this.update((T) this);
    }

    /**
     * Update data for Non null values
     */
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
                if (!(key.length() == 2 && "id".equals(key.toLowerCase()))) {
                    byte[] bs = Util.prase(o.getClass().getField(key), o, false);
                    if (bs != null) {
                        m.put(name, bs);
                    }
                }
            }
            AckBean ab = this.client.update(this.name, id, m);
            return ab.seq;
        } catch (Exception e) {
            throw new TlException(e);
        }
    }

    public long updateNonzero() throws TlException {
        return this.updateNonzero((T) this);
    }

    /**
     * Update data for Nonzero values
     */
    public long updateNonzero(T o) throws TlException {
        Map<String, byte[]> m = new HashMap<>();
        try {
            Map<String, Object[]> pm = cm.get(this.getClass());
            long id = o.getClass().getField("id").getLong(o);
            for (String key : pm.keySet()) {
                String name0 = (String) pm.get(key)[0];
                if (name0 == null) {
                    name0 = key;
                }
                if (!"id".equals(key.toLowerCase())) {
                    byte[] bs = Util.prase(o.getClass().getField(key), o, true);
                    if (bs != null) {
                        m.put(name0, bs);
                    }
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
        prase(true);
        Map<String, Object[]> pm = cm.get(this.getClass());
        Map<String, ColumnType> fm = new HashMap<>();
        List<String> idxlist = new ArrayList<>();
        for (Map.Entry<String, Object[]> me : pm.entrySet()) {
            if (!(me.getKey().length() == 2 && "id".equals(me.getKey().toLowerCase()))) {
                String name0 = (String) me.getValue()[0];
                ColumnType ct = praseColumnType((String) me.getValue()[2]);
                if (name0 == null) {
                    name0 = me.getKey();
                }
                fm.put(name0, ct);
                if ((Boolean) me.getValue()[1]) {
                    idxlist.add(name0);
                }
            }
        }
        this.client.alterTable(this.name, fm, idxlist.toArray(new String[]{}));
    }

    public long selectId() throws TlException {
        return this.client.selectId(this.name);
    }

    public long selectIdByIdx(String columnName, Object columnValue) throws TlException {
        try {
            byte[] bs = Util.praseValue(this.getClass().getField(columnName), columnValue);
            if (bs != null) {
                return this.client.selectIdByIdx(this.name, columnName, bs);
            }
        } catch (Exception e) {
            throw new TlException(e);
        }
        return 0;
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
                    String name0 = (String) pm.get(key)[0];
                    if (name0 == null) {
                        name0 = key;
                    }
                    if (m.containsKey(name0)) {
                        Util.prase4set(t.getClass().getField(key), t, m.get(name0));
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
                        String name0 = (String) pm.get(key)[0];
                        if (name0 == null) {
                            name0 = key;
                        }
                        if (m.containsKey(name0)) {
                            Util.prase4set(t.getClass().getField(key), t, m.get(name0));
                        }
                    }
                    rlist.add(t);
                } catch (Exception e) {
                    throw new TlException(e);
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
                            String name0 = (String) pm.get(key)[0];
                            if (name0 == null) {
                                name0 = key;
                            }
                            if (m.containsKey(name0)) {
                                Util.prase4set(t.getClass().getField(key), t, m.get(name0));
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
                rlist = new ArrayList<T>();
                Map<String, Object[]> pm = cm.get(this.getClass());
                for (DataBean db : dblist) {
                    Map<String, java.nio.ByteBuffer> m = db.getTBean();
                    try {
                        T t = (T) this.getClass().getDeclaredConstructor().newInstance();
                        t.getClass().getField("id").setLong(t, db.getId());
                        for (String key : pm.keySet()) {
                            String name0 = (String) pm.get(key)[0];
                            if (name0 == null) {
                                name0 = key;
                            }
                            if (m.containsKey(name0)) {
                                Util.prase4set(t.getClass().getField(key), t, m.get(name0));
                            }
                        }
                        rlist.add(t);
                    } catch (Exception e) {
                        throw new TlException(e);
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
                            String name0 = (String) pm.get(key)[0];
                            if (name0 == null) {
                                name0 = key;
                            }
                            if (m.containsKey(name0)) {
                                Util.prase4set(t.getClass().getField(key), t, m.get(name0));
                            }
                        }
                        rlist.add(t);
                    } catch (Exception e) {
                        throw new TlException(e);
                    }
                }
            }
        } catch (Exception e) {
            throw new TlException(e);
        }
        return rlist;
    }
}
