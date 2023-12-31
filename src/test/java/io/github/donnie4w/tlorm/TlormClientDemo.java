/*
 * Copyright 2023 tldb Author. All Rights Reserved.
 * email: donnie4w@gmail.com
 * https://githuc.com/donnie4w/tldb
 * https://githuc.com/donnie4w/tlorm-java
 */

package io.github.donnie4w.tlorm;

import io.github.donnie4w.tldb.tlcli.*;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class TlormClientDemo {
    public static void main(String[] args) throws TlException {
        Orm.registerDefaultResource(true, "127.0.0.1", 3336, "mycli=123");
        UserInfo u = new UserInfo();
        u.createTable();
        for (int i=1;i<100;i++){
            long seq = u.insert(new UserInfo(0, "tom", i, "aaaa".getBytes(StandardCharsets.UTF_8), 1.22f, (byte) 1, (char) 222));
            System.out.println("seq >>>" + seq);
        }
        u.update(new UserInfo(1, "jerry", 22, "bbbb".getBytes(StandardCharsets.UTF_8), 1.22f, (byte) 1, (char) 333));
        UserInfo user = new UserInfo();
        user.id = 1;
        user.name = "jerrytom";
        u.updateNonzero(user);
        System.out.println(u.selectById(1));
        System.out.println("————————————————————————————————————————————————");
        List<UserInfo> uis = u.selectsByIdLimit(1, 10);
        if (uis != null) {
            for (UserInfo v : uis) {
                System.out.println(v);
            }
        }
        System.out.println("————————————————————————————————————————————————");
        System.out.println(u.selectByIdx("name", "tom"));
        System.out.println("————————————————————————————————————————————————");
        List<UserInfo> uis2 = u.selectAllByIdx("name", "tom");
        if (uis2 != null) {
            for (UserInfo v : uis2) {
                System.out.println(v);
            }
        }
        System.out.println("————————————————————————————————————————————————");
        List<UserInfo> uis3 = u.selectByIdxLimit(0, 2, "name", "tom");
        if (uis2 != null) {
            for (UserInfo v : uis3) {
                System.out.println(v);
            }
        }
    }

    @Test
    public void Test() throws TlException {
        Orm.registerDefaultResource(true, "127.0.0.1", 3336, "mycli=123");
        UserInfo u = new UserInfo();
        List<UserInfo> list = u.selectByIdxDescLimit("uuid",22,22,1);
        for (UserInfo ui:list){
            System.out.println(ui);
        }
        System.out.println("--------------------------------------");
        List<UserInfo> list2 = u.selectByIdxAscLimit("uuid",22,1,3);
        for (UserInfo ui:list2){
            System.out.println(ui);
        }
        System.out.println("--------------------------------------");
        List<UserInfo> list3 = u.selectAllByIdx("uuid",22);
        for (UserInfo ui:list3){
            System.out.println(ui);
        }
        System.out.println("--------------------------------------");
        List<UserInfo> list4 = u.selectByIdxLimit(0,2,"uuid",22);
        for (UserInfo ui:list4){
            System.out.println(ui);
        }
    }
}

class UserInfo extends Orm<UserInfo> {
    public long id;
    @Index
    public String name;

    @Index
    public int uuid;
    public byte[] desc;
    @DefName(name = "Achi")
    public float achi;
    public byte gender;
    public char char1;

    public UserInfo() {
    }

    public UserInfo(int id, String name, int uuid, byte[] desc, float achi, byte gender, char char1) {
        super();
        this.id = id;
        this.name = name;
        this.uuid = uuid;
        this.desc = desc;
        this.achi = achi;
        this.gender = gender;
        this.char1 = char1;
    }

    public String toString() {
        return id + "," + name + "," + uuid + "," + new String(desc, StandardCharsets.UTF_8) + "," + achi + "," + gender + "," + (short) char1;
    }
}