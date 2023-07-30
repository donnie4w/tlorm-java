/*
 * Copyright 2023 tldb Author. All Rights Reserved.
 * email: donnie4w@gmail.com
 * https://githuc.com/donnie4w/tldb
 * https://githuc.com/donnie4w/tlorm-java
 */

package io.github.donnie4w.tlorm;

import io.github.donnie4w.tldb.tlcli.*;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class TlormClientDemo {
    public static void main(String[] args) throws TlException {
        Orm.registerDefaultResource(false, "127.0.0.1", 7100, "mycli=123");
        UserInfo u = new UserInfo();
        u.createTable();
        long seq = u.insert(new UserInfo(0,"tom",22,"aaaa".getBytes(StandardCharsets.UTF_8),1.22f, (byte) 1,(char)222));
        System.out.println("seq--->"+seq);
        u.update(new UserInfo(1, "jerry", 22, "bbbb".getBytes(StandardCharsets.UTF_8), 1.22f, (byte) 1,(char)333));
        System.out.println(u.selectById(1));
        System.out.println("————————————————————————————————————————————————");
        List<UserInfo> uis =u.selectsByIdLimit(1,10);
        for (UserInfo v:uis){
            System.out.println(v);
        }
        System.out.println("————————————————————————————————————————————————");
        System.out.println(u.selectByIdx("name","tom"));
        System.out.println("————————————————————————————————————————————————");
        List<UserInfo> uis2 =u.selectAllByIdx("name","jerry");
        for (UserInfo v:uis2){
            System.out.println(v);
        }
        System.out.println("————————————————————————————————————————————————");
        List<UserInfo> uis3 =u.selectByIdxLimit(0,2,"name","jerry");
        for (UserInfo v:uis3){
            System.out.println(v);
        }
    }
}


class UserInfo extends Orm<UserInfo> {
    public long id;
    @Index
    public String name;
    public int age;
    public byte[] desc;
    @Field(name = "Achi")
    public float achi;
    public byte gender;
    public char char1;

    public UserInfo() {
    }

    public UserInfo(int id, String name, int age, byte[] desc, float achi, byte gender,char char1) {
        super();
        this.id = id;
        this.name = name;
        this.age = age;
        this.desc = desc;
        this.achi = achi;
        this.gender = gender;
        this.char1 = char1;
    }

    public String toString() {
        return id + "," + name + "," + age + "," + new String(desc, StandardCharsets.UTF_8) + "," + achi + "," + gender+","+(char1==(char)222);
    }
}