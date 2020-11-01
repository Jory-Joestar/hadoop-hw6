package com.hw6;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

public class UserTuple implements WritableComparable<UserTuple> {
    public String user1;
    public String user2;

    @Override
    public void readFields(DataInput in) throws IOException{
        this.user1=in.readUTF();
        this.user2=in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeUTF(this.user1);
        out.writeUTF(this.user2);
    }

    @Override
    public int compareTo(UserTuple o){
        return (user1+user2).compareTo(o.user1+o.user2);
    }

    public UserTuple(String u1,String u2){
        super();
        if (u1.compareTo(u2)>0) {
            this.user1=u2;
            this.user2=u1;
        } else {
            this.user2=u2;
            this.user1=u1;
        }
    }

    public UserTuple(){
        super();
    }

    public String get_tuple(){
        return "["+user1+","+user2+"]";
    }

}