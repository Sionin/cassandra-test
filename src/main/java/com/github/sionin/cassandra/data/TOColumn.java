package com.github.sionin.cassandra.data;

public class TOColumn {

    public String name;
    public String value;
    public long timestamp;

    public TOColumn(String name, String value) {
        this.name = name;
        this.value = value;
    }

    public TOColumn(String name, String value, long timestamp) {
        this.name = name;
        this.value = value;
        this.timestamp = timestamp;
    }


}
