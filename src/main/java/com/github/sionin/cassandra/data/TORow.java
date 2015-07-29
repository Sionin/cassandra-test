package com.github.sionin.cassandra.data;

import java.util.ArrayList;
import java.util.List;

public class TORow {

    public String key;
    public List<TOColumn> columns;

    public TORow(String key) {
        this.key = key;
        this.columns = new ArrayList<TOColumn>();
    }

    public void add(TOColumn column) {
        columns.add(column);
    }

    public void add(String name, String value) {
        add(new TOColumn(name, value));
    }

    public void add(String name, String value, long time) {
        add(new TOColumn(name, value, time));
    }


}
