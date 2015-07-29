package com.github.sionin.cassandra.client;

import com.github.sionin.cassandra.data.TORow;

import java.util.List;

public interface IClient {

    void cleanAndInsert(Iterable<TORow> rows);

    List<TORow> readAll();

    void shutdown();
}
