package com.github.sionin.cassandra.client;

import com.github.sionin.cassandra.data.TORow;

import java.util.List;
import java.util.concurrent.ExecutionException;

public interface IClient {

    void cleanAndInsert(Iterable<TORow> rows);

    List<TORow> readAll();

    List<TORow> read(List<String> keys, int partition) throws ExecutionException, InterruptedException;

    void shutdown();
}
