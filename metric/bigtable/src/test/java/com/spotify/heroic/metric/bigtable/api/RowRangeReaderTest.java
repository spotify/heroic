package com.spotify.heroic.metric.bigtable.api;

import com.google.bigtable.v2.ReadRowsResponse.CellChunk;
import com.google.protobuf.ByteString;

import com.spotify.heroic.metric.bigtable.api.RowRangeReader.CellChunkHandler;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;

public class RowRangeReaderTest {

    @Test
    public void testCellChunkHandler() throws IOException {
        Random random = new Random(1);
        List<CellChunk> cells = generateCells(random);
        List<CellChunk> result = collectChunks(random, cells);
        assertEquals(cells, result);
    }

    @Test
    public void testCellChunkHandlerWithReset() throws IOException {
        Random random = new Random(2);
        List<CellChunk> cells = generateCells(random);
        List<CellChunk> cellsWithReset = addResets(random, cells);
        List<CellChunk> result = collectChunks(random, cellsWithReset);
        assertEquals(cells, result);
    }

    @Test
    public void testCellChunkHandlerWithResetAndIncompleteCells() throws IOException {
        Random random = new Random(3);
        List<CellChunk> cells = generateCells(random);
        List<CellChunk> cellsWithReset = addResets(random, cells);
        List<CellChunk> chunkedcellsWithReset = splitCells(random, cellsWithReset);
        List<CellChunk> result = collectChunks(random, chunkedcellsWithReset);

        assertEquals(cells, result);
    }

    private static List<CellChunk> collectChunks(Random random, List<CellChunk> chunks) {
        List<List<CellChunk>> dataObserver = new ArrayList<>();
        CellChunkHandler handler = new CellChunkHandler(dataObserver::add);

        while (!chunks.isEmpty()) {
            int numberToConsume = Math.min(random.nextInt(1000), chunks.size());
            handler = handler.consumeChunks(chunks.subList(0, numberToConsume));
            chunks = chunks.subList(numberToConsume, chunks.size());
        }

        return dataObserver.stream().flatMap(List::stream).collect(Collectors.toList());
    }

    private static List<CellChunk> generateCells(Random random) {
        int numToCreate = 1000000;
        return IntStream
            .range(0, numToCreate)
            .mapToObj(i -> newCell(random).setCommitRow(i + 1 == numToCreate))
            .map(cc -> random.nextDouble() < 0.01 ? cc.setCommitRow(true).build() : cc.build())
            .collect(Collectors.toList());
    }

    private static List<CellChunk> addResets(Random random, List<CellChunk> cells) {
        int lastCommit = -1;
        List<CellChunk> cellsWithReset = new ArrayList<>();
        for (int i = 0; i < cells.size(); i++) {
            CellChunk cell = cells.get(i);
            cellsWithReset.add(cell);
            if (cell.getCommitRow()) {
                lastCommit = i;
            } else if (random.nextDouble() < 0.003) {
                cellsWithReset.add(CellChunk.newBuilder().setResetRow(true).build());
                for (int j = lastCommit + 1; j <= i; j++) {
                    cellsWithReset.add(cells.get(j));
                }
            }
        }
        return cellsWithReset;
    }

    private static List<CellChunk> splitCells(Random random, List<CellChunk> cells) {
        List<CellChunk> chunks = new ArrayList<>();
        cells.forEach(cell -> {
            if (!cell.getCommitRow() && !cell.getResetRow() && random.nextDouble() < 0.2) {
                ByteString value = cell.getValue();
                int valueSize = value.size();
                while (true) {
                    int numberToConsume = random.nextInt(4) + 1;
                    if (numberToConsume >= value.size()) {
                        chunks.add(cell.toBuilder().setValue(value).setValueSize(0).build());
                        break;
                    } else {
                        chunks.add(cell
                            .toBuilder()
                            .setValue(value.substring(0, numberToConsume))
                            .setValueSize(valueSize)
                            .build());
                        value = value.substring(numberToConsume);
                    }
                }
            } else {
                chunks.add(cell);
            }
        });
        return chunks;
    }

    private static CellChunk.Builder newCell(Random random) {
        return CellChunk
            .newBuilder()
            .setValue(ByteString.copyFromUtf8(Long.toHexString(random.nextLong())));
    }
}
