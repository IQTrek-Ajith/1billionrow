package com.iqtrek.weather.ajith;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

class OneBillion {
    private static final ConcurrentHashMap<String, KeyValue> map = new ConcurrentHashMap<>();
    public static List<CountDownLatch> latches = new ArrayList<>();
    private static final AtomicInteger processedBatches = new AtomicInteger(0);
    private static final AtomicInteger totalBatches = new AtomicInteger(0);

    public static void main(String[] args) {
        final int numOfCores = Runtime.getRuntime().availableProcessors();
        int rowCount = 0;
        final int BATCH_SIZE = 8000;
        String csvFile = "./app/data/weather_stations.csv";
        ExecutorService executor = Executors.newFixedThreadPool(numOfCores);
        long startTime = System.nanoTime();

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
            List<String> batch = new ArrayList<>(BATCH_SIZE);
            String line;
            while ((line = br.readLine()) != null) {
                batch.add(line);
                rowCount++;
                if (batch.size() == BATCH_SIZE) {
                    submitBatch(executor, new ArrayList<>(batch));
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                submitBatch(executor, new ArrayList<>(batch));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            System.out.println("Waiting for tasks to complete...");
            while (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                System.out.println("Still waiting... Processed batches: " + processedBatches.get() + "/" + totalBatches.get());
                if(processedBatches.get() == totalBatches.get()) {
                    executor.shutdownNow();
                    break;
                }
            }
            System.out.println("All tasks completed. Waiting for latches...");
            for (int i = 0; i < latches.size(); i++) {
                CountDownLatch latch = latches.get(i);
                if (!latch.await(1, TimeUnit.SECONDS)) {
                    System.out.println("Latch " + i + " did not count down within timeout.");
                }
            }
            System.out.println("All latches processed.");
        } catch (InterruptedException e) {
            System.out.println("Interrupted while waiting for tasks to complete.");
            executor.shutdownNow();
        }
        executor.shutdown();
        long endTime = System.nanoTime();
        long executionTime = endTime - startTime;
        double executionTimeSeconds = (double) executionTime / 1_000_000_000;
        System.out.println("Number of CPU cores: " + numOfCores + ", Row count: " + rowCount + 
                           ", time: " + executionTime + ", time in sec: " + executionTimeSeconds);
        System.out.println("Out size: " + map.size());
        printbatch();
    }

    private static void submitBatch(ExecutorService executor, List<String> batch) {
        CountDownLatch latch = new CountDownLatch(1);
        latches.add(latch);
        totalBatches.incrementAndGet();
        executor.submit(() -> {
            try {
                processBatch(batch);
            } finally {
                latch.countDown();
            }
        });
    }

    public static void processBatch(List<String> batch) {
        for (String line : batch) {
            String[] columns = line.split(";");
            String key = columns[0];
            float val = Float.parseFloat(columns[1]);
            map.compute(key, (k, value) -> {
                if (value == null) {
                    return new KeyValue(val, val, val, 1);
                } else {
                    return new KeyValue(
                        Math.min(value.minimum, val),
                        value.sum + val,
                        Math.max(value.maximum, val),
                        value.count + 1
                    );
                }
            });
        }
        processedBatches.incrementAndGet();
    }

    public static void printbatch() {
      String outputFile = "output_results.csv";
      try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
          writer.write("Location,Minimum,Average,Maximum");
          writer.newLine();

          for (var entry : map.entrySet()) {
              String line = entry.getKey() + "," + entry.getValue().toString();
              writer.write(line);
              writer.newLine();
          }
          System.out.println("Results written to " + outputFile + " (total entries: " + map.size() + ")");
      } catch (IOException e) {
          System.err.println("Error writing to output file: " + e.getMessage());
          e.printStackTrace();
      }
  }
}