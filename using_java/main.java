import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

class OneBillion {

    private static final ConcurrentHashMap<String, KeyValue> map = new ConcurrentHashMap<>();

    public static void main(String[] args) {
      final int numOfCores = Runtime.getRuntime().availableProcessors();
      int rowCount = 0;
      final int BATCH_SIZE = 1000;
      String csvFile = "/home/user/Learning/1billion/data/weather_stations.csv";
      
      ExecutorService executor = Executors.newFixedThreadPool(numOfCores);

      long startTime = System.nanoTime();

      try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
        List<String> batch = new ArrayList<>(BATCH_SIZE);
        String line;
        while ((line = br.readLine()) != null) {
          batch.add(line);
          rowCount+=1;
          if (batch.size() == BATCH_SIZE) {
              List<String> currentBatch = new ArrayList<>(batch);
              executor.submit(() -> processBatch(currentBatch));
              batch.clear();
          }
        }
        if (!batch.isEmpty()) {
          List<String> currentBatch = new ArrayList<>(batch);
          executor.submit(() -> processBatch(currentBatch));
        }
      } catch (IOException e) {
        e.printStackTrace();
      }

      executor.shutdown();

      try {
        if (!executor.awaitTermination(60, TimeUnit.MINUTES)) {
          executor.shutdownNow();
        }
      } catch (InterruptedException e) {
        executor.shutdownNow();
      }
      long endTime = System.nanoTime();
      long executionTime = endTime - startTime;
      double executionTime2 = (double) executionTime / 1000000000;

      System.out.println("Number of CPU cores: " + numOfCores + ", Row count: " + rowCount + ", time: " + executionTime + ", time in sec: " + executionTime2);
      printbatch();
  }

  public static void processBatch(List<String> batch) {
    for (String line : batch) {
      String[] columns = line.split(",");
      String key = columns[0];
      float val = Float.parseFloat(columns[1]);

      map.compute(key, (k, value) -> {
        if (value == null) {
          return new KeyValue(val, val, val, 1);
        } else {
            float oldMin = value.minimum;
            float oldMax = value.maximum;
            float oldSum = value.sum;
            int oldCount = value.count;

            if (val < oldMin) {
                oldMin = val;
            }
            if (val > oldMax) {
                oldMax = val;
            }
            oldSum += val;
            oldCount += 1;

            return new KeyValue(oldMin, oldSum, oldMax, oldCount);
        }
    });
    }
  }

  public static void printbatch(){
    map.forEach((key, value) -> System.out.println(key + ": " + value.toString()));
  }
}