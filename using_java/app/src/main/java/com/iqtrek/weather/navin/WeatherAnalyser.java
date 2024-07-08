package com.iqtrek.weather.navin;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class WeatherAnalyser {
    private static final Logger logger = LoggerFactory.getLogger(WeatherAnalyser.class);

    private static final String FILE = "../data/weather_stations.csv";

    public static void main(String[] args) {
        try {
            new WeatherAnalyser().start();
        } catch (IOException e) {
            logger.error("Check the file path {}",e.getMessage());
        }
    }

    private void start() throws IOException {
        final int availableProcessors = Runtime.getRuntime().availableProcessors();
        final long startTime = System.currentTimeMillis();
        logger.info("Number of available processors: {}", availableProcessors);
        //add a try with finally block to close the file

        Map<String, Measurement> map = Files.lines(Paths.get(FILE)).parallel()
                .map(Measurement::parseLine)
                .collect(Collectors.toMap(Measurement::getName, Function.identity(), Measurement::aggregate));
//        logger.info("{}", map);
        logger.info("Execution time: {} ms", System.currentTimeMillis() - startTime);

        printBatch(map);
    }

    private void printBatch(Map<String, Measurement> map) {
        String outputFile = "output_results.csv";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
            writer.write("Location,Minimum,Average,Maximum");
            writer.newLine();

            for (var entry : map.entrySet()) {
                String line = entry.getKey() + "," + entry.getValue().toString();
                writer.write(line);
                writer.newLine();
            }
            logger.info("Results written to {} (total entries: {})", outputFile, map.size());
        } catch (IOException e) {
            logger.error("Error writing to output file: {}", e.getMessage());
        }
    }
}
