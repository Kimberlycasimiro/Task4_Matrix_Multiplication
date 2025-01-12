package org.ulpgc.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.ulpgc.matrix.basic.BasicMatrixMultiplication;
import org.ulpgc.matrix.parallel.ParallelMatrixMultiplication;
import org.ulpgc.matrix.mapreduce.MapReduceMatrixMultiplication;
import org.ulpgc.utils.MatrixGenerator;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 1, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class BenchmarkMatrixMultiplication {

    @State(Scope.Thread)
    public static class GlobalMatrixState {
        @Param({"64", "128", "256", "512", "1024"})
        public int matrixSize;

        public String inputDirPath;
        public String outputDirPath;

        public double[][] matrixA;
        public double[][] matrixB;

        @Setup(Level.Trial)
        public void setup() {
            inputDirPath = "src/main/resources/matrix_" + matrixSize + "/input";
            outputDirPath = "src/main/resources/matrix_" + matrixSize + "/output";

            matrixA = MatrixGenerator.generateDenseMatrix(matrixSize, matrixSize, 1, 10);
            matrixB = MatrixGenerator.generateDenseMatrix(matrixSize, matrixSize, 1, 10);

            MatrixGenerator.generateMatrixForMapReduce(matrixSize, matrixSize, "A", 1, 10, inputDirPath + "/matrixA.txt");
            MatrixGenerator.generateMatrixForMapReduce(matrixSize, matrixSize, "B", 1, 10, inputDirPath + "/matrixB.txt");
        }
    }

    @State(Scope.Thread)
    public static class ParallelState {
        @Param({"1", "2", "4", "8", "16"})
        public int numThreads;
    }

    @Benchmark
    public double[][] basicMultiplication(GlobalMatrixState state) {
        return new BasicMatrixMultiplication().multiply(state.matrixA, state.matrixB);
    }

    @Benchmark
    public double[][] parallelMultiplication(GlobalMatrixState matrixState, ParallelState parallelState) {
        return new ParallelMatrixMultiplication(parallelState.numThreads).multiply(matrixState.matrixA, matrixState.matrixB);
    }

    @Benchmark
    public double[][] mapReduceMultiplication(GlobalMatrixState state) {
        try {
            return MapReduceMatrixMultiplication.multiply(
                    state.inputDirPath + "/matrixA.txt",
                    state.inputDirPath + "/matrixB.txt",
                    state.outputDirPath,
                    state.matrixSize
            );
        } catch (Exception e) {
            throw new RuntimeException("Error during MapReduce multiplication", e);
        }
    }

    public static void main(String[] args) throws Exception {
        org.openjdk.jmh.runner.Runner runner = new org.openjdk.jmh.runner.Runner(new OptionsBuilder()
                .include(BenchmarkMatrixMultiplication.class.getSimpleName())
                .build());
        Collection<RunResult> results = runner.run();

        saveResultsAsJson(results);
    }

    private static void saveResultsAsJson(Collection<RunResult> results) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);

        List<Map<String, Object>> allResults = new ArrayList<>();

        for (RunResult result : results) {
            Map<String, Object> resultData = new HashMap<>();
            resultData.put("Benchmark", result.getParams().getBenchmark());
            resultData.put("matrixSize", result.getParams().getParam("matrixSize"));
            resultData.put("numThreads", result.getParams().getParam("numThreads"));
            resultData.put("Mode", result.getParams().getMode().name());
            resultData.put("Cnt", result.getParams().getMeasurement().getCount());
            resultData.put("Score", result.getPrimaryResult().getScore());
            resultData.put("Error", result.getPrimaryResult().getScoreError());
            resultData.put("Units", result.getPrimaryResult().getScoreUnit());

            allResults.add(resultData);
        }

        File outputFile = new File("benchmark_results.json");
        objectMapper.writeValue(outputFile, allResults);
    }
}