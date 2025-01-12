package org.ulpgc.matrix.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class MapReduceMatrixMultiplication {

    public static double[][] multiply(String matrixAPath, String matrixBPath, String outputDirPath, int matrixSize) throws IOException, InterruptedException, ClassNotFoundException {
        File outputDir = new File(outputDirPath);
        if (!outputDir.exists()) {
            Files.createDirectories(Paths.get(outputDirPath));
        }

        Path outputPath = new Path(outputDirPath);
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        conf.setInt("matrix.size", matrixSize);
        Job job = Job.getInstance(conf, "Matrix Multiplication");
        job.setJarByClass(MapReduceMatrixMultiplication.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(matrixAPath));
        FileInputFormat.addInputPath(job, new Path(matrixBPath));
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            throw new RuntimeException("MapReduce job failed: " + job.getStatus().getFailureInfo());
        }

        double[][] result = new double[matrixSize][matrixSize];
        File outputFile = new File(outputDir, "part-r-00000");
        try (BufferedReader reader = new BufferedReader(new FileReader(outputFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                String[] indices = parts[0].split(",");
                int row = Integer.parseInt(indices[0]);
                int col = Integer.parseInt(indices[1]);
                double value = Double.parseDouble(parts[1]);
                result[row][col] = value;
            }
        }

        return result;
    }

    public static class MatrixMapper extends Mapper<Object, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();
        private int matrixSize;

        @Override
        protected void setup(Context context) {
            matrixSize = context.getConfiguration().getInt("matrix.size", 0);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            String matrix = parts[0];
            int row = Integer.parseInt(parts[1]);
            int col = Integer.parseInt(parts[2]);
            double val = Double.parseDouble(parts[3]);

            if (matrix.equals("A")) {
                for (int k = 0; k < matrixSize; k++) {
                    outputKey.set(row + "," + k);
                    outputValue.set("A," + col + "," + val);
                    context.write(outputKey, outputValue);
                }
            } else if (matrix.equals("B")) {
                for (int i = 0; i < matrixSize; i++) {
                    outputKey.set(i + "," + col);
                    outputValue.set("B," + row + "," + val);
                    context.write(outputKey, outputValue);
                }
            }
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, Text> {
        private final Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Integer, Double> matrixA = new HashMap<>();
            Map<Integer, Double> matrixB = new HashMap<>();

            for (Text value : values) {
                String[] parts = value.toString().split(",");
                String matrix = parts[0];
                int index = Integer.parseInt(parts[1]);
                double val = Double.parseDouble(parts[2]);

                if (matrix.equals("A")) {
                    matrixA.put(index, val);
                } else if (matrix.equals("B")) {
                    matrixB.put(index, val);
                }
            }

            double sum = 0;
            for (Map.Entry<Integer, Double> entryA : matrixA.entrySet()) {
                sum += entryA.getValue() * matrixB.getOrDefault(entryA.getKey(), 0.0);
            }

            outputValue.set(String.valueOf(sum));
            context.write(key, outputValue);
        }
    }
}
