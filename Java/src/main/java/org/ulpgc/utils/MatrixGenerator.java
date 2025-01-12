package org.ulpgc.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;

public class MatrixGenerator {

    private static final Random RANDOM = new Random();

    public static double[][] generateDenseMatrix(int rows, int cols, double minValue, double maxValue) {
        validateParameters(rows, cols, minValue, maxValue);
        double[][] matrix = new double[rows][cols];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                matrix[i][j] = getRandomValue(minValue, maxValue);
            }
        }
        return matrix;
    }

    public static void generateMatrixForMapReduce(int rows, int cols, String matrixType, double minValue, double maxValue, String filePath) {
        validateParameters(rows, cols, minValue, maxValue);
        StringBuilder builder = new StringBuilder();

        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                double value = getRandomValue(minValue, maxValue);
                builder.append(matrixType).append(",").append(i).append(",").append(j).append(",").append(value).append("\n");
            }
        }

        try {
            Files.createDirectories(Paths.get(filePath).getParent());
            Files.write(Paths.get(filePath), builder.toString().getBytes(), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            System.out.println("Matrix " + matrixType + " saved to: " + filePath);
        } catch (IOException e) {
            System.err.println("Error writing matrix to file: " + e.getMessage());
        }
    }

    private static double getRandomValue(double minValue, double maxValue) {
        return minValue + (maxValue - minValue) * RANDOM.nextDouble();
    }

    private static void validateParameters(int rows, int cols, double minValue, double maxValue) {
        if (rows <= 0 || cols <= 0) {
            throw new IllegalArgumentException("Rows and columns must be greater than 0.");
        }
        if (minValue >= maxValue) {
            throw new IllegalArgumentException("minValue must be less than maxValue.");
        }
    }
}
