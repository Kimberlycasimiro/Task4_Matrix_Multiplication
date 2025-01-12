package org.ulpgc.matrix.parallel;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class ParallelMatrixMultiplication {

    private final ForkJoinPool pool;

    public ParallelMatrixMultiplication(int numThreads) {
        this.pool = new ForkJoinPool(numThreads);
    }

    public double[][] multiply(double[][] matrixA, double[][] matrixB) {
        int rows = matrixA.length;
        int cols = matrixB[0].length;
        int common = matrixB.length;
        double[][] result = new double[rows][cols];
        double[][] transposedB = transpose(matrixB);

        pool.invoke(new MultiplyTask(matrixA, transposedB, result, 0, rows, 0, cols, common));
        return result;
    }

    private static double[][] transpose(double[][] matrix) {
        int rows = matrix.length;
        int cols = matrix[0].length;
        double[][] transposed = new double[cols][rows];
        for (int i = 0; i < rows; i++) {
            for (int j = 0; j < cols; j++) {
                transposed[j][i] = matrix[i][j];
            }
        }
        return transposed;
    }

    private static class MultiplyTask extends RecursiveAction {
        private final double[][] matrixA;
        private final double[][] matrixB;
        private final double[][] result;
        private final int startRow, endRow, startCol, endCol, common;

        private static final int THRESHOLD = 64;

        MultiplyTask(double[][] matrixA, double[][] matrixB, double[][] result,
                     int startRow, int endRow, int startCol, int endCol, int common) {
            this.matrixA = matrixA;
            this.matrixB = matrixB;
            this.result = result;
            this.startRow = startRow;
            this.endRow = endRow;
            this.startCol = startCol;
            this.endCol = endCol;
            this.common = common;
        }

        @Override
        protected void compute() {
            if ((endRow - startRow) * (endCol - startCol) <= THRESHOLD) {
                for (int i = startRow; i < endRow; i++) {
                    for (int j = startCol; j < endCol; j++) {
                        result[i][j] = 0;
                        for (int k = 0; k < common; k++) {
                            result[i][j] += matrixA[i][k] * matrixB[j][k];
                        }
                    }
                }
            } else {
                int midRow = (startRow + endRow) / 2;
                int midCol = (startCol + endCol) / 2;

                invokeAll(
                        new MultiplyTask(matrixA, matrixB, result, startRow, midRow, startCol, midCol, common),
                        new MultiplyTask(matrixA, matrixB, result, startRow, midRow, midCol, endCol, common),
                        new MultiplyTask(matrixA, matrixB, result, midRow, endRow, startCol, midCol, common),
                        new MultiplyTask(matrixA, matrixB, result, midRow, endRow, midCol, endCol, common)
                );
            }
        }
    }
}
