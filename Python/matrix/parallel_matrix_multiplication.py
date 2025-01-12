import numpy as np
from concurrent.futures import ThreadPoolExecutor

class ParallelMatrixMultiplication:
    def __init__(self, num_threads=4):
        self.num_threads = num_threads

    def multiply(self, matrix_a, matrix_b):
        rows = len(matrix_a)
        cols = len(matrix_b[0])
        common = len(matrix_b)
        result = np.zeros((rows, cols))
        transposed_b = self.transpose(matrix_b)

        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            tasks = []
            self._create_tasks(matrix_a, transposed_b, result, 0, rows, 0, cols, common, executor, tasks)
            for task in tasks:
                task.result()

        return result

    @staticmethod
    def transpose(matrix):
        return np.array(matrix).T

    def _create_tasks(self, matrix_a, matrix_b, result, start_row, end_row, start_col, end_col, common, executor, tasks, threshold=256):
        if (end_row - start_row) * (end_col - start_col) <= threshold:
            tasks.append(executor.submit(self._compute, matrix_a, matrix_b, result, start_row, end_row, start_col, end_col, common))
        else:
            mid_row = (start_row + end_row) // 2
            mid_col = (start_col + end_col) // 2

            self._create_tasks(matrix_a, matrix_b, result, start_row, mid_row, start_col, mid_col, common, executor, tasks)
            self._create_tasks(matrix_a, matrix_b, result, start_row, mid_row, mid_col, end_col, common, executor, tasks)
            self._create_tasks(matrix_a, matrix_b, result, mid_row, end_row, start_col, mid_col, common, executor, tasks)
            self._create_tasks(matrix_a, matrix_b, result, mid_row, end_row, mid_col, end_col, common, executor, tasks)

    @staticmethod
    def _compute(matrix_a, matrix_b, result, start_row, end_row, start_col, end_col, common):
        for i in range(start_row, end_row):
            for j in range(start_col, end_col):
                result[i][j] = np.dot(matrix_a[i, :common], matrix_b[j, :common])
