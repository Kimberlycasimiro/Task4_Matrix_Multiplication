import os
import numpy as np

class MatrixGenerator:
    @staticmethod
    def generate_dense_matrix(rows, cols, min_value=1, max_value=10):
        if rows <= 0 or cols <= 0:
            raise ValueError("Rows and columns must be greater than 0.")
        return np.random.uniform(min_value, max_value, (rows, cols))

    @staticmethod
    def generate_matrix_for_mapreduce(rows, cols, matrix_type, min_value, max_value, file_path):
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        with open(file_path, "w") as file:
            for i in range(rows):
                for j in range(cols):
                    value = np.random.uniform(min_value, max_value)
                    file.write(f"{matrix_type},{i},{j},{value}\n")
        print(f"Matrix {matrix_type} saved to: {file_path}")
