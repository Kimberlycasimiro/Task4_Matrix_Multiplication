class BasicMatrixMultiplication:
    @staticmethod
    def multiply(matrix_a, matrix_b):
        if len(matrix_a[0]) != len(matrix_b):
            raise ValueError("The dimensions of the matrices are not compatible for multiplication.")

        rows = len(matrix_a)
        cols = len(matrix_b[0])
        common = len(matrix_b)

        result = [[0.0 for _ in range(cols)] for _ in range(rows)]

        for i in range(rows):
            for j in range(cols):
                for k in range(common):
                    result[i][j] += matrix_a[i][k] * matrix_b[k][j]

        return result
