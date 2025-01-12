import os
import pytest

from utils.matrix_generator import MatrixGenerator
from matrix.basic_matrix_multiplication import BasicMatrixMultiplication
from matrix.parallel_matrix_multiplication import ParallelMatrixMultiplication
from matrix.mapreduce_matrix_multiplication import DistributedMatrixMultiplication

@pytest.fixture(scope="module")
def setup_mapreduce_matrices():
    matrix_sizes = [64, 128, 256, 512, 1024]
    base_path = "benchmark_results"
    mapreduce_matrices = {}

    for size in matrix_sizes:
        input_dir = os.path.join(base_path, f"matrix_{size}", "input")
        output_dir = os.path.join(base_path, f"matrix_{size}", "output")
        os.makedirs(input_dir, exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)

        matrix_a_path = os.path.join(input_dir, "MatrixA.txt")
        matrix_b_path = os.path.join(input_dir, "MatrixB.txt")
        MatrixGenerator.generate_matrix_for_mapreduce(size, size, "A", 1, 10, matrix_a_path)
        MatrixGenerator.generate_matrix_for_mapreduce(size, size, "B", 1, 10, matrix_b_path)

        mapreduce_matrices[size] = {
            "input_dir": input_dir,
            "output_dir": output_dir,
            "matrix_a_path": matrix_a_path,
            "matrix_b_path": matrix_b_path,
        }
    return mapreduce_matrices


@pytest.fixture(scope="module")
def setup_in_memory_matrices():
    matrix_sizes = [64, 128, 256, 512, 1024]
    in_memory_matrices = {}
    for size in matrix_sizes:
        matrix_a = MatrixGenerator.generate_dense_matrix(size, size)
        matrix_b = MatrixGenerator.generate_dense_matrix(size, size)
        in_memory_matrices[size] = {
            "matrix_a": matrix_a,
            "matrix_b": matrix_b,
        }
    return in_memory_matrices


@pytest.mark.parametrize("size", [64, 128, 256, 512, 1024])
def test_basic_multiplication(benchmark, setup_in_memory_matrices, size):
    matrix_a = setup_in_memory_matrices[size]["matrix_a"]
    matrix_b = setup_in_memory_matrices[size]["matrix_b"]

    def multiply():
        return BasicMatrixMultiplication().multiply(matrix_a, matrix_b)

    result = benchmark(multiply)
    assert len(result) == size and len(result[0]) == size


@pytest.mark.parametrize("size", [64, 128, 256, 512, 1024])
@pytest.mark.parametrize("num_threads", [1, 2, 4, 8, 16])
def test_parallel_multiplication(benchmark, setup_in_memory_matrices, size, num_threads):
    matrix_a = setup_in_memory_matrices[size]["matrix_a"]
    matrix_b = setup_in_memory_matrices[size]["matrix_b"]

    parallel_multiplication = ParallelMatrixMultiplication(num_threads)

    def multiply():
        return parallel_multiplication.multiply(matrix_a, matrix_b)

    result = benchmark(multiply)
    assert len(result) == size and len(result[0]) == size


@pytest.mark.parametrize("size", [64, 128, 256, 512, 1024])
def test_mapreduce_multiplication(benchmark, setup_mapreduce_matrices, size):
    config = setup_mapreduce_matrices[size]
    matrix_a_path = config["matrix_a_path"]
    matrix_b_path = config["matrix_b_path"]
    output_dir = config["output_dir"]

    def multiply():
        job = DistributedMatrixMultiplication(args=[
            f'--matrix-a={matrix_a_path}',
            f'--matrix-b={matrix_b_path}'
        ])
        job.sandbox()
        with job.make_runner() as runner:
            runner.run()

    benchmark(multiply)
