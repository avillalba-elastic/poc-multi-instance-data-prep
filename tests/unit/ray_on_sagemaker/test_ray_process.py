import pyarrow as pa

from poc_multi_instance_data_preparation.ray_on_sagemaker.ray_process import transform


def test_transform() -> None:
    vectors = [[1, 2, 3], [4, 5], [6]]
    batch = pa.table({"vector": pa.array(vectors, type=pa.list_(pa.int64()))})

    result = transform(batch)

    assert "vector_x2" in result.column_names
    assert result.num_rows == batch.num_rows

    expected = [[x * 2 for x in sublist] for sublist in vectors]
    result_values = result["vector_x2"].to_pylist()
    assert result_values == expected
