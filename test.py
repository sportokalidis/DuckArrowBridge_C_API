import duckarrow
import pyarrow as pa

# Create an instance of DataProcessor
processor = duckarrow.DataProcessor()

# Execute a query and get the result as an Arrow Table
arrow_table = processor.processQuery("SELECT * FROM 'C:\\Users\\stavr\\Documents\\data\\test_output_light.parquet'")
print("Arrow Table:" )
print(arrow_table)
# Convert the arrow::Table to pyarrow.Table
# t = pa.table([
#       pa.array(["a", "a", "b", "b", "c"]),
#       pa.array([1, 2, 3, 4, 5]),
# ], names=["keys", "values"])
# print(t)
# pyarrow_table = pa.Table.from_pandas(arrow_table.to_pandas())
# print(pyarrow_table)