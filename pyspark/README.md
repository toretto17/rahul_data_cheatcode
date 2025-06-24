# 🧰 Nested JSON Flattener (PySpark)

This utility recursively flattens any deeply nested JSON or DataFrame (with arrays and structs) into a tabular format.

## 🔄 Function: `flatten_df(df)`
- Supports structs, arrays, and nested hierarchies.
- Designed for PySpark.
- Recursive and dynamic — no hardcoded schema assumptions.

## 📁 Sample Usage

```python
from flattening.flatten_json import flatten_df
df = spark.read.option("multiline", "true").json("sample_nested.json")
flat_df = flatten_df(df)
flat_df.printSchema()
flat_df.show(truncate=False)
