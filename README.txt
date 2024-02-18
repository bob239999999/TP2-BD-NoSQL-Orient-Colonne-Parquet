Import des Bibliothèques
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq 
import pyarrow.compute as pc 

Import des Données
Les données sont importées depuis les fichiers CSV villes_virgule.csv et academies_virgule.csv à l'aide de pd.read_csv.

Arrow Functions:
    pa.Table.from_pandas: Converts a Pandas DataFrame into a PyArrow Table.
    pa.Table.to_pandas: Converts a PyArrow Table into a Pandas DataFrame.
    pq.write_table: Writes a PyArrow Table to a Parquet file.
    pq.read_table: Reads a Parquet file into a PyArrow Table.
PyArrow Compute Functions:
    pc.count: Counts the number of non-null elements in a column.
    pc.count_distinct: Counts the number of distinct elements in a column.
    pc.sum: Calculates the sum of the elements in a column.
    pc.min: Finds the minimum value in a column.
    pc.max: Finds the maximum value in a column.
    pc.equal: Compares values in a column for equality.
Pandas DataFrame Operations:
    pd.merge: Performs a database-style join operation on DataFrame objects.
    groupby: Groups DataFrame using a mapper or by a Series of columns.
Data Filtering and Sorting:
    Filtering based on specific conditions using pa.compute.equal.
    Sorting data based on column values using pa.compute.sort_indices.
Visualization:
    Matplotlib is used for data visualization, particularly for plotting histograms.
Error Handling:
    There are try-except blocks used for error handling, particularly in functions where DataFrame operations are performed.
