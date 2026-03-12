import duckdb
con = duckdb.connect('fishtalk.duckdb')
cols = con.execute("SELECT * FROM mediciones_data WHERE sheet_name='i-STAT' LIMIT 1").df().columns.tolist()
print(cols)
