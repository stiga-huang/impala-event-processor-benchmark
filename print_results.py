#!/usr/bin/python3

import pandas as pd
import sys

def process_data(file_path):
  df = pd.read_csv(file_path, header=None, names=['raw_data'])

  df[['test', 'lag']] = df['raw_data'].str.extract(
    r'tests/hive_(.*?)\.sh>>+([0-9]*\.?[0-9]+)'
  )

  df['lag'] = df['lag'].astype(float)

  result = df.groupby('test')['lag'].agg(
    ['min', 'mean', 'median', 'max']
  ).reset_index()

  result.columns = ['Test', 'Min', 'Mean', 'Median', 'Max']
  return result.round(3)

if __name__ == "__main__":
  if len(sys.argv) < 1:
    print("Usage: test_log_file")
    exit(1)
  file_path = sys.argv[1]
  result = process_data(file_path)
  print(result.to_string(index=False))
  print("CSV Data")
  print(result.to_csv())
