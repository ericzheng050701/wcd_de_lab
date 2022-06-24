#!/usr/bin python3

import pandas as pd
import glob
import os

input_path = '/your/direct/folder/path/of/input'
output_path = '/your/direct/folder/path/of/output'
filename = 'all_years.csv'
csvs = glob.glob(os.path.join(input_path , "*.csv"))


li = []
for f in csvs:
    df = pd.read_csv(f, index_col=None, header=0)
    li.append(df)

df_concat = pd.concat(li,axis=0, ignore_index=True)

df_concat.to_csv(output_path+"/"+filename)




