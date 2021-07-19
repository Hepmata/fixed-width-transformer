import numpy as np
import pandas as pd

# 
# data = [
#     {"d1": [1, 2, 3]},
#     {"d2": [4, 5, 6]}
# ]
# 
# dfs = []
# for k in data:
#     dfs.append(pd.DataFrame(k))
# 
# concat = pd.concat(dfs, axis=1)
# print(dfs)
# print(concat.to_dict('records'))

p = pd.DataFrame({
    "1": [1]*5
})

print(p)