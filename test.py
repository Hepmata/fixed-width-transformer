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

p1 = pd.DataFrame(
    {
        "field1": [1,2,3],
        "field2": [1,2,3]
    }
)
p2 = pd.DataFrame(
    {
        "field2": [1] * 3
    }
)
print(p1.columns[0])
p1 = p1.rename(columns={p1.columns[0]: "somename"})
l1 = pd.DataFrame({"seg1": p1.to_dict('records')})
l2 = pd.DataFrame({"seg2": p2.to_dict('records')})
print(pd.concat([l1,l2], axis=1).to_dict('records'))