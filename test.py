import numpy as np
import pandas as pd

df = pd.DataFrame({
    "field": ["asda", np.NAN, "asdasd"]
})
df1 = pd.DataFrame({
    "field": ["asda", "asdasd", "asdasd"]
})
t = pd.DataFrame({
    "test": [1]
})

print(df.isnull().values.any())
print(df1.isnull().values.any())
print(type(t['test'][0]))