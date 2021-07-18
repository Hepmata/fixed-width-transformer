import numpy as np
import pandas as pd

df = pd.DataFrame({
    "field": ["asda", np.NAN, "asdasd"]
})
df1 = pd.DataFrame({
    "field": ["asda", "asdasd", "asdasd"]
})


print(df.isnull().values.any())
print(df1.isnull().values.any())