import pandas as pd
import numpy as np
d= pd.DataFrame({'number': np.random.randint(1, 100, 10)})
d['bins'] = pd.cut(x=d['number'], bins=[1, 20, 40, 60,
                                          80, 100])
print(d)
