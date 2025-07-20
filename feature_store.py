import pyarrow.parquet as pq

import pandas as pd, polars as pl
from pathlib import Path
PARQUET_DIR=Path("data/parquet")
from typing import Optional, Iterable, Tuple

def read_prices(
    cols: Iterable[str] = ("close",),
    start: Optional[str] = None,
    end: Optional[str]   = None,
    tickers: Optional[Iterable[str]] = None,
)-> pd.DataFrame:

    if isinstance(cols,str):
        cols=(cols,)
    cols:Tuple[str,...] = tuple(cols)
    lf = pl.scan_parquet(
        str(PARQUET_DIR),
        hive_partitioning=True,
    )
    selects:List[str]=["date","ticker",*cols]
    lf=lf.select(selects).collect()
    dfs=[]
    for col in cols:
        pivoted=(
            lf.pivot(values=col,index="date",columns="ticker",aggregate_function="first")
            .to_pandas()
            .set_index("date")
        )
        pivoted.columns=[f"{col}_{ticker}" for ticker in pivoted.columns]
        dfs.append(pivoted)
    df=pd.concat(dfs,axis=1).sort_index()
    print(df)
    return df
read_prices(("close","volume"))





