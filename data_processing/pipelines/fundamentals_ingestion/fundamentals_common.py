from pyspark import Row

BitemporalFundamentalValue = Row("metric", "ticker", "date", "value", "as_of_start", "as_of_end", "raw_value")
