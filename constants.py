PROJECT_NAME = 'astock'
DATABASE_NAME = PROJECT_NAME
DATE_FORMAT = '%Y%m%d'
PAGE_SIZE = 6000
FIELDS_DDL = {"ts_code": "ts_code CHAR(9)",
              "trade_date": "trade_date DATE NOT NULL",
              "open": "open FLOAT(10) NOT NULL",
              "high": "high FLOAT(10) NOT NULL",
              "low": "low FLOAT(10) NOT NULL",
              "close": "close FLOAT(10) NOT NULL",
              "pre_close": "pre_close FLOAT(10) NOT NULL",
              "change": "change FLOAT(10) NOT NULL",
              "pct_change": "pct_chg FLOAT(10) NOT NULL",
              "vol": "vol FLOAT(10) NOT NULL",
              "amount": "amount FLOAT(10) NOT NULL",
              "adj_factor": "adj_factor FLOAT(10) NOT NULL",
              "open_qfq": "open_qfq FLOAT(10) NOT NULL",
              "high_qfq": "high_qfq FLOAT(10) NOT NULL",
              "low_qfq": "low_qfq FLOAT(10) NOT NULL",
              "close_qfq": "close_qfq FLOAT(10) NOT NULL",
              "symbol": "symbol CHAR(6) NOT NULL",
              "name": "name VARCHAR(20) NOT NULL",
              "area": "area VARCHAR(8)",
              "industry": "industry VARCHAR(6)",
              "market": "market VARCHAR(10)",
              "list_status": "list_status CHAR(1)",
              "list_date": "list_date DATE NOT NULL",
              "delist_date": "delist_date DATE",
              "is_hs": "is_hs CHAR(1)"}
