from dags.lib.op_data_fetcher import fetch_data_from_op
from dags.lib.gv_data_fetcher import fetch_data_from_gv
from dags.lib.raw_to_fmt_gv import convert_raw_to_formatted_gv
from dags.lib.raw_to_fmt_op import convert_raw_to_formatted_op
from dags.lib.combine_data import combine_data
from dags.lib.index import indeex
from datetime import datetime, timedelta, date

current_day = date.today().strftime("%Y%m%d")

# fetch_data_from_op()
# fetch_data_from_gv()
#
# convert_raw_to_formatted_gv('data.csv',current_day)
# convert_raw_to_formatted_op('op.json',current_day)

# combine_data(current_day)
#
indeex('20220606')

