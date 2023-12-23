select
  sum(total_visitor)
from
  sample_st_daily_visitor
GROUP BY
  {{store_name}}