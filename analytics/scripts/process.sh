dt=$1
hr=$2
dt_str=$3

./load_topic.sh store-purchase
./load_topic.sh store-transact
./load_topic.sh store-iap

cat <<EOF | hive

set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

add jar /home/cb/keith/brickhouse-0.3.1.jar;

use store;

create external table if not exists iap_json (
  json string
)
PARTITIONED BY (dt string, hr string)
location '/data/analytics/store-iap/';

ALTER TABLE iap_json ADD IF NOT EXISTS PARTITION (dt = '$dt', hr = '$hr') location '$dt/$hr';

create external table if not exists purchase_json (
  json string
)
PARTITIONED BY (dt string, hr string)
location '/data/analytics/store-purchase/';

ALTER TABLE purchase_json ADD IF NOT EXISTS PARTITION (dt = '$dt', hr = '$hr') location '$dt/$hr';

create external table if not exists transact_json (
  json string
)
PARTITIONED BY (dt string, hr string)
location '/data/analytics/store-transact/';

ALTER TABLE transact_json ADD IF NOT EXISTS PARTITION (dt = '$dt', hr = '$hr') location '$dt/$hr';


create external table if not exists purchase_data (
  time string,
  store string,
  tag string,
  item string,
  currency string,
  amount double,
  player string
)
PARTITIONED BY (dt string, hr string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/analytics/parsed/purchase/';

INSERT OVERWRITE TABLE purchase_data PARTITION (dt = '$dt', hr = '$hr')
SELECT 
  time, store, tag, item, currency, amount, player
FROM (
  SELECT 
   time, store, tag, item, currency, CAST(amount AS double) AS amount, player
  FROM (
    SELECT json FROM purchase_json WHERE dt = '$dt' AND hr = '$hr'
  ) a
  LATERAL VIEW json_tuple(json, 'time', 'store', 'item', 'player', 'tag') v AS time, sobj, iobj, player, tag
  LATERAL VIEW json_tuple(sobj, '\$oid') p AS store
  LATERAL VIEW json_tuple(iobj, 'id', 'amount', 'currency') i AS item, amount, currency
) b
GROUP BY time, store, tag, item, currency, amount, player;


DROP VIEW IF EXISTS units_sold_view;

CREATE VIEW units_sold_view AS
SELECT 
  store, tag, item, currency, '9' AS metric, CAST(count(amount) AS double) AS value
FROM 
  purchase_data WHERE dt = '$dt' AND hr = '$hr'
GROUP BY store, tag, item, currency;


DROP VIEW IF EXISTS igc_spent_view;

CREATE VIEW igc_spent_view AS
SELECT 
  store, tag, item, currency, '10' AS metric, sum(amount) AS value
FROM 
  purchase_data WHERE dt = '$dt' AND hr = '$hr'
GROUP BY store, tag, item, currency;


DROP VIEW IF EXISTS payer_count_item_view;

CREATE VIEW payer_count_item_view AS
SELECT 
  store, tag, item, currency, '8' AS metric, CAST(count(value) AS double) AS value
FROM (
  SELECT store, tag, item, currency, player AS value FROM purchase_data WHERE dt = '$dt'
  GROUP BY store, tag, item, currency, player
) a
GROUP BY store, tag, item, currency;


DROP VIEW IF EXISTS payer_count_store_view;

CREATE VIEW payer_count_store_view AS
SELECT 
  store, '8' AS metric, CAST(count(value) AS double) AS value
FROM (
  SELECT store, tag, item, currency, player AS value FROM purchase_data WHERE dt = '$dt'
  GROUP BY store, tag, item, currency, player
) a
GROUP BY store;


create external table if not exists item_agg_hourly (
  store string,
  tag string,
  item string,
  currency string,
  metric string,
  value double
)
PARTITIONED BY (dt string, hr string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/analytics/aggregate/item/hourly/';

ALTER TABLE item_agg_hourly ADD IF NOT EXISTS PARTITION (dt = '$dt', hr = '$hr') location '$dt/$hr';

INSERT OVERWRITE TABLE item_agg_hourly PARTITION (dt = '$dt', hr = '$hr')
SELECT * FROM (
  SELECT * FROM units_sold_view
  UNION ALL
  SELECT * FROM igc_spent_view
) a
;


create external table if not exists item_agg_daily (
  date_string string,
  store string,
  tag string,
  item string,
  currency string,
  metric string,
  value double,
  ts bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/analytics/aggregate/item/daily/';

INSERT OVERWRITE TABLE item_agg_daily
SELECT * FROM (
  SELECT 
    '$dt_str' AS date_string, store, tag, item, currency, metric, SUM(value) AS value, unix_timestamp('$dt_str $hr:00:00') AS ts
  FROM 
    item_agg_hourly WHERE dt = '$dt'
  GROUP BY 
  store, tag, item, currency, metric
  UNION ALL
  SELECT 
    '$dt_str' AS date_string, store, tag, item, currency, metric, value, unix_timestamp('$dt_str $hr:00:00') AS ts
  FROM 
    payer_count_item_view
) a;


create external table if not exists store_agg_hourly (
store string,
currency string,
metric string,
value double
)
PARTITIONED BY (dt string, hr string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/analytics/aggregate/store/hourly/';

ALTER TABLE store_agg_hourly ADD IF NOT EXISTS PARTITION (dt = '$dt', hr = '$hr') location '$dt/$hr';

INSERT OVERWRITE TABLE store_agg_hourly PARTITION (dt = '$dt', hr = '$hr')
SELECT 
  store, currency, metric, SUM(value) AS value 
FROM  
  item_agg_hourly 
WHERE 
  dt = '$dt' and hr = '$hr'
GROUP BY 
  store, currency, metric;


create external table if not exists store_agg_daily (
date_string string,
store string,
metric string,
value double,
ts bigint
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/analytics/aggregate/store/daily/';

INSERT OVERWRITE TABLE store_agg_daily
SELECT * FROM (
  SELECT 
    '$dt_str' AS date_string, store, metric, SUM(value) AS value, unix_timestamp('$dt_str $hr:00:00') AS ts
  FROM 
    store_agg_hourly 
  WHERE 
    dt = '$dt'
  GROUP BY 
    store, metric
  UNION ALL
  SELECT 
    '$dt_str' AS date_string, store, metric, value, unix_timestamp('$dt_str $hr:00:00') AS ts
  FROM 
    payer_count_store_view  
) a;


quit;

EOF

sqoop export --connect jdbc:mysql://percona1j.caffeine.io/store_aggr --username root --password -W?@A]smXI --export-dir /data/analytics/aggregate/item/daily --table daily_item_aggr --staging-table daily_item_aggr_staging --clear-staging-table --input-fields-terminated-by '\t' --input-null-string '\\N' --input-null-non-string '\\N' 

sqoop export --connect jdbc:mysql://percona1j.caffeine.io/store_aggr --username root --password -W?@A]smXI --export-dir /data/analytics/aggregate/store/daily --table daily_store_aggr --staging-table daily_store_aggr_staging --clear-staging-table --input-fields-terminated-by '\t' --input-null-string '\\N' --input-null-non-string '\\N'

