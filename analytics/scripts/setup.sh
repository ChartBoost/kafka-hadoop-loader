cat <<EOF | hive

set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

use store_analytics;

create external table if not exists tiers_info (
  tier string,
  amount double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/analytics/info/tiers/';

create external table if not exists iap_json (
  json string
)
PARTITIONED BY (dt string, hr string)
location '/data/analytics/store-iap/';

create external table if not exists purchase_json (
  json string
)
PARTITIONED BY (dt string, hr string)
location '/data/analytics/store-purchase/';

create external table if not exists transact_json (
  json string
)
PARTITIONED BY (dt string, hr string)
location '/data/analytics/store-transact/';

create external table if not exists purchase_data (
  time bigint,
  store string,
  item string,
  currency string,
  amount double,
  player string,
  purchase string
)
PARTITIONED BY (dt string, hr string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/analytics/parsed/purchase/';

create external table if not exists iap_data (
  time string,
  store string,
  item string,
  currency string,
  tier string,
  amount float,
  player string
)
PARTITIONED BY (dt string, hr string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/analytics/parsed/iap/';

create external table if not exists item_agg_hourly (
  store string,
  item string,
  currency string,
  metric string,
  value double
)
PARTITIONED BY (dt string, hr string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/analytics/aggregate/item/hourly/';


create external table if not exists store_agg_hourly (
store string,
currency string,
metric string,
value double
)
PARTITIONED BY (dt string, hr string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/analytics/aggregate/store/hourly/';

quit;

EOF

