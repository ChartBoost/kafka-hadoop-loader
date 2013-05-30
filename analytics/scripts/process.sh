dt=$1
ydt=$2
hr=$3
dt_str=$4

cat <<EOF | hive

add jar /home/cb/runtime/bi_2.9.2-0.1.10.jar;
CREATE TEMPORARY FUNCTION group_first AS 'io.caffeine.hadoop.collect.FirstOfGroupUDAF'; 

set hive.exec.parallel=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

use store_analytics;

ALTER TABLE iap_json ADD IF NOT EXISTS PARTITION (dt = '$dt', hr = '$hr') location '$dt/$hr';

ALTER TABLE purchase_json ADD IF NOT EXISTS PARTITION (dt = '$dt', hr = '$hr') location '$dt/$hr';

ALTER TABLE transact_json ADD IF NOT EXISTS PARTITION (dt = '$dt', hr = '$hr') location '$dt/$hr';

INSERT OVERWRITE TABLE purchase_data PARTITION (dt = '$dt', hr = '$hr')
SELECT 
  time, store, item, currency, amount, player, purchase
FROM (
  SELECT 
   CAST(time AS bigint) as time, store, item, 
   CASE WHEN purchase = 'iap' THEN reward_currency ELSE currency END AS currency, 
   CAST(amount AS double) AS amount, player, purchase
  FROM (
    SELECT json FROM purchase_json WHERE dt = '$dt' AND hr = '$hr'
  ) a
  LATERAL VIEW json_tuple(json, 'time', 'store', 'item', 'player') v AS time, sobj, iobj, player
  LATERAL VIEW json_tuple(sobj, '\$oid') p AS store
  LATERAL VIEW json_tuple(iobj, 'id', 'amount', 'currency', 'reward_currency', 'purchase') i AS item, amount, currency, reward_currency, purchase 
) b
GROUP BY time, store, item, currency, amount, player, purchase;

INSERT OVERWRITE TABLE iap_data PARTITION (dt = '$dt', hr = '$hr')
SELECT 
  b.time, b.store, b.item, '' AS currency, b.tier, c.amount, b.player
FROM (
  SELECT 
    time, store, item, tier, player
  FROM (
    SELECT json FROM iap_json WHERE dt = '$dt' AND hr = '$hr'
  ) a
  LATERAL VIEW json_tuple(json, 'time', 'store', 'item', 'iap', 'player') v AS time, sobj, iobj, aobj, player
  LATERAL VIEW json_tuple(sobj, '\$oid') p AS store
  LATERAL VIEW json_tuple(iobj, 'id') i AS item
  LATERAL VIEW json_tuple(aobj, 'tier') t AS tier
  GROUP BY time, store, item, tier, player
) b
LEFT OUTER JOIN 
(SELECT * FROM tiers_info) c
ON
b.tier = c.tier;

DROP VIEW IF EXISTS units_sold_view;

CREATE VIEW units_sold_view AS
SELECT 
  store, item, '' AS currency, '9' AS metric, CAST(count(amount) AS double) AS value
FROM 
  purchase_data WHERE dt = '$dt' AND hr = '$hr' AND amount IS NOT NULL
GROUP BY store, item;


DROP VIEW IF EXISTS igc_spent_view;

CREATE VIEW igc_spent_view AS
SELECT 
  store, item, currency, '10' AS metric, sum(amount) AS value
FROM 
  purchase_data WHERE dt = '$dt' AND hr = '$hr' AND amount IS NOT NULL
GROUP BY store, item, currency;


DROP VIEW IF EXISTS payer_count_item_view;

CREATE VIEW payer_count_item_view AS
SELECT 
  store, item, '' AS currency, '8' AS metric, CAST(count(value) AS double) AS value
FROM (
  SELECT store, item, currency, player AS value FROM purchase_data WHERE dt = '$dt' and hr <= '$hr'
  GROUP BY store, item, currency, player
) a
GROUP BY store, item;


DROP VIEW IF EXISTS payer_count_store_view;

CREATE VIEW payer_count_store_view AS
SELECT 
  store, '8' AS metric, CAST(count(value) AS double) AS value
FROM (
  SELECT store, item, player AS value FROM iap_data WHERE dt = '$dt' and hr <= '$hr'
  GROUP BY store, item, player
) a
GROUP BY store;


DROP VIEW IF EXISTS iap_revenues_view;

CREATE VIEW iap_revenues_view AS
SELECT 
  store, item, currency, '1' AS metric, round(sum(amount), 2) AS value
FROM 
  iap_data WHERE dt = '$dt' AND hr = '$hr'
GROUP BY store, item, currency;

ALTER TABLE item_agg_hourly ADD IF NOT EXISTS PARTITION (dt = '$dt', hr = '$hr') location '$dt/$hr';

INSERT OVERWRITE TABLE item_agg_hourly PARTITION (dt = '$dt', hr = '$hr')
SELECT * FROM (
  SELECT * FROM units_sold_view
  UNION ALL
  SELECT * FROM igc_spent_view
  UNION ALL
  SELECT * FROM iap_revenues_view
) a
;

DROP VIEW IF EXISTS hook_purchase_item_view;

CREATE VIEW hook_purchase_item_view AS
SELECT 
  d.store, d.item, '' AS currency, '11' AS metric, CAST(count(d.item) AS double) AS value
FROM (
  SELECT
    c.store, c.player, c.currency, group_first(c.item) AS item, min(c.diff) AS diff
  FROM
  (
    SELECT a.time, a.store, a.player, b.item, a.currency, b.time - a.time AS diff
    FROM
    (SELECT * from purchase_data where dt >= '$ydt' AND dt <= '$dt' AND purchase = 'iap' AND currency IS NOT NULL) a
    JOIN 
    (SELECT * from purchase_data where dt >= '$ydt' AND dt <= '$dt' AND purchase = 'currency' AND currency IS NOT NULL) b
    ON
    a.store = b.store and a.player = b.player and a.currency = b.currency
  ) c
  WHERE diff > 0
  GROUP BY c.time, c.store, c.player, c.currency
) d
GROUP BY d.store, d.item
;

DROP VIEW IF EXISTS hook_purchase_store_view;

CREATE VIEW hook_purchase_store_view AS
SELECT 
  d.store, '11' AS metric, CAST(count(d.item) AS double) AS value
FROM (
  SELECT
    c.store, c.player, c.currency, group_first(c.item) AS item, min(c.diff) AS diff
  FROM
  (
    SELECT a.time, a.store, a.player, b.item, a.currency, b.time - a.time AS diff
    FROM
    (SELECT * from purchase_data where dt >= '$ydt' AND dt <= '$dt' AND purchase = 'iap' AND currency IS NOT NULL) a
    JOIN 
    (SELECT * from purchase_data where dt >= '$ydt' AND dt <= '$dt' AND purchase = 'currency' AND currency IS NOT NULL) b
    ON
    a.store = b.store and a.player = b.player and a.currency = b.currency
  ) c
  WHERE diff > 0
  GROUP BY c.time, c.store, c.player, c.currency
) d
GROUP BY d.store
;


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


DROP TABLE IF EXISTS item_agg_daily;

CREATE TABLE item_agg_daily 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/analytics/aggregate/item/daily/'
AS
SELECT * FROM (
  SELECT 
    '$dt_str' AS date_string, store, item, currency, metric, SUM(value) AS value, unix_timestamp('$dt_str $hr:00:00') AS ts
  FROM 
    item_agg_hourly WHERE dt = '$dt' and hr <= '$hr'
  GROUP BY 
  store, item, currency, metric
  UNION ALL
  SELECT 
    '$dt_str' AS date_string, store, item, currency, metric, value, unix_timestamp('$dt_str $hr:00:00') AS ts
  FROM 
    payer_count_item_view
  UNION ALL
  SELECT 
    '$dt_str' AS date_string, store, item, currency, metric, value, unix_timestamp('$dt_str $hr:00:00') AS ts
  FROM
    hook_purchase_item_view
) a;

DROP TABLE IF EXISTS store_agg_daily;

CREATE TABLE store_agg_daily 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/analytics/aggregate/store/daily/'
AS
SELECT * FROM (
  SELECT 
    '$dt_str' AS date_string, store, metric, SUM(value) AS value, unix_timestamp('$dt_str $hr:00:00') AS ts
  FROM 
    store_agg_hourly 
  WHERE 
    dt = '$dt' and hr <= '$hr'
  GROUP BY 
    store, metric
  UNION ALL
  SELECT 
    '$dt_str' AS date_string, store, metric, value, unix_timestamp('$dt_str $hr:00:00') AS ts
  FROM 
    payer_count_store_view  
    UNION ALL
  SELECT 
    '$dt_str' AS date_string, store, metric, value, unix_timestamp('$dt_str $hr:00:00') AS ts
  FROM
    hook_purchase_store_view
) a;

DROP TABLE IF EXISTS version_tracking;

CREATE TABLE version_tracking 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/data/analytics/aggregate/version/tracking/'
AS
SELECT * FROM (
  SELECT 
    '$dt_str' AS date_string, 'daily_item_aggr', unix_timestamp('$dt_str $hr:00:00') AS ts
  FROM 
    item_agg_daily 
  LIMIT 1
  UNION ALL
  SELECT 
    '$dt_str' AS date_string, 'daily_store_aggr', unix_timestamp('$dt_str $hr:00:00') AS ts
  FROM 
    store_agg_daily  
  LIMIT 1
) a;

quit;

EOF