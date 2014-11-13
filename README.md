mongotd
=======

Timeseries database for mongodb and PHP

Currently in alpha stage

### TODO
* Aggregation collections (cached rollup)
* Sids (sensor id) that can be something other than a primitive type?

### Basic usage

First we grab a mongodb connection and initialize mongotd.
````php
$host       = 'localhost';
$db_name    = 'mongotd';
$col_prefix = 'mongotd';

$conn    = new \Mongotd\Connection($host, $db_name, $col_prefix);
$mongotd = new \Mongotd\Mongotd($conn, null);
````

To insert timeseries data, we get the inserter from mongotd. We specify
the resolution to use. Any datetimes used later on will be rounded to
this resolution, and space will be allocated accordingly. The maximum
resolution is 1 hour, at which point only one data point is stored every hour.

When inserting data, we first specify the datetime to use for all points we
are currently inserting. Next we add a bunch of values with given sensor ids,
and an indication on whether the sensor is incremental (constantly increasing).
If the latter, the values are delta-calculated before insertion.

When we have added all data points, we run the execute function.
The points are then batch inserted into mongodb.
````php
$insert_resolution = \Mongotd\Resolution::FIVE_MINUTES;
$inserter = $mongotd->getInserter($insert_resolution);

$inserter->setDateTime(new \DateTime('now'));

$sensor_id1 = 1;
$sensor_id2 = 2;
$sensor_id3 = 3;

$value1 = 50;
$value2 = 50;
$value3 = 50;

$is_incremental = false;

$inserter->add($sensor_id1, $value1, $is_incremental);
$inserter->add($sensor_id2, $value2, $is_incremental);
$inserter->add($sensor_id3, $value3, $is_incremental);
$inserter->execute();
````

To get the timeseries data out of the database, we use the retriever.
Desired resolution and aggregation must be specified, and also an
optional padding value which will be used if no data is found for a given datetime.
The result set is given as an associative array with the datetime string as the key.
````php
$retriever = $mongotd->getRetriever();
$values_by_date = $retriever->get($sensor_id, new DateTime('-1 day'), new DateTime('now'), \Mongotd\Resolution::DAY, \Mongotd\Aggregation::SUM);

foreach($values_by_date as $date_str => $value){
    // Graph the values
}
````

Using the retriever, we can also get a list of the current
sensors which register as abnormal. Threshold is the number
of abnormal values in a row which must be present in order
for the sensor to register as abnormal.

The detection is implemented using the holt-winters algorithm.
````php
$threshold = 3;
$abnormals = $retriever->getCurrentAbnormal($threshold);
foreach($abnormals as $abnormal){
    echo 'Abnormal value found: ' .
            $abnormal['sid'] .
            ', Actual value: ' .
            $abnormal['val'] .
            ', Predicted value: ' .
            $abnormal['pred'] .
            "\n";
}
````

