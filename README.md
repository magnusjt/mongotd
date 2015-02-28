mongotd
=======

Timeseries database for mongodb and PHP

Currently in alpha stage

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

Inserting data is done in batches. Add counter values
with sensor id (sid), node id (nid), datetime, value,
and a bool true if the counter is always increasing.

When all counter values are added, run the insert function.
The values are then batch inserted into mongodb.
````php
$insert_resolution = \Mongotd\Resolution::FIVE_MINUTES;
$inserter = $mongotd->getInserter($insert_resolution);

$datetime = new \DateTime('now');
$node_id = 1;

$sensor_id1 = 1;
$sensor_id2 = 2;
$sensor_id3 = 3;

$value1 = 50;
$value2 = 50;
$value3 = 50;

$is_incremental = false;

$inserter->add($sensor_id1, $node_id, $datetime, $value1, $is_incremental);
$inserter->add($sensor_id2, $node_id, $datetime, $value2, $is_incremental);
$inserter->add($sensor_id3, $node_id, $datetime, $value3, $is_incremental);
$inserter->insert();
````

To get the timeseries data out of the database, we use the retriever.
Desired resolution and aggregation must be specified, and also an
optional padding value which will be used if no data is found for a given datetime.
The result set is given as an associative array with the datetime string as the key.
````php
$retriever = $mongotd->getRetriever();
$values_by_date = $retriever->get($sensor_id, $node_id, new DateTime('-1 day'), new DateTime('now'), \Mongotd\Resolution::DAY, \Mongotd\Aggregation::SUM);

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
            'Sensor ID: ' . $abnormal['sid'] .
            ', Node ID: ' . $abnormal['nid'] .
            ', Actual value: ' .
            $abnormal['val'] .
            ', Predicted value: ' .
            $abnormal['pred'] .
            "\n";
}
````

### Using vagrant setup
Vagrantfiles have been provided for easier testing.
Just rename the dist files to vagrant_bootstrap.sh and Vagrantfile, and run vagrant up.
Everything should be installed and configured automatically.

Note: If the machine has an unclean shutdown, the mongod.lock file will often be outdated, and mongod won't startup.
To resolve, just delete /var/lib/mongo/mongod.lock