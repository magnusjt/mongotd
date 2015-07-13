mongotd
=======

A PHP implementation of a timeseries database with mongodb.
It takes care of storing the timeseries as well as doing
anomaly detection on them using different techniques (currently with either 3-sigma, or Holt-winters).

The time series are stored using a sid (a sensor ID, i.e. what does the data represent)
and a nid (a node ID, i.e. where the data is collected from).
Together these IDs uniquely represent a timeseries. The time series may of type gauge, or incremental.
In the latter case, contiguous values are subtracted from each other in order to get the rate of change.

In order to be efficient with storage, the data is stored in one-day chunks,
using a hash with the key being the number of seconds since midnight. This avoids
storing metadata for each and every data point. For now, the smallest time step available is 1 minute.

On retrieval of the data, aggregation can be performed both in time (sum/avg/max/min over an hour, day, etc.),
space (sum/avg/max/min over certain nodes), or by a given formula. Do note, however, that pre-aggregation/caching
is not done. This is to avoid the timezone problem (a certain day may contain different hours, depending on which time
zone the data is viewed from).

## Basic usage

First we grab a mongodb connection and initialize mongotd.
````php
$host       = 'localhost';
$db_name    = 'mongotd';
$col_prefix = 'mongotd'; // All collections used by mongotd will have this prefix and an underscore

$conn    = new \Mongotd\Connection($host, $db_name, $col_prefix);
$mongotd = new \Mongotd\Mongotd($conn);
````

### Inserting data

To insert timeseries data, we get the inserter from mongotd. We specify
the resolution to use. Any datetimes used later on will be rounded to
this resolution, and space will be allocated accordingly. The maximum
resolution is 1 hour, at which point only one data point is stored every hour.

Add the values, specifying sensor id (sid), node id (nid), datetime, value,
and a bool true if the counter is of type incremental (always increasing).

When all counter values are added, run the insert function.
The values are then batch inserted into mongodb.

````php
$inserter = $mongotd->getInserter();

$insertInterval = \Mongotd\Resolution::FIVE_MINUTES;
$inserter->setInterval($insertInterval);

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

### Retrieving data

To get the timeseries data out of the database, we use the retriever.
Desired resolution and aggregation must be specified, and also an
optional padding value which will be used if no data is found for a given datetime.
The result set is given as an associative array with the datetime string as the key.

````php
$retriever = $mongotd->getRetriever();
$from = new \DateTime('-1 day');
$to = new \DateTime('now');
$padding = 'x'; // If a value is missing at a certain step, this will be returned in its place
$values_by_date = $retriever->get($sensor_id, $node_id, $from, $to, \Mongotd\Resolution::DAY, \Mongotd\Aggregation::SUM, $padding);

foreach($values_by_date as $date_str => $value){
    // Graph the values
}
````

### Retrieving anomalies

Using the retriever, we can also get a list of the current
sensors which register as abnormal (this is calculated at the time of insertion).


````php
$from = new \DateTime('-1 day');
$to = new \DateTime('now');
$nids = array(); // Limit results to these nids, no limit if empty
$sids = array(); // Limit results to these sids, no limit if empty
$minNumberOfAnomalies = 1;
$maxResults = 20;
$results = $retriever->getAnomalies($from, $to, $nids, $sids, $minNumberOfAnomalies, $maxResults){

/*
$results is now an array of the following form:

$result[] = array(
    'nid' => $nid,
    'sid' => $sid,
    'count' => $count,
    'anomalies' => $anomalies
);

Where $anomalies is an array of objects of the class \Mongotd\Anomaly
$count is just the length of the $anomalies array
*/
````

### Using vagrant setup
Vagrantfiles have been provided for easier testing.
Just rename the dist files to vagrant_bootstrap.sh and Vagrantfile, and run vagrant up.
Everything should be installed and configured automatically.

Note: If the machine has an unclean shutdown, the mongod.lock file will often be outdated, and mongod won't startup.
To resolve, just delete /var/lib/mongo/mongod.lock