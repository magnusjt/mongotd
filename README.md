mongotd
=======

A PHP implementation of a timeseries database with mongodb.
It takes care of storing the timeseries as well as doing
anomaly detection on them using different techniques (currently with either 3-sigma, or Holt-winters).

The time series are stored using a sid (a sensor ID, i.e. what does the data represent)
and a nid (a node ID, i.e. where the data is collected from).
Together these IDs uniquely represent a timeseries. The time series may of type gauge, or incremental.
In the latter case, contiguous values are subtracted from each other in order to get the rate of change.

* Examples of gauge counters are temperature, current number of visitors, etc.
* Examples of incremental counters are cumulative number of visitors, cumulative number of requests, etc.

In order to be efficient with storage, the data is stored in one-day chunks,
using a hash with the key being the number of seconds since midnight. This avoids
storing metadata for each and every data point. For now, the smallest time step available is 1 minute.

On retrieval of the data, aggregation can be performed both in time (sum/avg/max/min over an hour, day, etc.),
space (sum/avg/max/min over certain nodes), or by a given formula. Do note, however, that pre-aggregation
is not done. This is to avoid the timezone problem (i.e. a certain day may contain different hours, depending on which time
zone the data is viewed in).

## Basic usage

First we grab a mongodb connection and initialize mongotd.
````php
$host      = 'localhost';
$dbName    = 'mongotd';
$colPrefix = 'mongotd'; // All collections used by mongotd will have this prefix and an underscore

$conn    = new \Mongotd\Connection($host, $dbName, $colPrefix);
$mongotd = new \Mongotd\Mongotd($conn);
````

### About sensor ids and node ids

Sensor ids and node ids are stored as strings in the database.
Therefore, any supplied ids must be castable to string.

### About inserted values

Inserted values are always either integer or doubles, and must be castable to those data types.

### Inserting data

To insert timeseries data, we get the inserter from mongotd. We specify
the resolution to use. Any datetimes used later on will be rounded to
this resolution, and space will be allocated accordingly. For instance,
a resolution of Resolution::FIFTEEN_MINUTES will round all timestamps down to the nearest quarter,
and the mongodb document for this day will be filled with dummy values every 15 minutes of the day.

The maximum insertion resolution is 1 hour, at which point only one data point is stored every hour.

Add the values, specifying sensor id (sid), node id (nid), datetime, value,
and a bool true if the counter is of type incremental (always increasing).

When all counter values are added, run the insert function.
The values are then batch inserted into mongodb.

````php
$inserter = $mongotd->getInserter();

$insertResolution = \Mongotd\Resolution::FIFTEEN_MINUTES;
$inserter->setInterval($insertResolution);

$datetime = new \DateTime('now');
$nodeId = '1';

$sensorId1 = '1';
$sensorId2 = '2';
$sensorId3 = '3';

$value1 = 50;
$value2 = 50;
$value3 = 50;

$isIncremental = false;

$inserter->add($sensorId1, $nodeId, $datetime, $value1, $isIncremental);
$inserter->add($sensorId2, $nodeId, $datetime, $value2, $isIncremental);
$inserter->add($sensorId3, $nodeId, $datetime, $value3, $isIncremental);
$inserter->insert();
````

### Retrieving data

To get the timeseries data out of the database, we use the retriever.
Desired resolution and aggregation must be specified, and also an
optional padding value which will be used if no data is found for a given datetime.
The result set is given as an associative array with the timestamp as the key.

Please note that aggregation is done within the timezone of the given $start and $end DateTime's.
This has implications on the timestamp values you get back, as they will align
to the specified resolution, but only if converted back into the same timezone.

E.g. If you set resolution to Resolution::DAY, you will get timestamps that,
when converted to your timezone, will correspond to 00:00:00 o'clock for that day. But the timestamp
itself is in UTC, and will therefore not correspond exactly to 00:00:00 o'clock in the UTC timezone.

````php
$retriever = $mongotd->getRetriever();
$start = new DateTime('-1 day');
$end = new DateTime('now');
$padding = false;

$valsByTimestamp = $retriever->get(
    $sensorId, // Sensor ID as string
    $nodeId,   // Node ID as string
    $start,    // Start date as DateTime
    $end,      // End date as DateTime
    Resolution::DAY,  // Desired end resolution
    Aggregation::SUM, // How the values are aggregated up to the end resolution
    $padding // In case there is no values at a given step, this value will be put in its place. (default: false)
);

foreach($valsByTimestamp as $timestamp => $value){
    // Graph the values
}

````

### Converting timestamps into date strings

Sometimes it's handy to have the retrieved results by a time string key instead of a timestamp.
For this we can use the following method on the retriever:

````
$valsByDateStr = $retriever->convertToDateStringKeys($valsByTimestamp, $from->getTimezone()){
````

### Retrieving data with aggregated nodes

Sometimes you'll want to aggregate your data over a set of nodes.
Say you're measuring the number of requests on three nodes,
and you'd like to know the total number of requests on all these nodes over time.
To do this, give a list of node ids as input to the retriever instead of a single value.

In addition, you must now specify at what resolution level the nodes should be
aggregated together, along with the method of aggregation (sum, avg, max, min).

The reason we need to aggregate the nodes at a different level than
the result, is that the results differs depending on what type of aggregation we want
at what level. For instance, you may want the hourly sum of nodes displayed as
a daily average. In this case, we'll first sum each node up to their hourly values.
Next, we'll take the sum over all the nodes. And finally we'll calculate
the average values for each day.

````
$retriever->get(
    $sensor_id,
    array('1', '2', '3'),
    $start,
    $end,
    Resolution::DAY,  // End resolution
    Aggregation::AVG, // Aggregation at the end resolution
    $padding,
    Resolution::HOUR, // Resolution where nodes are aggregated
    Aggregation::SUM, // How the nodes are aggregated up to the node resolution
    Aggregation::SUM  // How the nodes are aggregated together
);
````

### Retrieving data based on a formula

So far we've seen how to retrieve data for a single sensor and one or more nodes.
By using a formula, we have the ability to aggregate together multiple sensors as well.

A formula looks like a regular arithmetic expression, but contains variables as well.
The variables are of the following form:

````
[sid=SID,agg=AGG]
````

Here SID is a sensor id, and agg is the type of aggregation (sum=1,avg=2,max=3,min=4).

Below is an example formula:

````
([sid=1,agg=1] / [sid=2,agg=1]) * 100
````

In this formula, we divide a variable by another and multiply by 100. When retrieving
the formula, the variables are actually substituted by arrays, and every element
in the array is run through the formula. The arrays contain the values aggregated
using the aggregation specified inside the variable.

As before, we use the retriever to retrieve a formula. There is two new parameters
to be aware of now: A boolean telling whether we'll be using a formula or not,
and another resolution that is used when aggregating the variable arrays.

In the example formula above, each variable will be summed up to the given
formula resolution before the formula itself is calculated.

A full example is shown below:

````
$formula = '([sid=1,agg=1] / [sid=2,agg=1]) * 100';
$retriever->get(
    $formula,
    $nodeId,
    $start,
    $end,
    Resolution::DAY,  // End resolution
    Aggregation::SUM, // Aggregation at the end resolution
    $padding,
    null, // Only one node give, don't need this
    null, // Only one node give, don't need this
    null, // Only one node give, don't need this
    true, // Set to true if the sensor id is to be evaluated as a formula
    Resolution::FIVE_MINUTES // The resolution at which the formula is evaluated
);
````

### Finding anomalous data

Anomalies are found during the insertion of new data. To achieve this,
we need to decide on an anomaly scanner to use.

Example using the 3-sigma method:

````
$scanner = $mongotd->getAnomalyScanner3Sigma();
$scanner->setDaysToScan(20); // How many days in the past to use for comparison

// Instead of looking at individual values,
// look at the average value within 300 seconds
$scanner->setWindowLength(300);

$scanner->setMinPrevDataPoints(14); // How many data points are required before we bother doing the scan
$scanner->setMinCurrDataPoints(1); // How many data points we need for evaluation

// The treshold for 3 sigma.
// If the data is normally distributed,
// a value of 3 gives anomalies if the value is outside of the 99.7 percentile
$scanner->setScoreTreshold(3);

$inserter->setAnomalyScanner($scanner);
````

The 3-sigma method takes the average and standard deviation of all values that
fall on the same time of day as the current time. It compares this average
to the current value, using the 3*STD as the allowed deviation from the average.

The downside to the 3-sigma method is that it assumes the values are normally distributed
(which is not always the case). In this case, it also assumes that values follow a
daily profile where values are likely to be the same at the same time of the day.
And finally, the method needs to go far back in time and retrieve values, which
may be slow.

Another method for finding anomalies is the Holt-Winters algorithm. It has
the same downside as 3-sigma, in that the data should be normally distributed and follow
a time profile. However, it uses exponential smoothing instead of exact calculation
of average and std. Therefore, it should be much faster.

Example using Holt-Winters:

````
$scanner = $mongotd->getAnomalyScannerHw();
$scanner->setMinDaysScanned(20);
$scanner->setHwAlpha(0.05); // Smoothing factor for average
$scanner->setHwBeta(0.005); // Smoothing factor for linear increase
$scanner->setHwGamma(0.1);  // Smoothing factor std
$scanner->setScoreTreshold(3); // Same as 3-sigma
$scanner->setSeasonLength(1440); // Season length in minutes (daily season by default)
$inserter->setAnomalyScanner($scanner);
````

### Retrieving anomalies

Using the retriever, we can also get a list of the current
sensors which register as abnormal (this is calculated at the time of insertion).

````php
$from = new DateTime('-1 day');
$to = new DateTime('now');
$nids = array(); // Limit results to these nids, no limit if empty
$sids = array(); // Limit results to these sids, no limit if empty
$minNumberOfAnomalies = 1;
$maxResults = 20;
$results = $retriever->getAnomalies($from, $to, $nids, $sids, $minNumberOfAnomalies, $maxResults);

/*
$results is now an array of the following form:

$result[] = array(
    'nid' => $nid,
    'sid' => $sid,
    'count' => $count,
    'anomalies' => $anomalies
);

Where $anomalies is an array of objects of the class \Mongotd\Anomaly
$count is just the length of the $anomalies array.
*/
````

In addition, there is a helper method for converting an array of anomalies into
an array holding the state (1=anomaly, 0=no anomaly) at each timestamp:

````
$res = $retriever->getAnomalies($from, $to, $nids, $sids, $minNumberOfAnomalies, $maxResults);
$anomalies = array();
foreach($res as $row){
    $anomalies = array_merge($anomalies, $row['anomalies']);
}

$anomalyStateByTimestamp = $retriever->getAnomalyStates($anomalies, $from, $to, $resolution);

````

### Using vagrant setup
Vagrantfiles have been provided for easier testing.
Just rename a dist Vagrantfile to Vagrantfile, and run vagrant up.
Everything should be installed and configured automatically.

Note: If the machine has an unclean shutdown, the mongod.lock file will often be outdated, and mongod won't startup.
To resolve, just delete /var/lib/mongo/mongod.lock