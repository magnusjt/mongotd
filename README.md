mongotd
=======

A PHP implementation of a timeseries database with mongodb.
It takes care of storing the timeseries as well as doing
anomaly detection on them using different techniques (currently with either 3-sigma, Holt-winters, or Kolmogorrow-Smirnov).

The time series are stored using a sid (a sensor ID, i.e. what does the data represent)
and a nid (a node ID, i.e. where the data is collected from).
Together these IDs uniquely represent a timeseries. The time series may of type gauge, or incremental.
In the latter case, contiguous values are subtracted from each other in order to get the rate of change.

* Examples of gauge counters are temperature, current number of visitors, etc.
* Examples of incremental counters are cumulative number of visitors, cumulative number of requests, etc.

In order to be efficient with storage, the data is stored in one-day chunks,
using a hash with the key being the number of seconds since midnight. This avoids
storing metadata for each and every data point.

On retrieval of the data, aggregation can be performed both in time (sum/avg/max/min over an hour, day, etc.),
space (sum/avg/max/min over certain nodes), or by a given formula. Do note, however, that pre-aggregation
is not done. This is to avoid the timezone problem (i.e. a certain day may contain different hours, depending on which time
zone the data is viewed in).

## Installation

Install with composer:

````
composer require magnusjt/mongotd
````

## Basic usage

First we grab a mongodb connection and initialize mongotd.
````php
$host      = '127.0.0.1'; // Or mongodb connection string
$dbName    = 'mongotd';
$colPrefix = 'mongotd'; // All collections used by mongotd will have this prefix and an underscore

$conn = new \Mongotd\Connection($host, $dbName, $colPrefix);
````

### About sensor ids and node ids

Sensor ids and node ids are stored as strings in the database.
Therefore, any supplied ids must be castable to string.

### About inserted values

Inserted values are always either integer or doubles, and must be castable to those data types.

### Inserting data

Inserting data is done by using a set of storage middleware.
You can pick and choose what middleware you want, or even create your own.
In the extreme case, it is actually possible to replace mongodb with another database.

For example:
````
$storage = new Storage();
$storage->addMiddleware(new FilterCounterValues());
$storage->addMiddleware(new CalculateDeltas($conn, new Logger(null), Resolution::FIVE_MINUTES));
$storage->addMiddleware(new InsertCounterValues($conn));
$storage->addMiddleware(new FindAnomaliesUsingSigmaTest($conn));
$storage->addMiddleware(new StoreAnomalies($conn));
````

Storing time series data is done via an array of CounterValue:

````
$storage->store([
    new CounterValue($sid, $nid, $datetime, $value, $isIncremental)
]);
````

### Retrieving data

Because there are so many ways of massaging time series data,
we use a pipeline solution for processing the raw values.
The pipeline can be assembled in many different ways,
and it is possible to add new or modified processing steps
according to your needs.

Some examples of what you can do with the default pipeline:

#### Roll up to daily values by sum, average, max, min:

````
$start = new DateTime('2015-11-01 00:00:00');
$end = new DateTime('2015-12-01 00:00:00');
DateTimeHelper::normalizeTimeRange($start, $end, Resolution::DAY);

$sequence = [
    new Find($conn, $sid, $nid, $start, $end),
    new RollupTime(Resolution::DAY, Aggregation::Sum),
    new Pad(Resolution::DAY, $start, $end)
];

$pipeline = new Pipeline();
$series = $pipeline->run($sequence);
````

#### Combine different series using sum, average, max, min:

````
$sequence = [
    [
        [
            new Find($conn, $sid, $nid1, $start, $end),
            new RollupTime(Resolution::FIVE_MINUTES, Aggregation::SUM),
            new Pad(Resolution::FIVE_MINUTES, $start, $end)
        ],
        [
            new Find($conn, $sid, $nid2, $start, $end),
            new RollupTime(Resolution::FIVE_MINUTES, Aggregation::SUM),
            new Pad(Resolution::FIVE_MINUTES, $start, $end)
        ]
    ],
    new RollupSpace(Aggregation::SUM),
    new RollupTime(Resolution::DAY, Aggregation::SUM),
    new Pad(Resolution::DAY, $start, $end)
];
$pipeline = new Pipeline();
$series = $pipeline->run($sequence);
````

In this example you can see that we first roll up each series, then combine them into one series,
and finally roll that result up to the daily series.

In this case we just sum everything, so the order of operations isn't that big a deal.
However, if you're using aggregations such as average/max/min, the order does matter.
By using the pipeline, you have full control of the order of operations.

E.g.:

#### Combine different series using a formula

Example:
````
$formula = '([sid=1,nid=1,agg=1] / [sid=2,nid=1,agg=1]) * 100';

$parser = new Parser();
$ast = $parser->parse($formula);

$astEvaluator = new AstEvaluator();
$astEvaluator->setVariableEvaluatorCallback(function($options) use($conn, $start, $end)){
    $pipeline = new Pipeline();
    return $pipeline->run([
        new Find($conn, $options['sid'], $options['nid'], $start, $end),
        new RollupTime(Resolution::FIVE_MINUTES, $options['agg']),
        new Pad(Resolution::FIVE_MINUTES, $start, $end)
    ])->vals;
]);

$sequence = [
    new Formula($ast, $astEvaluator),
    new RollupTime(Resolution::DAY, Aggregation::SUM),
    new Pad(Resolution::DAY, $start, $end)
];
$pipeline = new Pipeline();
$series = $pipeline->run($sequence);
````

#### Pipeline factory

Some default pipeline sequences can be created using the Mongotd\Pipeline\Factory class.
Of course, it's possible to create custom factories as well.

#### What about timezones?

Timezones are a huge pain when dealing with time series. It's not enough to
figure out the timezone offset for the time series, since daylight savings time
may cause the offset to change during the duration of the series.
It is possible to use some datetime library to get rid of some of this pain,
but the problem then becomes performance. So in this project we use
timestamps for as many things as possible, and only take timezones into
account when we absolutely have to. The most common place where we need to
think about timezones is when rolling up in time. Here we offset each
timestamp in the calculation such that when the result is converted into
a datetime in the given timezone, the values will fall exactly at
the desired resolution steps.

#### Finding anomalous data

Anomalies are found during the insertion of new data. To achieve this,
we need to decide on an anomaly test to use.

Example using the 3-sigma method:

````
$sigmaTest = new FindAnomaliesUsingSigmaTest($conn);
$sigmaTest->setDaysToScan(20); // How many days in the past to use for comparison

// Instead of looking at individual values,
// look at the average value within 300 seconds
$sigmaTest->setWindowLength(300);

$sigmaTest->setMinPrevDataPoints(14); // How many data points are required before we bother doing the scan
$sigmaTest->setMinCurrDataPoints(1); // How many data points we need for evaluation

// The treshold for 3 sigma.
// If the data is normally distributed,
// a value of 3 gives anomalies if the value is outside of the 99.7 percentile
$sigmaTest->setScoreTreshold(3);

// Add sigma test as middleware
$storage->addMiddleware($sigmaTest);

// Store the anomalies as well
$storage->addMiddleware(new StoreAnomalies($conn));
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
$hwTest = new FindAnomaliesUsingHwTest($conn);
$hwTest->setMinDaysScanned(20);
$hwTest->setHwAlpha(0.05); // Smoothing factor for average
$hwTest->setHwBeta(0.005); // Smoothing factor for linear increase
$hwTest->setHwGamma(0.1);  // Smoothing factor std
$hwTest->setScoreTreshold(3); // Same as 3-sigma
$hwTest->setSeasonLength(1440); // Season length in minutes (daily season by default)

$storage->addMiddleware($hwTest);
$storage->addMiddleware(new StoreAnomalies($conn));
````

### Retrieving anomalies

Using the pipeline, we can also find stored anomalies which were detected during insertion.

In addition, we have a helper sequence for finding the anomaly state in a given period.
The state is an array of timestamp => 0|1, rolled up to the desired resolution.

````php
$from = new DateTime('-1 day');
$to = new DateTime('now');
$nids = array(); // Limit results to these nids, no limit if empty
$sids = array(); // Limit results to these sids, no limit if empty
$minNumberOfAnomalies = 1;
$maxResults = 20;

$sequence = [
    new FindAnomalies($conn, $start, $end, $nids, $sids, $minCount, $limit),
    new AddAnomalyState($start, $end, Resolution::FIVE_MINUTES)
];

$pipeline = new Pipeline();
$result = $pipeline->run($sequence);

/*
$results is now an array of the following form:

$result[] = array(
    'nid' => $nid,
    'sid' => $sid,
    'count' => $count,
    'anomalies' => $anomalies,
    'state' => $state // Added by Mongotd\Pipeline\AddAnomalyState
);

Where $anomalies is an array of objects of the class Mongotd\Anomaly
$count is just the length of the $anomalies array.
*/
````