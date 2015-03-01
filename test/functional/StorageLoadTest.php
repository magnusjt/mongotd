<?php
/*
 * Insert 1 days of 5-min values every iteration. Does this with a bunch of sids/nids.
 * Many sids/nids/datetimes, so should be fairly realistic situation.
 *
 * Simulates 60 days of data with 4000 "sensors" (20 nodes, 200 sensors), once every 5 minutes
 * 60*4000*12*24 = about 70M data points total
 */
require __DIR__ . '/../../vendor/autoload.php';
date_default_timezone_set('Europe/Oslo');
$conn = new \Mongotd\Connection('localhost', 'test', 'test');
$mongotd = new \Mongotd\Mongotd($conn);
$conn->dropDb();
$mongotd->ensureIndexes();
$inserter = $mongotd->getInserter(\Mongotd\Resolution::FIVE_MINUTES);

$start = new \DateTime('2015-02-28 00:00:00');
$iteration = 1;
$totalInserts = 0;
while($iteration <= 60){
    $end = clone $start;
    $end->add(DateInterval::createFromDateString('1 day'));
    $dateperiod = new \DatePeriod($start, DateInterval::createFromDateString('5 minutes'), $end);

    $timerStart = microtime(true);

    $inserts = 0;
    foreach($dateperiod as $datetime){
        for($nid = 1; $nid <= 20; $nid++){
            for($sid = 1; $sid <= 200; $sid++){
                $inserter->add(1, 1, $datetime, rand());
                $inserts++;
            }
        }

        $inserter->insert();
    }

    $totalInserts += $inserts;

    $totalTime = (microtime(true) - $timerStart);
    echo 'Iteration: ' . $iteration . "\n";
    echo 'Time taken: ' . $totalTime . ' seconds' . "\n";
    echo 'Total inserts: ' . $totalInserts . "\n";
    echo 'Inserts/sec: ' . $inserts/$totalTime . "\n";
    echo 'Peak memory used: ' . memory_get_peak_usage(true)/(1024*1000)  . ' MB' . "\n\n";

    $start->add(DateInterval::createFromDateString('10 days'));
    $iteration++;
}