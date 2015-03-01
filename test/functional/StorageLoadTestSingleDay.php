<?php
/*
 * Insert 1 day of 5-minute values.
 * Insert with a bunch of nids and sids
 * Many "sensors", but only a day of data
 */
require __DIR__ . '/../../vendor/autoload.php';
date_default_timezone_set('Europe/Oslo');
$conn = new \Mongotd\Connection('localhost', 'test', 'test');
$mongotd = new \Mongotd\Mongotd($conn);
$conn->dropDb();
$mongotd->ensureIndexes();
$inserter = $mongotd->getInserter(\Mongotd\Resolution::FIVE_MINUTES);

$start = new \DateTime('2015-02-28 00:00:00');
$end = new \DateTime('2015-02-29 00:00:00');

$dateperiod = new \DatePeriod($start, DateInterval::createFromDateString('5 minutes'), $end);

$iteration = 1;
$totalInserts = 0;
foreach($dateperiod as $datetime){
    $timerStart = microtime(true);

    $inserts = 0;
    for($nid = 1; $nid < 20; $nid++){
        for($sid = 1; $sid < 200; $sid++){
            $inserter->add($sid, $nid, $datetime, rand());
            $inserts++;
        }
    }

    $totalInserts += $inserts;
    $inserter->insert();

    $totalTime = (microtime(true) - $timerStart);
    echo 'Iteration: ' . $iteration . "\n";
    echo 'Time taken: ' . $totalTime . ' seconds' . "\n";
    echo 'Total inserts: ' . $totalInserts . "\n";
    echo 'Inserts/sec: ' . $inserts/$totalTime . "\n";
    echo 'Peak memory used: ' . memory_get_peak_usage(true)/(1024*1000)  . ' MB' . "\n\n";
    $iteration++;
}