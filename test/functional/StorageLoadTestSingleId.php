<?php
/*
 * Insert 10 days of 5-min values every iteration.
 * Use same sid/nid, so there will only be one "sensor" in the database, but many dates.
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
while($iteration <= 100){
    $end = clone $start;
    $end->add(DateInterval::createFromDateString('10 days'));
    $dateperiod = new \DatePeriod($start, DateInterval::createFromDateString('5 minutes'), $end);

    $timerStart = microtime(true);

    $inserts = 0;
    foreach($dateperiod as $datetime){
        $inserter->add(1, 1, $datetime, rand());
        $inserts++;
    }

    $totalInserts += $inserts;
    $inserter->insert();

    $totalTime = (microtime(true) - $timerStart);
    echo 'Iteration: ' . $iteration . "\n";
    echo 'Time taken: ' . $totalTime . ' seconds' . "\n";
    echo 'Total inserts: ' . $totalInserts . "\n";
    echo 'Inserts/sec: ' . $inserts/$totalTime . "\n";
    echo 'Peak memory used: ' . memory_get_peak_usage(true)/(1024*1000)  . ' MB' . "\n\n";

    $start->add(DateInterval::createFromDateString('10 days'));
    $iteration++;
}