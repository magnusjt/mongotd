<?php
require __DIR__ . '/../../vendor/autoload.php';

use Mongotd\Connection;
use Mongotd\CounterValue;
use Mongotd\Logger;
use Mongotd\Pipeline\Factory;
use Mongotd\Pipeline\Pipeline;
use Mongotd\Storage;
use Mongotd\StorageMiddleware\CalculateDeltas;
use Mongotd\StorageMiddleware\FilterCounterValues;
use Mongotd\StorageMiddleware\FindAnomaliesUsingHwTest;
use Mongotd\StorageMiddleware\FindAnomaliesUsingKsTest;
use Mongotd\StorageMiddleware\FindAnomaliesUsingSigmaTest;
use Mongotd\StorageMiddleware\InsertCounterValues;
use Mongotd\StorageMiddleware\StoreAnomalies;

$config = json_decode(file_get_contents('loadTestConfig.json'), true);
date_default_timezone_set($config['timezone']);
$start = new \DateTime($config['starttime']);
$conn = new Connection($config['dbhost'], $config['dbname'], $config['dbprefix']);
$conn->db()->drop();
$conn->createIndexes();
$pipelineFactory = new Factory($conn);
$pipeline = new Pipeline();
$storage = new Storage();
$storage->addMiddleware(new FilterCounterValues());
$storage->addMiddleware(new InsertCounterValues($conn));
$storage->addMiddleware(new CalculateDeltas($conn, new Logger(null), $config['insertIntervalInSeconds']));

if($config['anomalyDetectionMethod'] == 'ks'){
    $storage->addMiddleware(new FindAnomaliesUsingKsTest($conn));
    $storage->addMiddleware(new StoreAnomalies($conn));
}else if($config['anomalyDetectionMethod'] == 'hw'){
    $storage->addMiddleware(new FindAnomaliesUsingHwTest($conn));
    $storage->addMiddleware(new StoreAnomalies($conn));
}else if($config['anomalyDetectionMethod'] == 'sigma'){
    $storage->addMiddleware(new FindAnomaliesUsingSigmaTest($conn));
    $storage->addMiddleware(new StoreAnomalies($conn));
}

$currIteration = 1;
$totalInserts = 0;

while($currIteration <= $config['nIterations']){
    echo "Starting iteration $currIteration...\n";
    $end = clone $start;
    $end->add(DateInterval::createFromDateString($config['daysPerIteration']));
    $dateperiod = new DatePeriod($start, DateInterval::createFromDateString($config['insertIntervalInSeconds'] . ' seconds'), $end);

    $timerStart = microtime(true);
    $inserts = 0;
    /** @var \DateTime $datetime */
    foreach($dateperiod as $datetime){
        echo "Date/Time: " . $datetime->format('Y-m-d H:i:s') . "\r";
        $cvs = [];
        for($nid = 1; $nid <= $config['nNids']; $nid++){
            for($sid = 1; $sid <= $config['nSids']; $sid++){
                $cvs[] = new CounterValue($sid, $nid, $datetime, rand(), $config['valuesAreIncremental']);
            }
        }

        $inserts += count($cvs);
        $totalInserts += count($cvs);
        $storage->store($cvs);
    }

    $insertTime = (microtime(true) - $timerStart);
    $peakMem = memory_get_peak_usage(true) / (1024 * 1000);
    $insertRate = $inserts / $insertTime;
    $stats = $conn->db()->command(array('dbstats' => 1, 'scale' => 1000*1024));

    echo "\n" . '------------- Inserts -------------' . "\n";
    echo 'Time taken:       ' . $insertTime . ' seconds'    . "\n";
    echo 'Total inserts:    ' . $totalInserts               . "\n";
    echo 'Inserts/sec:      ' . $insertRate                 . "\n";
    echo 'DB data size:     ' . $stats['dataSize'] . 'MB'   . "\n";

    if($config['doRetrievalTest']){
        echo '------------- Retrievals -------------' . "\n";
        $timerStart = microtime(true);
        $retrievals = 0;
        for($nid = 1; $nid <= $config['nNids']; $nid++){
            for($sid = 1; $sid <= $config['nSids']; $sid++){
                $sequence = $pipelineFactory->createMultiAction($sid, $nid, $start, $end, $config['retrieveResolution'], $config['retrieveAggregation']);
                $pipeline->run($sequence);
                $retrievals++;
            }
        }

        $retrievalTime = (microtime(true) - $timerStart);

        echo 'Time taken:      ' . $retrievalTime             . "\n";
        echo 'Retrievals done: ' . $retrievals                . "\n";
        echo 'Retrievals/sec:  ' . $retrievals/$retrievalTime . "\n";
    }

    $start->add(DateInterval::createFromDateString($config['daysPerIteration']));
    $currIteration++;
    echo 'Peak memory used: ' . $peakMem . ' MB' . "\n";
    echo "\n";
}