<?php
require __DIR__ . '/../../vendor/autoload.php';

use \Mongotd\Connection;
use \Mongotd\Mongotd;
use \Mongotd\AnomalyScanner3Sigma;
use \Mongotd\AnomalyScannerHw;
use \Mongotd\AnomalyScannerKs;

$config = json_decode(file_get_contents('loadTestConfig.json'), true);
date_default_timezone_set($config['timezone']);
$start = new \DateTime($config['starttime']);
$conn = Connection::fromParameters($config['dbhost'], $config['dbname'], $config['dbprefix']);
$mongotd = new Mongotd($conn);
$conn->dropDb();
$mongotd->ensureIndexes();
$inserter = $mongotd->getInserter();
$retriever = $mongotd->getRetriever();

$inserter->setInterval($config['insertIntervalInSeconds']);

if($config['anomalyDetectionMethod'] == 'ks'){
    $inserter->setAnomalyScanner(new AnomalyScannerKs($conn));
}else if($config['anomalyDetectionMethod'] == 'hw'){
    $inserter->setAnomalyScanner(new AnomalyScannerHw($conn));
}else if($config['anomalyDetectionMethod'] == 'sigma'){
    $inserter->setAnomalyScanner(new AnomalyScanner3Sigma($conn));
}

$currIteration = 1;
$totalInserts = 0;

$nidsidPairs = array();
for($nid = 1; $nid <= $config['nNids']; $nid++){
    for($sid = 1; $sid <= $config['nSids']; $sid++){
        $nidsidPairs[] = array('nid' => $nid, 'sid' => $sid);
    }
}

while($currIteration <= $config['nIterations']){
    echo "Starting iteration $currIteration...\n";
    $end = clone $start;
    $end->add(DateInterval::createFromDateString($config['daysPerIteration']));
    $dateperiod = new \DatePeriod($start, DateInterval::createFromDateString($config['insertIntervalInSeconds'] . ' seconds'), $end);

    $timerStart = microtime(true);
    $inserts = 0;
    /** @var \DateTime $datetime */
    foreach($dateperiod as $datetime){
        echo "Date/Time: " . $datetime->format('Y-m-d H:i:s') . "\r";
        for($nid = 1; $nid <= $config['nNids']; $nid++){
            for($sid = 1; $sid <= $config['nSids']; $sid++){
                $inserter->add($sid, $nid, $datetime, rand(), $config['valuesAreIncremental']);
                $inserts++;
                $totalInserts++;
            }
        }

        $inserter->insert();
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
                $retriever->get($nid, $sid, $start, $end, $config['retrieveResolution'], $config['retrieveAggregation']);
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