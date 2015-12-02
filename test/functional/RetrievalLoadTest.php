<?php
require __DIR__ . '/../../vendor/autoload.php';

use Mongotd\Connection;
use Mongotd\CounterValue;
use Mongotd\Pipeline\Factory;
use Mongotd\Pipeline\Pipeline;
use Mongotd\Storage;
use Mongotd\StorageMiddleware\FilterCounterValues;
use Mongotd\StorageMiddleware\InsertCounterValues;

$config = json_decode(file_get_contents('retrievalLoadTestConfig.json'), true);
date_default_timezone_set($config['timezone']);
$start = new \DateTime($config['starttime']);
$end = new \DateTime('now');
$conn = new Connection($config['dbhost'], $config['dbname'], $config['dbprefix']);
$sid = '1';
$nid = '1';
$pipelineFactory = new Factory($conn);
$pipeline = new Pipeline();
$storage = new Storage();
$storage->addMiddleware(new FilterCounterValues());
$storage->addMiddleware(new InsertCounterValues($conn));

if($config['doInsertion']){
    $conn->db()->drop();
    $conn->createIndexes();
    $dateperiod = new \DatePeriod($start, DateInterval::createFromDateString($config['insertIntervalInSeconds'] . ' seconds'), $end);
    $cvs = [];
    foreach($dateperiod as $datetime){
        $cvs[] = new CounterValue($sid, $nid, $datetime, rand());
    }
    $storage->store($cvs);
    echo "Insertion done\n";
}

$timerStart = microtime(true);
$sequence = $pipelineFactory->createMultiAction($sid, $nid, $start, $end, $config['retrieveResolution'], $config['retrieveAggregation']);
$pipeline->run($sequence);
$totalTime = (microtime(true) - $timerStart);
echo 'Retrieved in: ' . $totalTime . " seconds\n";