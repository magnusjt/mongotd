<?php
require __DIR__ . '/../../vendor/autoload.php';

use Mongotd\Connection;
use Mongotd\Mongotd;

$config = json_decode(file_get_contents('retrievalLoadTestConfig.json'), true);
date_default_timezone_set($config['timezone']);
$start = new \DateTime($config['starttime']);
$end = new \DateTime('now');
$conn = new Connection($config['dbhost'], $config['dbname'], $config['dbprefix']);
$mongotd = new Mongotd($conn);
$sid = '1';
$nid = '1';
$inserter = $mongotd->getInserter();
$inserter->setInterval($config['insertIntervalInSeconds']);
$retriever = $mongotd->getRetriever();

if($config['doInsertion']){
    $conn->db()->drop();
    $mongotd->ensureIndexes();
    $dateperiod = new \DatePeriod($start, DateInterval::createFromDateString($config['insertIntervalInSeconds'] . ' seconds'), $end);
    foreach($dateperiod as $datetime){
        $inserter->add($sid, $nid, $datetime, rand(), false);
    }
    $inserter->insert();
    echo "Insertion done\n";
}

$timerStart = microtime(true);
$retriever->get($sid, $nid, $start, $end, $config['retrieveResolution'], $config['retrieveAggregation']);
$totalTime = (microtime(true) - $timerStart);
echo 'Retrieved in: ' . $totalTime . " seconds\n";