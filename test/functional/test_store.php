<?php
require(__DIR__.'/../../vendor/autoload.php');
date_default_timezone_set('Europe/Oslo');

$conn = new \Mongotd\Connection('localhost', 'mongotdtest', 'mongotdtest');
$conn->dropDb();

$mongotd = new \Mongotd\Mongotd($conn, null);
$mongotd->ensureIndexes();
$inserter = $mongotd->getInserter(\Mongotd\Resolution::FIVE_MINUTES);
$retriever = $mongotd->getRetriever();
$dateperiod = new DatePeriod(new DateTime('2014-09-10 00:00:00'), DateInterval::createFromDateString('5 minutes'), new DateTime('2014-09-20 05:00:00'));

/** @var \DateTime $datetime */
foreach($dateperiod as $datetime){
    $start = microtime(true);

    echo $datetime->format("Y-m-d H:i:s") . "\n";

    for($nid = 1; $nid <= 3; $nid++){
        for($sid = 1; $sid <= 100; $sid++){
            $inserter->add($sid, $nid, $datetime, $sid*100 + 100*sin(2*pi()*(($datetime->getTimestamp()/60)%1440)/1440) + rand()%20);
        }
    }

    try{
        $inserter->insert();
    }catch(Exception $e){
        echo "ERROR: " . $conn->db()->lastError()['err'] . "\n";
        echo $e->getMessage();
    }

    break;

    try{
        $abnormals = $retriever->getCurrentAbnormal();
        if(count($abnormals) > 0){
            foreach($abnormals as $abnormal){
                echo $abnormal['sid'] . ', Val: ' . $abnormal['val'] . ', Predicted: ' . $abnormal['pred'] . "\n";
            }
        }
    }catch(Exception $e){
        echo "ERROR: " . $conn->db()->lastError()['err'] . "\n";
        echo $e->getMessage();
    }

    echo "Time taken: " . (microtime(true) - $start) . "\n";
    echo "Peak memory used: " . memory_get_peak_usage(true)/(1024*1000)  . " MB\n\n";
}