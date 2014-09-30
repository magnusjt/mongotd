<?php
require(__DIR__.'/../vendor/autoload.php');
date_default_timezone_set('Europe/Oslo');

$conn = new \Mongotd\Connection('localhost', 'mongotdtest', 'mongotdtest');
$conn->dropDb();

$mongotd = new \Mongotd\Mongotd($conn, null);
$inserter = $mongotd->getInserter(\Mongotd\Resolution::FIVE_MINUTES);
$dateperiod = new DatePeriod(new DateTime('2014-09-10 00:00:00'), DateInterval::createFromDateString('5 minutes'), new DateTime('2014-09-20 05:00:00'));

foreach($dateperiod as $datetime){
    $start = microtime(true);
    $inserter->setDatetime($datetime);

    echo $datetime->format("Y-m-d H:i:s") . "\n";

    for($sid = 1; $sid <= 10000; $sid++){
        $inserter->add($sid, $sid*10000 + rand());
    }

    try{
        $inserter->execute();
    }catch(Exception $e){
        echo "ERROR: " . $conn->db()->lastError()['err'] . "\n";
    }

    echo "Time taken: " . (microtime(true) - $start) . "\n";
    echo "Peak memory used: " . memory_get_peak_usage(true)/(1024*1000)  . " MB\n\n";
}