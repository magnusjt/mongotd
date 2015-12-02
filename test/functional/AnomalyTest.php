<?php
require __DIR__ . '/../../vendor/autoload.php';
date_default_timezone_set('Europe/Oslo');

use Mongotd\Connection;
use Mongotd\Pipeline\Factory;
use Mongotd\Pipeline\Pipeline;
use Mongotd\Storage;
use Mongotd\StorageMiddleware\FilterCounterValues;
use Mongotd\StorageMiddleware\InsertCounterValues;
use Mongotd\StorageMiddleware\FindAnomaliesUsingSigmaTest;
use Mongotd\StorageMiddleware\FindAnomaliesUsingHwTest;
use Mongotd\StorageMiddleware\FindAnomaliesUsingKsTest;
use Mongotd\StorageMiddleware\StoreAnomalies;

$config = json_decode(file_get_contents('anomalyTestConfig.json'), true);
$conn = new Connection($config['dbhost'], $config['dbname'], $config['dbprefix']);
$conn->db()->drop();
$conn->createIndexes();
$pipelineFactory = new Factory($conn);
$pipeline = new Pipeline();
$storage = new Storage();
$storage->addMiddleware(new FilterCounterValues());
$storage->addMiddleware(new InsertCounterValues($conn));

if($config['anomalyDetectionMethod'] == 'ks'){
    $storage->addMiddleware(new FindAnomaliesUsingKsTest($conn));
}else if($config['anomalyDetectionMethod'] == 'hw'){
    $storage->addMiddleware(new FindAnomaliesUsingHwTest($conn));
}else if($config['anomalyDetectionMethod'] == 'sigma'){
    $storage->addMiddleware(new FindAnomaliesUsingSigmaTest($conn));
}else{
    throw new Exception('Unknown anomaly scan method');
}
$storage->addMiddleware(new StoreAnomalies($conn));

function generateSineDailyPeriodWithNoise($nDays, $interval = 300, $noiseRate = 0.1){
    $datetimeEnd = new DateTime('now');
    $datetimeStart = clone $datetimeEnd;
    $datetimeStart->sub(DateInterval::createFromDateString($nDays . ' days'));
    $dateperiod = new DatePeriod($datetimeStart, DateInterval::createFromDateString($interval . ' seconds'), $datetimeEnd);

    $series = [];
    $amplitude = 100;
    /** @var \DateTime $datetime */
    foreach($dateperiod as $datetime){
        $seconds = (int)$datetime->format('H')*60*60 + (int)$datetime->format('i')*60 + (int)$datetime->format('s');

        $signal = $amplitude/2 + ($amplitude/2)*sin(M_PI*2*($seconds)/86400);
        $signal += $amplitude*$noiseRate*(rand()%1000)/1000; // Random value between -1 and 1, times noise amplitude
        $series[] = ['datetime' => $datetime, 'value' => $signal];
    }

    return $series;
}

$series = generateSineDailyPeriodWithNoise($config['nDays'], $config['insertIntervalInSeconds'], $config['noiseRate']);

$iteration = 1;
$totalAnomalies = 0;
$totalBoth = 0;
$hits = 0;
$misses = 0;
$falsesPositives = 0;
$persist = 0;

foreach($series as $data){
    echo 'Progress: ' . $iteration . '/' . count($series) . "\r";
    $iteration++;
    $expectedAnomaly = false;

    if(rand()%1000 == 0){
        $persist = $config['anomalyPersistIntervals'];
    }

    if($persist > 0){
        $persist--;
        $expectedAnomaly = true;
        $totalAnomalies++;
        $data['value'] *= 2;
    }

    $storage->store([
        new \Mongotd\CounterValue(1, 1, $data['datetime'], $data['value'])
    ]);

    $datetimeStartInterval = clone $data['datetime'];
    $datetimeStartInterval->sub(DateInterval::createFromDateString(($config['insertIntervalInSeconds']-1) . ' seconds'));

    $sequence = $pipelineFactory->createAnomalyAction($datetimeStartInterval, $data['datetime']);
    $anomalyList = $pipeline->run($sequence);

    $isAnomaly = count($anomalyList) > 0 and count($anomalyList[0]['anomalies']) > 0;

    if($isAnomaly or $expectedAnomaly){
        echo $data['datetime']->format('Y-m-d H:i') . ' - ' . $data['value'];
        if($isAnomaly){
            echo ' Detected anomaly! (predicted avg ' . $anomalyList[0]['anomalies'][0]->predicted . ')';
        }
        if($expectedAnomaly){
            echo ' Actual anomaly!';
        }

        $totalBoth++;
        if($isAnomaly and $expectedAnomaly){
            $hits++;
        }else if($isAnomaly){
            $falsesPositives++;
        }else if($expectedAnomaly){
            $misses++;
        }

        echo "\n";
    }
}

echo 'Hits: ' . $hits . ' (rate: ' . ($hits/$totalAnomalies) . ")\n";
echo 'Misses: ' . $misses . ' (rate: ' . ($misses/$totalAnomalies) . ")\n";
echo 'False positive: ' . $falsesPositives . ' (rate: ' . ($falsesPositives/$totalBoth) . ")\n";

