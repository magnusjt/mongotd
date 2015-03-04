<?php
require __DIR__ . '/../../vendor/autoload.php';
date_default_timezone_set('Europe/Oslo');

$config = json_decode(file_get_contents('anomalyTestConfig.json'), true);
$conn    = new \Mongotd\Connection($config['dbhost'], $config['dbname'], $config['dbprefix']);
$mongotd = new \Mongotd\Mongotd($conn);
$conn->dropDb();
$mongotd->ensureIndexes();
$inserter = $mongotd->getInserter($config['insertIntervalInSeconds'], true, $config['anomalyDetectionMethod']);
$retriever = $mongotd->getRetriever();
$signalGenerator = new \Mongotd\SignalGenerator();

$series = $signalGenerator->generateSineDailyPeriodWithNoise($config['nDays'], $config['insertIntervalInSeconds'], $config['noiseRate']);

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

    $inserter->add(1, 1, $data['datetime'], $data['value']);
    $inserter->insert();

    $datetimeStartInterval = clone $data['datetime'];
    $datetimeStartInterval->sub(\DateInterval::createFromDateString(($config['insertIntervalInSeconds']-1) . ' seconds'));
    $anomalies = $retriever->getAnomalies($datetimeStartInterval, $data['datetime']);

    $isAnomaly = count($anomalies) > 0;

    if($isAnomaly or $expectedAnomaly){
        echo $data['datetime']->format('Y-m-d H:i') . ' - ' . $data['value'];
        if($isAnomaly){
            echo ' Detected anomaly! (predicted avg ' . $anomalies[0]['avg'] . ')';
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

