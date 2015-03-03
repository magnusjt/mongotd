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
$resultText = '';
foreach($series as $data){
    echo 'Progress: ' . $iteration . '/' . count($series) . "\r";
    $iteration++;

    $expectedAnomaly = false;
    if(rand()%1000 == 0){
        $expectedAnomaly = true;
        $data['value'] *= 2;
    }

    $inserter->add(1, 1, $data['datetime'], $data['value']);
    $inserter->insert();

    $resultText .= $data['datetime']->format('Y-m-d H:i') . ' - ' . $data['value'];

    $datetimeStartInterval = clone $data['datetime'];
    $datetimeStartInterval->sub(\DateInterval::createFromDateString($config['insertIntervalInSeconds'] . ' seconds'));
    $anomalies = $retriever->getAnomalies($datetimeStartInterval, $data['datetime']);
    if(count($anomalies) > 0){
        $resultText .= ' Anomaly!';
    }
    if($expectedAnomaly){
        $resultText .= ' Anomaly Expected!';
    }

    $resultText .= "\n";
}

echo $resultText;

