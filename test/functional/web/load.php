<?php
require(__DIR__ . '/../../../vendor/autoload.php');
date_default_timezone_set('Europe/Oslo');

use \Mongotd\Connection;
use Mongotd\FlotFormatter;
use Mongotd\FlotLoader;
use \Mongotd\Mongotd;
use \Mongotd\Resolution;
use \Mongotd\Aggregation;

set_error_handler(function($errno, $errstr, $errfile, $errline, array $errcontext){
    header($_SERVER['SERVER_PROTOCOL'] . ' 500 Internal Server Error', true, 500);
    die($errstr.
        ' file '.
        $errfile.
        ' line '.
        $errline.
        ' '.
        json_encode($errcontext, JSON_PRETTY_PRINT).
        ' '.
        json_encode(debug_backtrace(), JSON_PRETTY_PRINT)
    );
});
set_exception_handler(function($e){
    header($_SERVER['SERVER_PROTOCOL'] . ' 500 Internal Server Error', true, 500);
    die($e->getMessage());
});
register_shutdown_function(function(){
    $error = error_get_last();
    if($error !== NULL){
        header($_SERVER['SERVER_PROTOCOL'] . ' 500 Internal Server Error', true, 500);
        die($error['message']);
    }
});

function aggregationStrToEnum($aggregation){
    switch($aggregation){
        case 'SUM': return Aggregation::SUM;
        case 'AVG': return Aggregation::AVG;
        case 'MAX': return Aggregation::MAX;
        case 'MIN': return Aggregation::MIN;
    }

    throw new Exception('Invalid aggregation');
}
function resolutionStrToEnum($resolution){
    switch($resolution){
        case 'DAY': return Resolution::DAY;
        case 'HOUR': return Resolution::HOUR;
        case 'FIFTEEN_MINUTES': return Resolution::FIFTEEEN_MINUTES;
        case 'FIVE_MINUTES': return Resolution::FIVE_MINUTES;
        case 'ONE_MINUTE': return Resolution::MINUTE;
    }

    throw new Exception('Invalid resolution');
}

$conn = new Connection('localhost', 'test', 'test');
$mongotd = new Mongotd($conn, null);
$retriever = $mongotd->getRetriever();

$asFormula = false;
$nids = array(1,2);
$sids = array(1,2);
$start = "-20 days";
$end = "now";
$aggregation = Aggregation::SUM;
$resolution = Resolution::HOUR;
$combinedNodeAggregation = Aggregation::SUM;
$singleNodeAggregation = Aggregation::SUM;
$nodeResolution = Resolution::FIVE_MINUTES;
$formulaResolution = Resolution::FIVE_MINUTES;

if(isset($_GET['asFormula'])) $asFormula = $_GET['asFormula']==='true';

if($asFormula){
    $sids = array($_GET['sids']);
}else{
    if(isset($_GET['sids'])) $sids = explode(',', $_GET['sids']);
}

if(isset($_GET['nids'])) $nids = explode(',', $_GET['nids']);
if(isset($_GET['start'])) $start = $_GET['start'];
if(isset($_GET['end'])) $end = $_GET['end'];
if(isset($_GET['aggregation'])) $aggregation = aggregationStrToEnum($_GET['aggregation']);
if(isset($_GET['resolution'])) $resolution = resolutionStrToEnum($_GET['resolution']);
if(isset($_GET['singleNodeAggregation'])) $singleNodeAggregation = aggregationStrToEnum($_GET['singleNodeAggregation']);
if(isset($_GET['combinedNodeAggregation'])) $combinedNodeAggregation = aggregationStrToEnum($_GET['combinedNodeAggregation']);
if(isset($_GET['nodeResolution'])) $nodeResolution = resolutionStrToEnum($_GET['nodeResolution']);
if(isset($_GET['formulaResolution'])) $formulaResolution = resolutionStrToEnum($_GET['formulaResolution']);

$flotData = array();
$start = new DateTime($start);
$end = new DateTime($end);
foreach($sids as $sid){
    $valsByTimestamp = $retriever->get(
        $sid,
        $nids,
        $start,
        $end,
        $resolution,
        $aggregation,
        false,
        $nodeResolution,
        $singleNodeAggregation,
        $combinedNodeAggregation,
        $asFormula,
        $formulaResolution
    );
    $data = array_map(function($timestamp, $value){
        $value===false?$value=null:$value; // Flot required 'null' to be the value if no value is present
        return array($timestamp*1000, $value);
    }, array_keys($valsByTimestamp), $valsByTimestamp);

    $flotData[] = array(
        'data' => $data,
        'label' => 'Sensor '.$sid,
        'xaxis' => 1,
        'yaxis' => 1
    );
}

$res = $retriever->getAnomalies($start, $end, $nids, $sids, 0, count($nids)*count($nids));
$anomalies = array();
foreach($res as $row){
    $anomalies = array_merge($anomalies, $row['anomalies']);
}
$anomalyStates = $retriever->getAnomalyStates($anomalies, $start, $end, $resolution);
$data = array_map(function($timestamp, $value){
    return array($timestamp*1000, $value);
}, array_keys($anomalyStates), $anomalyStates);

$flotData[] = array(
    'data' => $data,
    'label' => false,
    'xaxis' => 2,
    'yaxis' => 2,
    'state' => array(
        'show' => true,
        'showText' => false,
        'label' => '',
        'states' => array(0 => "rgba(0,255,0,1)", 1 => "rgba(255,0,0,1)")
    )
);

$min = $flotData[0]['data'][0][0];
$max = $flotData[0]['data'][count($flotData[0]['data']) - 1][0];

echo json_encode(array(
    'data' => $flotData,
    'title' => 'Test',
    'min' => $min,
    'max' => $max
));