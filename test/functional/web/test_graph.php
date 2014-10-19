<?php
require(__DIR__ . '/../../../vendor/autoload.php');
date_default_timezone_set('Europe/Oslo');

use \Mongotd\Connection;
use \Mongotd\Mongotd;
use \Mongotd\Resolution;
use \Mongotd\Aggregation;

$conn = new Connection('localhost', 'mongotdtest', 'mongotdtest');
$mongotd = new Mongotd($conn, null);
$retriever = $mongotd->getRetriever();

$sids = array(1,2);
$start = "2014-09-10 00:00:00";
$end = "2014-09-20 00:00:00";
$aggregation = Aggregation::SUM;
$resolution = Resolution::HOUR;

if(isset($_GET['sids'])) $sids = explode(',', $_GET['sids']);
if(isset($_GET['start'])) $start = $_GET['start'];
if(isset($_GET['end'])) $end = $_GET['end'];
if(isset($_GET['aggregation'])) $aggregation = $_GET['aggregation'];
if(isset($_GET['resolution'])) $resolution = $_GET['resolution'];

$labels = array();
$vals_by_date_list = array();

foreach($sids as $sid){
    $sid = (int)trim($sid);
    $vals_by_date_list[] = $retriever->get($sid, new DateTime($start), new DateTime($end), $resolution, $aggregation, null);
    $labels[] = "SID $sid";
}

$flot_str = valsByDateToFlot($vals_by_date_list, $labels);

function valsByDateToFlot($vals_by_date_list, $labels){
    if(count($vals_by_date_list) == 0){
        throw new Exception("Empty data");
    }

    if(count($vals_by_date_list) != count($labels)){
        throw new Exception("Label count not equal to graph line count");
    }

    $data_list = array();
    $min_timestamp = 1000*strtotime(key($vals_by_date_list[0]));
    end($vals_by_date_list[0]);
    $max_timestamp = 1000*strtotime(key($vals_by_date_list[0]));
    reset($vals_by_date_list[0]);

    for($i = 0; $i < count($vals_by_date_list); $i++){
        $data = array();
        foreach($vals_by_date_list[$i] as $datetime_str => $val){
            $datetime = new DateTime($datetime_str);
            $data[] = array($datetime->getTimestamp()*1000, $val);
        }

        $data_list[] = array('data' => $data, 'label' => $labels[$i]);
    }

    $options = array(
        'xaxis' => array(
            'mode' => 'time',
            'timezone' => 'browser',
            'timeformat' => "%d/%m \n %H:%M",
            'min' => $min_timestamp,
            'max' => $max_timestamp
        ),
        'series' => array(
            'lines' => array('show' => true),
            'points' => array('show' => true),
        )
    );

    return '$.plot("#placeholder", ' . json_encode($data_list) . ', ' . json_encode($options) . ' );';
}
?>
<!DOCTYPE html>
<html>
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
    <script src="js/jquery-1.11.1.min.js"></script>
    <script src="js/bootstrap-3.2.0-dist/js/bootstrap.min.js"></script>
    <script src="js/flot-0.8.3/flot/jquery.flot.min.js"></script>
    <script src="js/flot-0.8.3/flot/jquery.flot.time.min.js"></script>
    <link rel="stylesheet" href="js/bootstrap-3.2.0-dist/css/bootstrap.min.css" />
    <script>
        $(document).ready(function(){
            <?php echo $flot_str; ?>
        });
    </script>
</head>
<body>
<div class="container">
    <div id="placeholder" style="width: 960px; height: 400px; margin: 0 auto;"></div>
    <br />
    <form method="get" class="form-horizontal">
        <div class="form-group">
            <label class="col-sm-2 control-label">Sids:</label>
            <div class="col-sm-2">
                <input type="text" name="sids" value="<?php echo implode(',', $sids);?>" class="form-control" />
            </div>
        </div>
        <div class="form-group">
            <label class="col-sm-2 control-label">Start:</label>
            <div class="col-sm-2">
                <input type="datetime" name="start" value="<?php echo $start;?>" class="form-control" />
            </div>
        </div>
        <div class="form-group">
            <label class="col-sm-2 control-label">End:</label>
            <div class="col-sm-2">
                <input type="datetime" name="end" value="<?php echo $end;?>" class="form-control" />
            </div>
        </div>
        <div class="form-group">
            <label class="col-sm-2 control-label">Resolution:</label>
            <div class="col-sm-2">
            <select name="resolution" class="form-control">
                <option value="<?php echo Resolution::DAY;?>"              <?php echo $resolution==Resolution::DAY?'selected':'';?>>Daily</option>
                <option value="<?php echo Resolution::HOUR;?>"             <?php echo $resolution==Resolution::HOUR?'selected':'';?>>Hourly</option>
                <option value="<?php echo Resolution::FIFTEEEN_MINUTES;?>" <?php echo $resolution==Resolution::FIFTEEEN_MINUTES?'selected':'';?>>Fifteen min</option>
                <option value="<?php echo Resolution::FIVE_MINUTES;?>"     <?php echo $resolution==Resolution::FIVE_MINUTES?'selected':'';?>>Five min</option>
                <option value="<?php echo Resolution::MINUTE;?>"           <?php echo $resolution==Resolution::MINUTE?'selected':'';?>>One min</option>
            </select>
            </div>
        </div>
        <div class="form-group">
            <label class="col-sm-2 control-label">Aggregation:</label>
            <div class="col-sm-2">
            <select name="aggregation" class="form-control">
                <option value="<?php echo Aggregation::SUM;?>" <?php echo $aggregation==Aggregation::SUM?'selected':'';?>>Sum</option>
                <option value="<?php echo Aggregation::AVG;?>" <?php echo $aggregation==Aggregation::AVG?'selected':'';?>>Average</option>
            </select>
            </div>
        </div>
        <div class="form-group">
            <div class="col-sm-2 col-sm-offset-2">
                <input type="submit" value="Load" class="btn btn-success">
            </div>
        </div>
    </form>
</div>
</body>
</html>