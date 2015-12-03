<?php namespace Mongotd\Pipeline;

use DateTime;
use Mongotd\Aggregation;

/**
 * This pipe takes input as given by the FindAnomalies pipe.
 * It looks at each list of anomalies, and creates a
 * state list based on them. The state list is a
 * series of 0's and 1's, indicating anomalies.
 * At the same time, the list is rolled up in
 * time, so any anomaly in a given interval
 * will result in an anomaly for the entire
 * interval.
 */
class AddAnomalyState{
    public $start;
    public $end;
    public $resolution;

    public function __construct(DateTime $start, DateTime $end, $resolution){
        $this->start = $start;
        $this->end = $end;
        $this->resolution = $resolution;
    }

    public function run(array $anomalyList){
        foreach($anomalyList as &$anomalyResult){
            $anomalyResult['state'] = $this->getState($anomalyResult['anomalies']);
        }

        return $anomalyList;
    }

    protected function getState(array $anomalies){
        $statesByTimestamp = [];
        foreach($anomalies as $anomaly){
            $statesByTimestamp[$anomaly->cv->datetime->getTimestamp()] = 1;
        }
        $series = new Series($statesByTimestamp);

        $pipeline = new Pipeline();
        return $pipeline->run([
            new RollupTime($this->resolution, Aggregation::MAX),
            new Pad($this->resolution, $this->start, $this->end, 0)
        ], $series)->vals;
    }
}