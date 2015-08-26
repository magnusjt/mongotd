<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;
use \DateTime;
use \DateInterval;
use \DateTimeZone;
use \MongoDate;

class Retriever{
    /** @var  Connection */
    private $conn;

    /** @var  LoggerInterface */
    private $logger;

    /** @var  KpiParser */
    private $kpiParser;

    /** @var  AstEvaluator */
    private $astEvaluator;

    public function __construct(Connection $conn, LoggerInterface $logger, KpiParser $kpiParser, AstEvaluator $astEvaluator){
        $this->conn = $conn;
        $this->logger = $logger;
        $this->kpiParser = $kpiParser;
        $this->astEvaluator = $astEvaluator;
    }

    protected function getRaw(
        $sid,
        $nid,
        DateTime $start,
        DateTime $end
    ){
        $start = clone $start;
        $end = clone $end;
        $start->setTimezone(new DateTimeZone('UTC'))->setTime(0,0,0);
        $end->setTimezone(new DateTimeZone('UTC'))->setTime(0,0,0);

        $match = array(
            'sid' => (string)$sid,
            'nid' => (string)$nid,
            'mongodate' => array(
                '$gte' => new MongoDate($start->getTimestamp()),
                '$lte' => new MongoDate($end->getTimestamp())
            )
        );

        $cursor = $this->conn->col('cv')->find($match, array('mongodate' => 1, 'vals' => 1));

        $valsByTimestamp = array();
        foreach($cursor as $doc){
            $timestamp = $doc['mongodate']->sec;
            foreach($doc['vals'] as $seconds => $value){
                if($value === null){
                    continue;
                }

                $valsByTimestamp[$timestamp+$seconds] = $value;
            }
        }

        return $valsByTimestamp;
    }

    protected function evaluateFormula(
        $formula,
        $nid,
        DateTime $start,
        DateTime $end,
        $resolution,
        $padding
    ){
        $timezoneOffset = $start->getTimezone()->getOffset($start);
        $this->astEvaluator->setPaddingValue($padding);
        $this->astEvaluator->setVariableEvaluatorCallback(
            function ($options) use ($nid, $start, $end, $resolution, $timezoneOffset, $padding){
            if (!isset($options['sid'])) {
                throw new \Exception('sid was not specified in variable. Need this to determine which sensor to get for the calculation of the formula.');
            }
            if (!isset($options['agg'])) {
                throw new \Exception('agg was not specified in variable. Need this in order to aggregate up to the correct resolution before calculating formula.');
            }

            $valsByTimestamp = $this->getRaw($options['sid'], $nid, $start, $end);
            $valsByTimestamp = $this->rollUpTime($valsByTimestamp, $resolution, $options['agg'], $timezoneOffset, $padding);
            $valsByTimestamp = $this->padValues($valsByTimestamp, $start->getTimestamp(), $end->getTimestamp(), $resolution, $padding);
            return $valsByTimestamp;
        });

        $astNode = $this->kpiParser->parse($formula);
        return $this->astEvaluator->evaluate($astNode);
    }

    /**
     * @param string          $sid
     * @param string|string[] $nid
     * @param DateTime        $start
     * @param DateTime        $end
     * @param int             $resultResolution
     * @param int             $resultAggregation
     * @param mixed           $padding
     * @param int             $nodeResolution
     * @param int             $singleNodeAggregation
     * @param int             $combinedNodesAggregation
     * @param bool            $evaluateAsFormula
     * @param int             $formulaResolution
     *
     * @return array
     * @throws \Exception
     */
    public function get(
        $sid,
        $nid,
        DateTime $start,
        DateTime $end,
        $resultResolution = Resolution::FIFTEEEN_MINUTES,
        $resultAggregation = Aggregation::SUM,
        $padding = false,
        $nodeResolution = Resolution::FIFTEEEN_MINUTES,
        $singleNodeAggregation = Aggregation::SUM,
        $combinedNodesAggregation = Aggregation::SUM,
        $evaluateAsFormula = false,
        $formulaResolution = Resolution::FIFTEEEN_MINUTES
    ){
        $start = clone $start;
        $end = clone $end;
        $this->normalizeDatetimes($start, $end, $resultResolution);
        $timezoneOffset = $start->getTimezone()->getOffset($start);

        if(is_array($nid) and count($nid) == 1){
            $nid = $nid[0];
        }

        if(is_array($nid)){
            $series = array();
            foreach($nid as $aNid){
                if($evaluateAsFormula){
                    $serie = $this->evaluateFormula($sid, $aNid, $start, $end, $formulaResolution, $padding);
                }else{
                    $serie = $this->getRaw($sid, $aNid, $start, $end);
                }

                $serie = $this->rollUpTime($serie, $nodeResolution, $singleNodeAggregation, $timezoneOffset, $padding);
                $serie = $this->padValues($serie, $start->getTimestamp(), $end->getTimestamp(), $nodeResolution, $padding);
                $series[] = $serie;
            }
            $valsByTimestamp = $this->rollUpAcross($series, $combinedNodesAggregation, $padding);
        }else{
            if($evaluateAsFormula){
                $valsByTimestamp = $this->evaluateFormula($sid, $nid, $start, $end, $formulaResolution, $padding);
            }else{
                $valsByTimestamp = $this->getRaw($sid, $nid, $start, $end);
            }
        }

        $valsByTimestamp = $this->rollUpTime($valsByTimestamp, $resultResolution, $resultAggregation, $timezoneOffset, $padding);
        return $this->padValues($valsByTimestamp, $start->getTimestamp(), $end->getTimestamp(), $resultResolution, $padding);
    }

    /**
     * @param array        $valsByTimestamp
     * @param DateTimeZone $timezone
     *
     * @return array
     */
    public function convertToDateStringKeys($valsByTimestamp, DateTimeZone $timezone){
        $valsByDateStr = array();
        foreach($valsByTimestamp as $timestamp => $value){
            $datetime = new DateTime('@'.($timestamp));
            $datetime->setTimezone($timezone);
            $valsByDateStr[$datetime->format('Y-m-d H:i:s')] = $value;
        }

        return $valsByDateStr;
    }

    /**
     * Makes sure there is a data point for every resolution step.
     * Uses the padding value if there is no existing value at a given step.
     *
     * @param $valsByTimestampIn array
     * @param $start             int
     * @param $end               int
     * @param $resolution        int
     * @param $padding           mixed
     * @return array
     */
    public function padValues($valsByTimestampIn, $start, $end, $resolution, $padding){
        $valsByTimestamp = array();
        for($step = $start; $step < $end; $step += $resolution){
            $valsByTimestamp[$step] = $padding;
        }

        foreach($valsByTimestampIn as $second => $value){
            if(isset($valsByTimestamp[$second])){
                $valsByTimestamp[$second] = $value;
            }
        }

        return $valsByTimestamp;
    }

    /**
     * Aggregates time within the given timezone (but returns in timestamps).
     *
     * @param $valsByTimestampIn array
     * @param $resolution        int
     * @param $aggregation       int
     * @param $timezoneOffset    int
     * @param $padding           mixed
     * @return array
     */
    public function rollUpTime($valsByTimestampIn, $resolution, $aggregation, $timezoneOffset, $padding){
        $valsByTimestamp = array();
        $nValsByTimestamp = array();
        foreach($valsByTimestampIn as $timestamp => $value){
            if($value === $padding){
                continue;
            }

            /*
             * Here we find the unix time offset by the the timezone
             * We clamp that time down to the desired resolution, and
             * lastly move the clamped time back to unix time
             */
            $timezoneTime = $timestamp + $timezoneOffset;
            $roundedTime = $timezoneTime - ($timezoneTime % $resolution);
            $roundedTime -= $timezoneOffset;

            if($aggregation == Aggregation::SUM){
                isset($valsByTimestamp[$roundedTime]) ?
                    $valsByTimestamp[$roundedTime] += $value :
                    $valsByTimestamp[$roundedTime] = $value;
            }elseif($aggregation == Aggregation::AVG){
                isset($valsByTimestamp[$roundedTime]) ?
                    $valsByTimestamp[$roundedTime] += $value :
                    $valsByTimestamp[$roundedTime] = $value;
                isset($nValsByTimestamp[$roundedTime]) ?
                    $nValsByTimestamp[$roundedTime]++ :
                    $nValsByTimestamp[$roundedTime] = 1;
            }elseif($aggregation == Aggregation::MAX){
                isset($valsByTimestamp[$roundedTime]) ?
                    $valsByTimestamp[$roundedTime] = max($valsByTimestamp[$roundedTime], $value) :
                    $valsByTimestamp[$roundedTime] = $value;
            }elseif($aggregation == Aggregation::MIN){
                isset($valsByTimestamp[$roundedTime]) ?
                    $valsByTimestamp[$roundedTime] = min($valsByTimestamp[$roundedTime], $value) :
                    $valsByTimestamp[$roundedTime] = $value;
            }
        }

        if($aggregation == Aggregation::AVG){
            foreach($valsByTimestamp as $second => $value){
                $valsByTimestamp[$second] = $value/$nValsByTimestamp[$second];
            }
        }

        return $valsByTimestamp;
    }

    /**
     * This function takes an array of valsByTimestamp arrays (timestamp => value),
     * and merges them into one, using the specified aggregation method.
     *
     * It assumes that the timestamp keys are equal for all the arrays.
     *
     * Ignores all pad values in the calculations! The result needs to be repadded.
     *
     * @param $series      array    Array of valsByTimestamp
     * @param $aggregation int
     * @param $padding     mixed
     *
     * @return array
     */
    public function rollUpAcross($series, $aggregation, $padding = false){
        $valsByTimestamp = array();
        $nValsByTimestamp = array();
        for($i = 0; $i < count($series); $i++){
            foreach($series[$i] as $timestamp => $value){
                if($value === $padding){
                    continue;
                }

                if($aggregation == Aggregation::SUM){
                    isset($valsByTimestamp[$timestamp]) ?
                        $valsByTimestamp[$timestamp] += $value :
                        $valsByTimestamp[$timestamp] = $value;
                }elseif($aggregation == Aggregation::AVG){
                    isset($valsByTimestamp[$timestamp]) ?
                        $valsByTimestamp[$timestamp] += $value :
                        $valsByTimestamp[$timestamp] = $value;
                    isset($nValsByTimestamp[$timestamp]) ?
                        $nValsByTimestamp[$timestamp]++ :
                        $nValsByTimestamp[$timestamp] = 1;
                }elseif($aggregation == Aggregation::MAX){
                    isset($valsByTimestamp[$timestamp]) ?
                        $valsByTimestamp[$timestamp] = max($valsByTimestamp[$timestamp], $value) :
                        $valsByTimestamp[$timestamp] = $value;
                }elseif($aggregation == Aggregation::MIN){
                    isset($valsByTimestamp[$timestamp]) ?
                        $valsByTimestamp[$timestamp] = min($valsByTimestamp[$timestamp], $value) :
                        $valsByTimestamp[$timestamp] = $value;
                }
            }
        }

        if($aggregation == Aggregation::AVG){
            foreach($valsByTimestamp as $timestamp => $value){
                $valsByTimestamp[$timestamp] /= $nValsByTimestamp[$timestamp];
            }
        }

        return $valsByTimestamp;
    }

    /**
     * @param $start      DateTime
     * @param $end        DateTime
     * @param $resolution int
     *
     * Clamps the datetimes down to the nearest resolution step.
     * Also move the end datetime to the next step, so that
     * the entire step is included in the result.
     *
     * @throws \Exception
     */
    protected function normalizeDatetimes(&$start, &$end, $resolution){
        if($resolution == Resolution::MINUTE){
            $start = DateTimeHelper::clampToMinute($start);
            $end = DateTimeHelper::clampToMinute($end);
            $end->add(DateInterval::createFromDateString('1 minute'));
        }else if($resolution == Resolution::FIVE_MINUTES){
            $start = DateTimeHelper::clampToFiveMin($start);
            $end = DateTimeHelper::clampToFiveMin($end);
            $end->add(DateInterval::createFromDateString('5 minutes'));
        }else if($resolution == Resolution::FIFTEEEN_MINUTES){
            $start = DateTimeHelper::clampToFifteenMin($start);
            $end = DateTimeHelper::clampToFifteenMin($end);
            $end->add(DateInterval::createFromDateString('15 minutes'));
        }else if($resolution == Resolution::HOUR){
            $start = DateTimeHelper::clampToHour($start);
            $end = DateTimeHelper::clampToHour($end);
            $end->add(DateInterval::createFromDateString('1 hour'));
        }else if($resolution == Resolution::DAY){
            $start = DateTimeHelper::clampToDay($start);
            $end = DateTimeHelper::clampToDay($end);
            $end->add(DateInterval::createFromDateString('1 day'));
        }else{
            throw new \Exception('Invalid resolution given');
        }
    }

    /**
     * @param $datetimeFrom DateTime
     * @param $datetimeTo   DateTime
     * @param $nids         array
     * @param $sids         array
     * @param $minCount     int
     * @param $limit        int
     *
     * @return Anomaly[]
     */
    public function getAnomalies(DateTime $datetimeFrom, DateTime $datetimeTo, array $nids = array(), array $sids = array(), $minCount = 1, $limit = 20){
        foreach($nids as &$nid){
            $nid = (string)$nid;
        }

        foreach($sids as &$sid){
            $sid = (string)$sid;
        }

        // First match with datetime and nid/sid we are interested in
        $matchInitial = array();
        if(count($nids) > 0){
            $matchInitial['nid'] = array('$in' => $nids);
        }

        if(count($sids) > 0){
            $matchInitial['sid'] = array('$in' => $sids);
        }

        $matchInitial['mongodate'] = array(
            '$gte' => new MongoDate($datetimeFrom->getTimestamp()),
            '$lte' => new MongoDate($datetimeTo->getTimestamp())
        );

        // Next ensure that everything is sorted by date in ascending order
        $sortOnDate = array(
          'mongodate' => 1
        );

        // Next group by nid/sid, and count the number of anomalies for each such combo
        $groupByNidSidAndDoCount = array(
            '_id' => array('nid' => '$nid', 'sid' => '$sid'), // This is what is aggregated on
            'count' => array('$sum' => 1),
            'anomalies' => array('$push' => array('mongodate' => '$mongodate', 'predicted' => '$predicted', 'actual' => '$actual')),
            'sid' => array('$first' => '$sid'),
            'nid' => array('$first' => '$nid'),
        );

        // Now grab only the combos with a minimum number of anomalies
        $matchCountHigherThanMin = array(
            'count' => array('$gte' => $minCount)
        );

        // Next sort on count in descending order, so the most anomalies appear first in the result
        $sortOnCountDesc = array(
            'count' => -1
        );

        // Lastly project only the items we are interested in
        $project = array(
            '_id' => 0,
            'sid' => 1,
            'nid' => 1,
            'count' => 1,
            'anomalies' => 1
        );

        $res = $this->conn->col('anomalies')->aggregate(array(
            array('$match'   => $matchInitial),
            array('$sort'    => $sortOnDate),
            array('$group'   => $groupByNidSidAndDoCount),
            array('$match'   => $matchCountHigherThanMin),
            array('$sort'    => $sortOnCountDesc),
            array('$limit'   => $limit),
            array('$project' => $project),
        ));

        $result = array();
        foreach($res['result'] as $doc){
            $nid = $doc['nid'];
            $sid = $doc['sid'];
            $count = $doc['count'];

            $anomalies = array();
            foreach($doc['anomalies'] as $subdoc){
                $datetime = new DateTime('@'.$subdoc['mongodate']->sec);
                $datetime->setTimezone($datetimeFrom->getTimezone());
                $cv = new CounterValue($sid, $nid, $datetime, $subdoc['actual']);
                $anomalies[] = new Anomaly($cv, $subdoc['predicted']);
            }

            $result[] = array(
                'nid' => $nid,
                'sid' => $sid,
                'count' => $count,
                'anomalies' => $anomalies
            );
        }

        return $result;
    }

    /**
     * Takes an array of anomalies, and computes
     * an array of states (0 if no anomalies and 1 if at least one anomaly),
     * normalized to the given resolution and over the given time range
     *
     * @param $anomalies   Anomaly[]
     * @param $start       DateTime
     * @param $end         DateTime
     * @param $resolution  int
     *
     * @return array
     * @throws \Exception
     */
    public function getAnomalyStates($anomalies, $start, $end, $resolution){
        $start = clone $start;
        $end = clone $end;
        $this->normalizeDatetimes($start, $end, $resolution);
        $targetTimezone = $start->getTimezone();
        $timezoneOffset = $targetTimezone->getOffset($start);

        $stateByTimestamp = array();
        foreach($anomalies as $anomaly){
            $stateByTimestamp[$anomaly->cv->datetime->getTimestamp()] = 1;
        }

        $stateByTimestamp = $this->rollUpTime($stateByTimestamp, $resolution, Aggregation::MAX, $timezoneOffset, 0);
        return $this->padValues($stateByTimestamp, $start->getTimestamp(), $end->getTimestamp(), $resolution, 0);
    }
}