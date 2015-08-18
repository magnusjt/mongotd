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

    /**
     * @param $sid         string
     * @param $nid         string
     * @param $start       \DateTime
     * @param $end         \DateTime
     * @param $resolution  int
     * @param $aggregation int
     * @param $padding     mixed
     *
     * @return array
     * @throws \Exception
     */
    public function getByTimestamp($sid, $nid, $start, $end, $resolution = Resolution::FIFTEEEN_MINUTES, $aggregation = Aggregation::SUM, $padding = false){
        $start = clone $start;
        $end = clone $end;
        $this->normalizeDatetimes($start, $end, $resolution);
        $targetTimezone = $start->getTimezone();
        $timezoneOffset = $targetTimezone->getOffset($start);

        $startMongo = clone $start;
        $endMongo = clone $end;
        $startMongo->setTimezone(new DateTimeZone('UTC'))->setTime(0,0,0);
        $endMongo->setTimezone(new DateTimeZone('UTC'))->setTime(0,0,0);

        $match = array(
            'sid' => (string)$sid,
            'nid' => (string)$nid,
            'mongodate' => array(
                '$gte' => new MongoDate($startMongo->getTimestamp()),
                '$lte' => new MongoDate($endMongo->getTimestamp())
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

        $valsByTimestamp = $this->aggregateTime($valsByTimestamp, $resolution, $aggregation, $timezoneOffset, $padding);
        return $this->padValues($valsByTimestamp, $start->getTimestamp(), $end->getTimestamp(), $resolution, $padding);
    }

    /**
     * @param $sid         string
     * @param $nid         string
     * @param $start       \DateTime
     * @param $end         \DateTime
     * @param $resolution  int
     * @param $aggregation int
     * @param $padding     mixed
     *
     * @return array
     * @throws \Exception
     */
    public function get($sid, $nid, $start, $end, $resolution = Resolution::FIFTEEEN_MINUTES, $aggregation = Aggregation::SUM, $padding = false){
        $valsByTimestamp = $this->getByTimestamp($sid, $nid, $start, $end, $resolution, $aggregation, $padding);
        $valsByDate = $this->valsByTimestampToValsByDateStr($valsByTimestamp, $start->getTimezone());
        return $valsByDate;
    }

    /**
     * This function allows you to retrieve dataseries based on a formula, and a set of node ids.
     *
     * Steps to do the aggregations:
     * 1. The formula is calculated for each node. The formula is calculated at a certain resolution.
     *    The result of the formula is then aggregated further up to another resolution.
     * 2. Aggregation is performed over the node ids
     * 3. The final aggregation is done up to the desired end resolution.
     *
     *
     * @param $formula              string    KPI formula
     * @param $nids                 string[]  Node ids to aggregate
     * @param $start                DateTime
     * @param $end                  DateTime
     * @param $resultResolution     int       Resolution of the end result
     * @param $resultAggregation    int       Aggregation up to the end result
     * @param $formulaResolution    int       Resolution at which the formula is calculated
     * @param $formulaAggregation   int       Aggregation of the formula result
     * @param $nodeResolution       int       Resolution at which the nodes are aggregated
     * @param $nodeAggregation      int       Aggregation of the nodes
     * @param $padding              mixed     Padding value for missing data
     *
     *
     * @return array
     * @throws \Exception
     */
    public function getAdvanced(
        $formula,
        array $nids,
        DateTime $start,
        DateTime $end,
        $resultResolution = Resolution::FIVE_MINUTES,
        $resultAggregation = Aggregation::SUM,
        $formulaResolution = Resolution::FIVE_MINUTES,
        $formulaAggregation = Aggregation::SUM,
        $nodeResolution = Resolution::FIVE_MINUTES,
        $nodeAggregation = Aggregation::SUM,
        $padding = false
    ){
        if($resultResolution < $nodeResolution){
            throw new \Exception('End result resolution must be equal or higher than the node resolution');
        }
        if($nodeResolution < $formulaResolution){
            throw new \Exception('Node resolution must be equal or higher than the formula resolution');
        }

        $start = clone $start;
        $end = clone $end;
        $this->normalizeDatetimes($start, $end, $resultResolution);
        $targetTimezone = $start->getTimezone();
        $timezoneOffset = $targetTimezone->getOffset($start);

        $series = array();
        foreach($nids as $nid){
            $series[] = $this->getFormulaByTimestamp($formula, $nid, $start, $end, $nodeResolution, $formulaResolution, $formulaAggregation, $padding);
        }

        $valsByTimestamp = $this->aggregateAcross($series, $nodeAggregation, $padding);
        $valsByTimestamp = $this->aggregateTime($valsByTimestamp, $resultResolution, $resultAggregation, $timezoneOffset, $padding);
        $valsByTimestamp = $this->padValues($valsByTimestamp, $start->getTimestamp(), $end->getTimestamp(), $resultResolution, $padding);
        return $this->valsByTimestampToValsByDateStr($valsByTimestamp, $targetTimezone);
    }

    /**
     * Same as getFormulaByTimestamp, except this one returns
     * with keys as date strings in the same timezone as the input datetime
     *
     * @param $formula           string      KPI syntax is regular arithmetic. Variables: [sid=1,agg=1]
     * @param $nid               string
     * @param $start             DateTime
     * @param $end               DateTime
     * @param $resultResolution  int
     * @param $formulaResolution int
     * @param $resultAggregation int
     * @param $padding           bool
     *
     * @return array
     * @throws \Exception
     */
    public function getFormula(
        $formula,
        $nid,
        DateTime $start,
        DateTime $end,
        $resultResolution = Resolution::FIVE_MINUTES,
        $resultAggregation = Aggregation::SUM,
        $formulaResolution = Resolution::FIVE_MINUTES,
        $padding = false
    ){
        $valsByTimestamp = $this->getFormulaByTimestamp($formula, $nid, $start, $end, $resultResolution, $resultAggregation, $formulaResolution, $padding);
        return $this->valsByTimestampToValsByDateStr($valsByTimestamp, $start->getTimezone());
    }

    /**
     * This function finds a data series based on a formula. In order to do this,
     * a separate resolution needs to be specified at which the formula will be calculated.
     * Every variable used in the formula will be aggregated to this resolution before calculating.
     * The aggregation used in this case must be specified within the variables.
     *
     * After the calculation is done, the data series is further aggregated to the desired result resolution.
     *
     * @param $formula           string      KPI syntax is regular arithmetic. Variables: [sid=1,agg=1]
     * @param $nid               string
     * @param $start             DateTime
     * @param $end               DateTime
     * @param $resultResolution  int
     * @param $formulaResolution int
     * @param $resultAggregation int
     * @param $padding           bool
     *
     * @return array
     * @throws \Exception
     */
    public function getFormulaByTimestamp(
        $formula,
        $nid,
        DateTime $start,
        DateTime $end,
        $resultResolution = Resolution::FIVE_MINUTES,
        $resultAggregation = Aggregation::SUM,
        $formulaResolution = Resolution::FIVE_MINUTES,
        $padding = false
    )
    {
        if ($resultResolution < $formulaResolution) {
            throw new \Exception('End result resolution must be equal or higher than the formula resolution');
        }

        $start = clone $start;
        $end = clone $end;
        $this->normalizeDatetimes($start, $end, $resultResolution);
        $targetTimezone = $start->getTimezone();
        $timezoneOffset = $targetTimezone->getOffset($start);

        $this->astEvaluator->setPaddingValue($padding);
        $this->astEvaluator->setVariableEvaluatorCallback(function ($options) use ($nid, $start, $end, $formulaResolution, $padding) {
            if (!isset($options['sid'])) {
                throw new \Exception('sid was not specified in variable. Need this to determine which sensor to get for the calculation of the formula.');
            }
            if (!isset($options['agg'])) {
                throw new \Exception('agg was not specified in variable. Need this in order to aggregate up to the correct resolution before calculating formula.');
            }

            return $this->getByTimestamp($options['sid'], $nid, $start, $end, $formulaResolution, $options['agg'], $padding);
        });

        $astNode = $this->kpiParser->parse($formula);
        $valsByTimestamp = $this->astEvaluator->evaluate($astNode);
        $valsByTimestamp = $this->aggregateTime($valsByTimestamp, $resultResolution, $resultAggregation, $timezoneOffset, $padding);
        return $this->padValues($valsByTimestamp, $start->getTimestamp(), $end->getTimestamp(), $resultResolution, $padding);
    }

    /**
     * @param $valsByTimestamp array
     * @param $targetTimezone  DateTimeZone
     * @return array
     */
    public function valsByTimestampToValsByDateStr($valsByTimestamp, $targetTimezone){
        $valsByDateStr = array();
        foreach($valsByTimestamp as $timestamp => $value){
            $datetime = new DateTime('@'.($timestamp));
            $datetime->setTimezone($targetTimezone);
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
    public function aggregateTime($valsByTimestampIn, $resolution, $aggregation, $timezoneOffset, $padding){
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
    public function aggregateAcross($series, $aggregation, $padding = false){
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
    private function normalizeDatetimes(&$start, &$end, $resolution){
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
    public function getAnomalyStateArray($anomalies, $start, $end, $resolution){
        $start = clone $start;
        $end = clone $end;
        $this->normalizeDatetimes($start, $end, $resolution);
        $targetTimezone = $start->getTimezone();
        $timezoneOffset = $targetTimezone->getOffset($start);

        $stateByTimestamp = array();
        foreach($anomalies as $anomaly){
            $stateByTimestamp[$anomaly->cv->datetime->getTimestamp()] = 1;
        }

        $stateByTimestamp = $this->aggregateTime($stateByTimestamp, $resolution, Aggregation::MAX, $timezoneOffset, 0);
        $stateByTimestamp = $this->padValues($stateByTimestamp, $start->getTimestamp(), $end->getTimestamp(), $resolution, 0);
        return $this->valsByTimestampToValsByDateStr($stateByTimestamp, $targetTimezone);
    }
}