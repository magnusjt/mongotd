<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;
use \DateTime;
use \DatePeriod;
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

        $series = array();
        foreach($nids as $nid){
            $series[] = $this->getFormula($formula, $nid, $start, $end, $nodeResolution, $formulaResolution, $formulaAggregation, $padding);
        }

        $valsByDate = $this->aggregateAcross($series, $nodeAggregation, $padding);
        return $this->aggregateTime($valsByDate, $resultAggregation, $resultResolution, $padding);
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
        $targetTimezone = $start->getTimezone();

        if($resolution == Resolution::MINUTE){
            $start = DateTimeHelper::clampToMinute($start);
            $end = DateTimeHelper::clampToMinute($end);
            $interval = '1 minute';
        }else if($resolution == Resolution::FIVE_MINUTES){
            $start = DateTimeHelper::clampToFiveMin($start);
            $end = DateTimeHelper::clampToFiveMin($end);
            $interval = '5 minutes';
        }else if($resolution == Resolution::FIFTEEEN_MINUTES){
            $start = DateTimeHelper::clampToFifteenMin($start);
            $end = DateTimeHelper::clampToFifteenMin($end);
            $interval = '15 minutes';
        }else if($resolution == Resolution::HOUR){
            $start = DateTimeHelper::clampToHour($start);
            $end = DateTimeHelper::clampToHour($end);
            $interval = '1 hour';
        }else if($resolution == Resolution::DAY){
            $start = DateTimeHelper::clampToDay($start);
            $end = DateTimeHelper::clampToDay($end);
            $interval = '1 day';
        }else{
            throw new \Exception('Invalid resolution given');
        }

        $end->add(DateInterval::createFromDateString($interval));
        $dateperiod = new DatePeriod($start, \DateInterval::createFromDateString($interval), $end);

        // Create a template array with padded values
        $valsByDatePadded = array();
        foreach($dateperiod as $datetime){
            $dateStr = $datetime->format('Y-m-d H:i:s');
            $valsByDatePadded[$dateStr] = $padding;
        }

        $match = array(
            'sid' => (string)$sid,
            'nid' => (string)$nid,
            'mongodate' => array(
                '$gte' => new MongoDate($start->setTimezone(new DateTimeZone('UTC'))->setTime(0, 0, 0)->getTimestamp()),
                '$lte' => new MongoDate($end->setTimezone(new DateTimeZone('UTC'))->setTime(0, 0, 0)->getTimestamp())
            )
        );

        $cursor = $this->conn->col('cv')->find($match, array('mongodate' => 1, 'vals' => 1));

        $valsByDate = array();
        foreach($cursor as $doc){
            $timestamp = $doc['mongodate']->sec;
            foreach($doc['vals'] as $seconds => $value){
                if($value === null){
                    continue;
                }

                // Convert to local timezone so date strings are local.
                $datetime = new DateTime('@'.($timestamp+$seconds));
                $datetime->setTimezone($targetTimezone);
                $valsByDate[$datetime->format('Y-m-d H:i:s')] = $value;
            }
        }

        $valsByDate = $this->aggregateTime($valsByDate, $aggregation, $resolution, $padding);

        // Fill in the actual values in the template array
        foreach($valsByDate as $dateStr => $val){
            if(isset($valsByDatePadded[$dateStr])){
                $valsByDatePadded[$dateStr] = $val;
            }
        }

        return $valsByDatePadded;
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
        if($resultResolution < $formulaResolution){
            throw new \Exception('End result resolution must be equal or higher than the formula resolution');
        }

        $this->astEvaluator->setPaddingValue($padding);
        $this->astEvaluator->setVariableEvaluatorCallback(function($options) use($nid, $start, $end, $formulaResolution, $padding){
            if(!isset($options['sid'])){
                throw new \Exception('sid was not specified in variable. Need this to determine which sensor to get for the calculation of the formula.');
            }
            if(!isset($options['agg'])){
                throw new \Exception('agg was not specified in variable. Need this in order to aggregate up to the correct resolution before calculating formula.');
            }

            return $this->get($options['sid'], $nid, $start, $end, $formulaResolution, $options['agg'], $padding);
        });

        $astNode = $this->kpiParser->parse($formula);
        $valsByDate = $this->astEvaluator->evaluate($astNode);

        return $this->aggregateTime($valsByDate, $resultAggregation, $resultResolution, $padding);
    }

    /**
     * This function takes an array of valsByDate arrays (dateStr => value),
     * and merges them into one, using the specified aggregation method.
     *
     * It assumes that the dateStr keys are equal for all the arrays
     *
     * @param $series      array    Array of valsByDate
     * @param $aggregation int
     * @param $padding     mixed
     *
     * @return array
     */
    public function aggregateAcross($series, $aggregation, $padding = false){
        $n = count($series);
        if($n == 0){
            return array();
        }

        $valsByDate = $series[0];
        for($i = 1; $i < $n; $i++){
            foreach($series[$i] as $dateStr => $value){
                if($valsByDate[$dateStr] === $padding or $value === $padding){
                    $valsByDate[$dateStr] = $padding;
                    continue;
                }

                if($aggregation == Aggregation::SUM or $aggregation == Aggregation::AVG){
                    $valsByDate[$dateStr] += $value;
                }else if($aggregation == Aggregation::MAX){
                    $valsByDate[$dateStr] = max($valsByDate[$dateStr], $value);
                }else if($aggregation == Aggregation::MIN){
                    $valsByDate[$dateStr] = min($valsByDate[$dateStr], $value);
                }
            }
        }

        if($aggregation == Aggregation::AVG){
            foreach($valsByDate as $dateStr => $value){
                if($value !== $padding){
                    $valsByDate[$dateStr] /= $n;
                }
            }
        }

        return $valsByDate;
    }

    /**
     * This function takes a valsByDate (dateStr => value) array
     * and aggregates up to a more coarse resolution
     *
     * @param $valsByDateIn array
     * @param $aggregation  int
     * @param $resolution   int
     * @param $padding      mixed
     *
     * @return array
     * @throws \Exception
     */
    public function aggregateTime(array $valsByDateIn, $aggregation, $resolution, $padding = false){
        // Depending on the resolution, the values will be clamped
        // to different datetimes.
        if($resolution == Resolution::MINUTE){
            $clampFunction = '\Mongotd\DateTimeHelper::clampToMinute';
        }else if($resolution == Resolution::FIVE_MINUTES){
            $clampFunction = '\Mongotd\DateTimeHelper::clampToFiveMin';
        }else if($resolution == Resolution::FIFTEEEN_MINUTES){
            $clampFunction = '\Mongotd\DateTimeHelper::clampToFifteenMin';
        }else if($resolution == Resolution::HOUR){
            $clampFunction = '\Mongotd\DateTimeHelper::clampToHour';
        }else if($resolution == Resolution::DAY){
            $clampFunction = '\Mongotd\DateTimeHelper::clampToDay';
        }else{
            throw new \Exception('Invalid resolution given');
        }

        $valsByDate = array();
        $countsByDate = array();
        foreach($valsByDateIn as $dateStr => $value){
            $datetime = new DateTime($dateStr);

            // Clamp the datetime so it is unique for the current resolution
            $dateStr = call_user_func($clampFunction, $datetime)->format('Y-m-d H:i:s');

            if($value === $padding){
                $valsByDate[$dateStr] = $padding;
                continue;
            }

            if(isset($valsByDate[$dateStr])){
                if($aggregation == Aggregation::SUM or $aggregation == Aggregation::AVG){
                    $valsByDate[$dateStr] += $value;
                }else if($aggregation == Aggregation::MAX){
                    $valsByDate[$dateStr] = max($valsByDate[$dateStr], $value);
                }else if($aggregation == Aggregation::MIN){
                    $valsByDate[$dateStr] = min($valsByDate[$dateStr], $value);
                }
            }else{
                $valsByDate[$dateStr] = $value;
                $countsByDate[$dateStr] = 0;
            }

            $countsByDate[$dateStr]++;
        }

        if($aggregation == Aggregation::AVG){
            foreach($valsByDate as $dateStr => $value){
                if($value !== $padding){
                    $valsByDate[$dateStr] /= $countsByDate[$dateStr];
                }
            }
        }

        return $valsByDate;
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
}