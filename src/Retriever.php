<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;
use \DateTime;
use \DatePeriod;
use \DateInterval;
use \DateTimeZone;
use \MongoDate;
use \MongoCursor;

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

    public function getAdvanced(
        $formula,
        array $nids,
        DateTime $start,
        DateTime $end,
        $resolution = Resolution::FIVE_MINUTES,
        $aggregation = Aggregation::SUM,
        $formulaResolution = Resolution::FIVE_MINUTES,
        $nodeResolution = Resolution::FIVE_MINUTES,
        $nodeAggregation = Aggregation::SUM,
        $padding
    )
    {
        // TODO: Create function that can aggregate in all the dimensions
    }

    /**
     * @param $sid         int|string
     * @param $nid         int|string
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
            'sid' => $sid,
            'nid' => $nid,
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

        $valsByDate = $this->aggregate($valsByDate, $aggregation, $resolution);

        // Fill in the actual values in the template array
        foreach($valsByDate as $dateStr => $val){
            if(isset($valsByDatePadded[$dateStr])){
                $valsByDatePadded[$dateStr] = $val;
            }
        }

        return $valsByDatePadded;
    }

    /**
     * @param $formula           string      KPI syntax is regular arithmetic. Variables: [sid=1,agg=Sum]
     * @param $nid               string|int
     * @param $start             DateTime
     * @param $end               DateTime
     * @param $resolution        int
     * @param $formulaResolution int
     * @param $aggregation       int
     * @param $padding           bool
     *
     * @return array
     * @throws \Exception
     */
    public function getFormula($formula, $nid, DateTime $start, DateTime $end, $resolution = Resolution::FIFTEEEN_MINUTES, $formulaResolution = Resolution::FIVE_MINUTES, $aggregation = Aggregation::SUM, $padding = false){
        $this->astEvaluator->setVariableEvaluatorCallback(function($options) use($nid, $start, $end, $formulaResolution, $padding){
            return $this->get($options['sid'], $nid, $start, $end, $formulaResolution, $options['agg'], $padding);
        });

        $astNode = $this->kpiParser->parse($formula);
        $valsByDate = $this->astEvaluator->evaluate($astNode);

        return $this->aggregate($valsByDate, $aggregation, $resolution);
    }

    /**
     * @param $valsByDateIn array
     * @param $aggregation  int
     * @param $resolution   int
     *
     * @return array
     * @throws \Exception
     */
    private function aggregate($valsByDateIn, $aggregation, $resolution){
        // Depending on the resolution, the values will be clamped
        // to different datetimes. Determine that here.
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

            if(isset($valsByDate[$dateStr])){
                if($aggregation == Aggregation::SUM){
                    $valsByDate[$dateStr] += $value;
                }else if($aggregation == Aggregation::AVG){
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
                $valsByDate[$dateStr] /= $countsByDate[$dateStr];
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