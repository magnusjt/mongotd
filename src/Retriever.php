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

    public function __construct(Connection $conn, LoggerInterface $logger){
        $this->conn = $conn;
        $this->logger = $logger;
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
            $clampFunction = '\Mongotd\DateTimeHelper::clampToMinute';
        }else if($resolution == Resolution::FIVE_MINUTES){
            $start = DateTimeHelper::clampToFiveMin($start);
            $end = DateTimeHelper::clampToFiveMin($end);
            $interval = '5 minutes';
            $clampFunction = '\Mongotd\DateTimeHelper::clampToFiveMin';
        }else if($resolution == Resolution::FIFTEEEN_MINUTES){
            $start = DateTimeHelper::clampToFifteenMin($start);
            $end = DateTimeHelper::clampToFifteenMin($end);
            $interval = '15 minutes';
            $clampFunction = '\Mongotd\DateTimeHelper::clampToFifteenMin';
        }else if($resolution == Resolution::HOUR){
            $start = DateTimeHelper::clampToHour($start);
            $end = DateTimeHelper::clampToHour($end);
            $interval = '1 hour';
            $clampFunction = '\Mongotd\DateTimeHelper::clampToHour';
        }else if($resolution == Resolution::DAY){
            $start = DateTimeHelper::clampToDay($start);
            $end = DateTimeHelper::clampToDay($end);
            $interval = '1 day';
            $clampFunction = '\Mongotd\DateTimeHelper::clampToDay';
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

        $cursor = $this->conn->col('cv')->find(array('sid' => $sid, 'nid' => $nid, 'mongodate' => array(
                                                                        '$gte' => new MongoDate($start->setTimezone(new DateTimeZone('UTC'))->setTime(0, 0, 0)->getTimestamp()),
                                                                        '$lte' => new MongoDate($end->setTimezone(new DateTimeZone('UTC'))->setTime(0, 0, 0)->getTimestamp()))
                                               ), array('mongodate' => 1, 'vals' => 1));

        $valsByDate = $this->getValsByDate($cursor, $aggregation, $clampFunction, $targetTimezone);

        // Fill in the actual values in the template array
        foreach($valsByDate as $dateStr => $val){
            if(isset($valsByDatePadded[$dateStr])){
                $valsByDatePadded[$dateStr] = $val;
            }
        }

        return $valsByDatePadded;
    }

    /**
     * @param $cursor MongoCursor
     * @param $aggregation int
     * @param $clampFunction
     * @param $targetTimezone DateTimeZone
     *
     * @return array
     */
    private function getValsByDate($cursor, $aggregation, $clampFunction, DateTimeZone $targetTimezone){
        $valsByDate = array();
        $countsByDate = array();
        foreach($cursor as $doc){
            $timestamp = $doc['mongodate']->sec;
            foreach($doc['vals'] as $seconds => $value){
                if($value === null){
                    continue;
                }

                // Clamp the datetime so it is unique for the current resolution
                // Also convert to local timezone so date strings are local.
                $datetime = new DateTime('@'.($timestamp+$seconds));
                $datetime->setTimezone($targetTimezone);
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