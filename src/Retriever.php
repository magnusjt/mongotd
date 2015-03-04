<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;

class Retriever{
    /** @var  Connection */
    private $conn;

    /** @var  LoggerInterface */
    private $logger;

    /**
     * @param $conn Connection
     * @param $logger LoggerInterface
     */
    public function __construct($conn, $logger = NULL){
        $this->conn       = $conn;
        $this->logger     = $logger;
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

        $end->add(\DateInterval::createFromDateString($interval));
        $dateperiod = new \DatePeriod($start, \DateInterval::createFromDateString($interval), $end);

        $cursor = $this->conn->col('cv')->find(array('sid' => $sid, 'nid' => $nid, 'mongodate' => array(
                                                                        '$gte' => new \MongoDate($start->setTimezone(new \DateTimeZone('UTC'))->setTime(0, 0, 0)->getTimestamp()),
                                                                        '$lte' => new \MongoDate($end->setTimezone(new \DateTimeZone('UTC'))->setTime(0, 0, 0)->getTimestamp()))
                                               ), array('mongodate' => 1, 'vals' => 1));

        $valsByDate = $this->getValsByDate($cursor, $aggregation, $clampFunction, $targetTimezone);

        foreach($dateperiod as $datetime){
            $dateStr = $datetime->format('Y-m-d H:i:s');
            if(!isset($valsByDate[$dateStr])){
                $valsByDate[$dateStr] = $padding;
            }
        }

        return $valsByDate;
    }

    private function getValsByDate($cursor, $aggregation, $clampFunction, $targetTimezone){
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
                $datetime = new \DateTime('@'.($timestamp+$seconds));
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
     * @param $datetimeFrom \DateTime
     * @param $datetimeTo   \DateTime
     * @param $minCount     int
     * @param $maxResults   int|bool
     *
     * @return array
     */
    public function getAnomalies($datetimeFrom, $datetimeTo, $minCount = 1, $maxResults = false){
        $cursor = $this->conn->col('anomalies')->find(array(
            'mongodate' => array('$gte' => new \MongoDate($datetimeFrom->getTimestamp()), '$lte' => new \MongoDate($datetimeTo->getTimestamp()))
        ));

        $anomalies = array();
        foreach($cursor as $doc){
            $nid = $doc['nid'];
            $sid = $doc['sid'];
            $key = $nid . '_' . $sid;

            if(!isset($anomalies[$key])){
                $anomalies[$key] = array('nid' => $nid, 'sid' => $sid, 'count' => 0, 'avg' => $doc['predicted']);
            }

            $anomalies[$key]['count']++;
        }

        $anomaliesWithMinCount = array();
        foreach($anomalies as $anomaly){
            if($anomaly['count'] >= $minCount){
                $anomaliesWithMinCount[] = $anomaly;
            }
        }

        foreach($anomaliesWithMinCount as $anomaly){
            $anomaly['avg'] /= $anomaly['count'];
        }

        // Sort from largest to smallest counts
        usort($anomaliesWithMinCount, function($a, $b){
            if($a['count'] > $b['count']){
                return -1;
            }

            return 1;
        });

        if($maxResults !== false){
            return array_slice($anomaliesWithMinCount, 0, $maxResults);
        }else{
            return $anomaliesWithMinCount;
        }
    }
}