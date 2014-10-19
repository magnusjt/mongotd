<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;

class AggregatorSubHour{
    /** @var  LoggerInterface */
    private $logger;

    public function __construct(LoggerInterface $logger = NULL){
        $this->logger = $logger;
    }

    /**
     * @param $cursor \MongoCursor
     * @param $resolution int
     * @param $aggregation int
     * @param $target_timezone \DateTimeZone
     * @return array
     */
    public function aggregate($cursor, $resolution, $aggregation, $target_timezone){
        $vals_by_date = array();
        foreach($cursor as $doc){
            $date = new \DateTime();
            $date->setTimezone(new \DateTimeZone('UTC'));
            $date->setTimestamp($doc['mongodate']->sec);

            foreach($doc['hours'] as $hour => $vals_by_minute){
                $date->setTime($hour, 0, 0);
                $vals_by_minute_group = $this->groupFromMinuteData($vals_by_minute, $resolution, $aggregation);

                foreach($vals_by_minute_group as $minute => $val){
                    $date->setTime($hour, $minute, 0);
                    $date_in_target_timezone = clone $date;
                    $date_in_target_timezone->setTimezone($target_timezone);
                    $vals_by_date[$date_in_target_timezone->format('Y-m-d H:i:00')] = $val;
                }
            }
        }

        return $vals_by_date;
    }

    /**
     * @param $vals_by_minute array
     * @param $resolution int
     * @param $aggregation int
     * @return array
     */
    private function groupFromMinuteData($vals_by_minute, $resolution, $aggregation){
        if($resolution == Resolution::MINUTE){
            return $vals_by_minute;
        }

        $stats_by_minute_group = array();
        foreach($vals_by_minute as $minute => $val){
            if(is_numeric($val)){
                $minute_group = $minute - $minute % $resolution;

                if(!isset($stats_by_minute_group[$minute_group])){
                    $stats_by_minute_group[$minute_group] = array('n' => 1, 'sum' => $val);
                }else{
                    $stats_by_minute_group[$minute_group]['sum'] += $val;
                    $stats_by_minute_group[$minute_group]['n'] += 1;
                }
            }
        }

        $vals_by_minute_group = array();
        foreach($stats_by_minute_group as $minute => $stats){
            if($aggregation == Aggregation::SUM){
                $vals_by_minute_group[$minute] = $stats['sum'];
            }else{
                $vals_by_minute_group[$minute] = $stats['sum'] / $stats['n'];
            }
        }

        return $vals_by_minute_group;
    }
}