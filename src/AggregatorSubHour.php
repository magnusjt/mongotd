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
     * @param $targetTimezone \DateTimeZone
     * @return array
     */
    public function aggregate($cursor, $resolution, $aggregation, $targetTimezone){
        $valsByDate = array();
        foreach($cursor as $doc){
            $date = new \DateTime();
            $date->setTimezone(new \DateTimeZone('UTC'));
            $date->setTimestamp($doc['mongodate']->sec);

            foreach($doc['hours'] as $hour => $valsByMinute){
                $date->setTime($hour, 0, 0);
                $valsByMinuteGroup = $this->groupFromMinuteData($valsByMinute, $resolution, $aggregation);

                foreach($valsByMinuteGroup as $minute => $val){
                    $date->setTime($hour, $minute, 0);
                    $dateInTargetTimezone = clone $date;
                    $dateInTargetTimezone->setTimezone($targetTimezone);
                    $valsByDate[$dateInTargetTimezone->format('Y-m-d H:i:00')] = $val;
                }
            }
        }

        return $valsByDate;
    }

    /**
     * @param $valsByMinute array
     * @param $resolution int
     * @param $aggregation int
     * @return array
     */
    private function groupFromMinuteData($valsByMinute, $resolution, $aggregation){
        if($resolution == Resolution::MINUTE){
            return $valsByMinute;
        }

        $statsByMinuteGroup = array();
        foreach($valsByMinute as $minute => $val){
            if(is_numeric($val)){
                $minuteGroup = $minute - $minute % $resolution;

                if(!isset($statsByMinuteGroup[$minuteGroup])){
                    $statsByMinuteGroup[$minuteGroup] = array('n' => 1, 'sum' => $val);
                }else{
                    $statsByMinuteGroup[$minuteGroup]['sum'] += $val;
                    $statsByMinuteGroup[$minuteGroup]['n'] += 1;
                }
            }
        }

        $valsByMinuteGroup = array();
        foreach($statsByMinuteGroup as $minute => $stats){
            if($aggregation == Aggregation::SUM){
                $valsByMinuteGroup[$minute] = $stats['sum'];
            }else{
                $valsByMinuteGroup[$minute] = $stats['sum'] / $stats['n'];
            }
        }

        return $valsByMinuteGroup;
    }
}