<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;

class AggregatorHour{
    /** @var  LoggerInterface */
    private $logger;

    public function __construct(LoggerInterface $logger = NULL){
        $this->logger = $logger;
    }

    /**
     * @param $cursor \MongoCursor
     * @param $aggregation int
     * @param $targetTimezone \DateTimeZone
     * @return array
     */
    public function aggregate($cursor, $aggregation, $targetTimezone){
        $valsByDate = array();
        foreach($cursor as $doc){
            $date = new \DateTime();
            $date->setTimezone(new \DateTimeZone('UTC'));
            $date->setTimestamp($doc['mongodate']->sec);

            foreach($doc['hours'] as $hour => $valsByMinute){
                $date->setTime($hour, 0, 0);
                $dateInTargetTimezone = clone $date;
                $dateInTargetTimezone->setTimezone($targetTimezone);

                $n = 0;
                $sum = 0;
                foreach($valsByMinute as $val){
                    if(is_numeric($val)){
                        $n++;
                        $sum += $val;
                    }
                }

                if($n > 0){
                    if($aggregation == Aggregation::SUM){
                        $valsByDate[$dateInTargetTimezone->format('Y-m-d H:00:00')] = $sum;
                    }else{
                        $valsByDate[$dateInTargetTimezone->format('Y-m-d H:00:00')] = $sum/$n;
                    }
                }
            }
        }

        return $valsByDate;
    }
}