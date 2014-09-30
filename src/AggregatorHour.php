<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;

class AggregatorHour{
    /** @var  LoggerInterface */
    private $logger;

    public function __construct(LoggerInterface $logger){
        $this->logger = $logger;
    }

    /**
     * @param $cursor \MongoCursor
     * @param $aggregation int
     * @param $target_timezone \DateTimeZone
     * @return array
     */
    public function aggregate($cursor, $aggregation, $target_timezone){
        $vals_by_date = array();
        foreach($cursor as $doc){
            $date = new \DateTime();
            $date->setTimezone(new \DateTimeZone('UTC'));
            $date->setTimestamp($doc['mongodate']->sec);

            foreach($doc['hours'] as $hour => $vals_by_minute){
                $date->setTime($hour, 0, 0);
                $date_in_target_timezone = clone $date;
                $date_in_target_timezone->setTimezone($target_timezone);

                $n = 0;
                $sum = 0;
                foreach($vals_by_minute as $val){
                    if(is_numeric($val)){
                        $n++;
                        $sum += $val;
                    }
                }

                if($n > 0){
                    if($aggregation == Aggregation::SUM){
                        $vals_by_date[$date_in_target_timezone->format('Y-m-d H:00:00')] = $sum;
                    }else{
                        $vals_by_date[$date_in_target_timezone->format('Y-m-d H:00:00')] = $sum/$n;
                    }
                }
            }
        }

        return $vals_by_date;
    }
}