<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;

class AggregatorDay{
    /** @var  LoggerInterface */
    private $logger;

    public function __construct(LoggerInterface $logger = NULL){
        $this->logger = $logger;
    }

    /**
     * @param $cursor \MongoCursor
     * @param $aggregation int
     * @param $target_timezone \DateTimeZone
     * @return array
     */
    public function aggregate($cursor, $aggregation, $target_timezone){
        $stats_by_date = array();
        foreach($cursor as $doc){
            $date = new \DateTime();
            $date->setTimezone(new \DateTimeZone('UTC'));
            $date->setTimestamp($doc['mongodate']->sec);

            foreach($doc['hours'] as $hour => $vals_by_minute){
                $n = 0;
                $sum = 0;
                foreach($vals_by_minute as $val){
                    if(is_numeric($val)){
                        $n++;
                        $sum += $val;
                    }
                }

                if($n > 0){
                    $date->setTime($hour, 0, 0);
                    $date_in_target_timezone = clone $date;
                    $date_in_target_timezone->setTimezone($target_timezone);

                    $date_str = $date_in_target_timezone->format('Y-m-d 00:00:00');
                    if(isset($stats_by_date[$date_str])){
                        $stats_by_date[$date_str]['n'] += $n;
                        $stats_by_date[$date_str]['sum'] += $sum;
                    }else{
                        $stats_by_date[$date_str] = array('n' => $n, 'sum' => $sum);
                    }
                }
            }
        }

        $vals_by_date = array();
        foreach($stats_by_date as $date_str => $stats){
            if($aggregation == Aggregation::SUM){
                $vals_by_date[$date_str] = $stats['sum'];
            }else{
                $vals_by_date[$date_str] = $stats['sum']/$stats['n'];
            }
        }

        return $vals_by_date;
    }
}