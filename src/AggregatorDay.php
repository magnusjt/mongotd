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
     * @param $targetTimezone \DateTimeZone
     * @return array
     */
    public function aggregate($cursor, $aggregation, $targetTimezone){
        $statsByDate = array();
        foreach($cursor as $doc){
            $date = new \DateTime();
            $date->setTimezone(new \DateTimeZone('UTC'));
            $date->setTimestamp($doc['mongodate']->sec);

            foreach($doc['hours'] as $hour => $valsByMinute){
                $n = 0;
                $sum = 0;
                foreach($valsByMinute as $val){
                    if(is_numeric($val)){
                        $n++;
                        $sum += $val;
                    }
                }

                if($n > 0){
                    $date->setTime($hour, 0, 0);
                    $dateInTargetTimezone = clone $date;
                    $dateInTargetTimezone->setTimezone($targetTimezone);

                    $dateStr = $dateInTargetTimezone->format('Y-m-d 00:00:00');
                    if(isset($statsByDate[$dateStr])){
                        $statsByDate[$dateStr]['n'] += $n;
                        $statsByDate[$dateStr]['sum'] += $sum;
                    }else{
                        $statsByDate[$dateStr] = array('n' => $n, 'sum' => $sum);
                    }
                }
            }
        }

        $valsByDate = array();
        foreach($statsByDate as $dateStr => $stats){
            if($aggregation == Aggregation::SUM){
                $valsByDate[$dateStr] = $stats['sum'];
            }else{
                $valsByDate[$dateStr] = $stats['sum']/$stats['n'];
            }
        }

        return $valsByDate;
    }
}