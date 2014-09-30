<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;

class Retriever{
    /** @var  Connection */
    private $conn;

    /** @var  LoggerInterface */
    private $logger;

    /** @var  Aggregator */
    private $aggregator;

    /**
     * @param $conn Connection
     * @param $aggregator Aggregator
     * @param $logger LoggerInterface
     */
    public function __construct($conn, $aggregator, $logger = NULL){
        $this->conn       = $conn;
        $this->logger     = $logger;
        $this->aggregator = $aggregator;
    }

    /**
     * @param $sid mixed
     * @param $start_in \DateTime
     * @param $end_in \DateTime
     * @param int $resolution int
     * @param int $aggregation int
     * @param $padding mixed
     * @return array
     */
    public function get($sid, $start_in, $end_in, $resolution = Resolution::FIFTEEEN_MINUTES, $aggregation = Aggregation::SUM, $padding = false){
        $target_timezone = $start_in->getTimezone();

        $start = clone $start_in;
        $end = clone $end_in;
        $this->adjustDatetimesToResolution($resolution, $start, $end);

        $vals_by_date = $this->aggregator->aggregate($sid, $start, $end,  $resolution, $aggregation, $target_timezone);
        $vals_by_date = $this->filterAndPadVals($vals_by_date, $resolution, $start, $end, $padding);
        return $vals_by_date;
    }

    /**
     * @param $vals_by_date array
     * @param $resolution int
     * @param $start \DateTime
     * @param $end \DateTime
     * @param $padding mixed
     * @return array
     */
    private function filterAndPadVals($vals_by_date, $resolution, $start, $end, $padding){
        $interval = $resolution . " minutes";
        $end_adjusted = clone $end;
        $end_adjusted->modify("+ " . $interval);
        $dateperiod = new \DatePeriod($start, \DateInterval::createFromDateString($interval), $end_adjusted);

        $vals_by_date_padded = array();
        foreach($dateperiod as $datetime){
            $date_str = $datetime->format('Y-m-d H:i:00');
            if(isset($vals_by_date[$date_str]) and $vals_by_date[$date_str] !== false){
                $vals_by_date_padded[$date_str] = $vals_by_date[$date_str];
            }else{
                $vals_by_date_padded[$date_str] = $padding;
            }
        }

        return $vals_by_date_padded;
    }

    /**
     * @param $resolution int
     * @param $start \DateTime
     * @param $end \DateTime
     */
    private function adjustDatetimesToResolution($resolution, $start, $end){
        if($resolution == Resolution::DAY){
            $start->setTime(0, 0, 0);
            $end->setTime(0, 0, 0);
        }else if($resolution <= Resolution::HOUR){
            $start_min = (int)$start->format('i');
            $end_min   = (int)$end->format('i');
            $start_min -= $start_min % $resolution;
            $end_min -= $end_min % $resolution;
            $start->setTime($start->format('H'), $start_min, 0);
            $end->setTime($end->format('H'), $end_min, 0);
        }
    }
}