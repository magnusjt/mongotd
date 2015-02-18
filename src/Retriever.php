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
     * @param $sid         int|string
     * @param $nid         int|string
     * @param $start_in    \DateTime
     * @param $end_in      \DateTime
     * @param $resolution  int
     * @param $aggregation int
     * @param $padding     mixed
     *
     * @return array
     */
    public function get($sid, $nid, $start_in, $end_in, $resolution = Resolution::FIFTEEEN_MINUTES, $aggregation = Aggregation::SUM, $padding = false){
        $target_timezone = $start_in->getTimezone();

        $start = clone $start_in;
        $end = clone $end_in;
        $this->adjustDatetimesToResolution($resolution, $start, $end);

        $vals_by_date = $this->aggregator->aggregate($sid, $nid, $start, $end,  $resolution, $aggregation, $target_timezone);
        $vals_by_date = $this->filterAndPadVals($vals_by_date, $resolution, $start, $end, $padding);
        return $vals_by_date;
    }

    /**
     * @param $threshold int Number of anomalies in a row required for it to be considered abnormal
     *
     * @return array
     */
    public function getCurrentAbnormal($threshold = 3){
        $col = $this->conn->col('acache');

        $cursor = $col->find(array('anomalies' => array('$gte' => $threshold)), array(
            'sid'  => 1,
            'nid'  => 1,
            'pred' => 1,
            'val'  => 1
        ))->sort(array('anomalies' => -1));

        $docs = array();
        foreach($cursor as $doc){
            $docs[] = $doc;
        }

        return $docs;
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