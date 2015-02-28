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
     * @param $start      \DateTime
     * @param $end        \DateTime
     * @param $resolution  int
     * @param $aggregation int
     * @param $padding     mixed
     *
     * @return array
     */
    public function get($sid, $nid, $start, $end, $resolution = Resolution::FIFTEEEN_MINUTES, $aggregation = Aggregation::SUM, $padding = false){
        $targetTimezone = $start->getTimezone();

        $start = clone $start;
        $end = clone $end;
        $this->adjustDatetimesToResolution($resolution, $start, $end);

        $valsByDate = $this->aggregator->aggregate($sid, $nid, $start, $end,  $resolution, $aggregation, $targetTimezone);
        $valsByDate = $this->filterAndPadVals($valsByDate, $resolution, $start, $end, $padding);
        return $valsByDate;
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
     * @param $valsByDate array
     * @param $resolution int
     * @param $start \DateTime
     * @param $end \DateTime
     * @param $padding mixed
     * @return array
     */
    private function filterAndPadVals($valsByDate, $resolution, $start, $end, $padding){
        $interval = $resolution . " minutes";
        $endAdjusted = clone $end;
        $endAdjusted->modify("+ " . $interval);
        $dateperiod = new \DatePeriod($start, \DateInterval::createFromDateString($interval), $endAdjusted);

        $valsByDatePadded = array();
        foreach($dateperiod as $datetime){
            $dateStr = $datetime->format('Y-m-d H:i:00');
            if(isset($valsByDate[$dateStr]) and $valsByDate[$dateStr] !== false){
                $valsByDatePadded[$dateStr] = $valsByDate[$dateStr];
            }else{
                $valsByDatePadded[$dateStr] = $padding;
            }
        }

        return $valsByDatePadded;
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
            $startMin = (int)$start->format('i');
            $endMin   = (int)$end->format('i');
            $startMin -= $startMin % $resolution;
            $endMin -= $endMin % $resolution;
            $start->setTime($start->format('H'), $startMin, 0);
            $end->setTime($end->format('H'), $endMin, 0);
        }
    }
}