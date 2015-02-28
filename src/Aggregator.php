<?php namespace Mongotd;

class Aggregator{
    /** @var  Connection */
    private $conn;

    /** @var  AggregatorDay */
    private $aggregatorDay;

    /** @var  AggregatorHour */
    private $aggregatorHour;

    /** @var  AggregatorSubHour */
    private $aggregatorSubHour;

    public function __construct($conn, $aggregatorDay, $aggregatorHour, $aggregatorSubHour){
        $this->conn              = $conn;
        $this->aggregatorSubHour = $aggregatorSubHour;
        $this->aggregatorHour    = $aggregatorHour;
        $this->aggregatorDay     = $aggregatorDay;
    }

    /**
     * @param $sid            int|string
     * @param $nid            int|string
     * @param $start          \DateTime
     * @param $end            \DateTime
     * @param $resolution     int
     * @param $aggregation    int
     * @param $targetTimezone \DateTimeZone
     * @return array
     */
    public function aggregate($sid, $nid, $start, $end, $resolution, $aggregation, $targetTimezone){
        if($resolution < 0){
            throw new \InvalidArgumentException('Resolution must be a positive number');
        }

        $start = clone $start;
        $end   = clone $end;

        if($resolution == Resolution::DAY){
            $start->setTime(0, 0, 0);
            $end->setTime(0, 0, 0);
        }

        $start->setTimezone(new \DateTimeZone('UTC'));
        $end->setTimezone(new \DateTimeZone('UTC'));
        $start->setTime(0, 0, 0);
        $end->setTime(0, 0, 0);

        if($resolution == Resolution::DAY){
            // Daily resolution means the
            // timezone may push the date back one day.
            // Counteract this here.
            $end->modify("+1 day");
        }

        $cursor = $this->conn->col('cv')->find(array(
                                                   'sid'       => $sid,
                                                   'nid'       => $nid,
                                                   'mongodate' => array('$gte' => new \MongoDate($start->getTimestamp()),
                                                                        '$lte' => new \MongoDate($end->getTimestamp()))
                                               ),
                                               array('mongodate' => 1, 'hours' => 1));

        if($resolution == Resolution::DAY){
            return $this->aggregatorDay->aggregate($cursor, $aggregation, $targetTimezone);
        }else if($resolution == Resolution::HOUR){
            return $this->aggregatorHour->aggregate($cursor, $aggregation, $targetTimezone);
        }else if($resolution < Resolution::HOUR){
            return $this->aggregatorSubHour->aggregate($cursor, $resolution, $aggregation, $targetTimezone);
        }

        return array();
    }
}