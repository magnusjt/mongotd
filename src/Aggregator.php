<?php namespace Mongotd;

class Aggregator{
    /** @var  Connection */
    private $conn;

    /** @var  AggregatorDay */
    private $aggregator_day;

    /** @var  AggregatorHour */
    private $aggregator_hour;

    /** @var  AggregatorSubHour */
    private $aggregator_sub_hour;

    public function __construct($conn, $aggregator_day, $aggregator_hour, $aggregator_sub_hour){
        $this->conn                = $conn;
        $this->aggregator_sub_hour = $aggregator_sub_hour;
        $this->aggregator_hour     = $aggregator_hour;
        $this->aggregator_day      = $aggregator_day;
    }

    /**
     * @param $sid             int|string
     * @param $nid             int|string
     * @param $start           \DateTime
     * @param $end             \DateTime
     * @param $resolution      int
     * @param $aggregation     int
     * @param $target_timezone \DateTimeZone
     * @return array
     */
    public function aggregate($sid, $nid, $start, $end, $resolution, $aggregation, $target_timezone){
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
            return $this->aggregator_day->aggregate($cursor, $aggregation, $target_timezone);
        }else if($resolution == Resolution::HOUR){
            return $this->aggregator_hour->aggregate($cursor, $aggregation, $target_timezone);
        }else if($resolution < Resolution::HOUR){
            return $this->aggregator_sub_hour->aggregate($cursor, $resolution, $aggregation, $target_timezone);
        }

        return array();
    }
}