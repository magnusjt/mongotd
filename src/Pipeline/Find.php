<?php namespace Mongotd\Pipeline;

use DateTime;
use DateTimeZone;
use MongoDate;
use Mongotd\Connection;

/**
 * Entry pipe for finding values from database.
 * It rounds the time range to the date values,
 * so make sure to normalize time range before,
 * and normalize values after.
 */
class Find{
    /** @var  Connection */
    private $conn;

    public $sid;
    public $nid;
    public $start;
    public $end;

    public function __construct(Connection $conn, $sid, $nid, DateTime $start, DateTime $end){
        $this->conn = $conn;
        $this->sid = (string)$sid;
        $this->nid = (string)$nid;
        $this->start = clone $start;
        $this->end = clone $end;
        $this->start->setTimezone(new DateTimeZone('UTC'))->setTime(0, 0, 0);
        $this->end->setTimezone(new DateTimeZone('UTC'))->setTime(0, 0, 0);
    }

    public function run(){
        $match = array(
            'sid' => $this->sid,
            'nid' => $this->nid,
            'mongodate' => array(
                '$gte' => new MongoDate($this->start->getTimestamp()),
                '$lte' => new MongoDate($this->end->getTimestamp())
            )
        );

        $cursor = $this->conn->col('cv')->find($match, array('mongodate' => 1, 'vals' => 1));

        $vals = array();
        foreach ($cursor as $doc) {
            $timestamp = $doc['mongodate']->sec;
            foreach ($doc['vals'] as $seconds => $value) {
                if ($value === null) {
                    continue;
                }

                $vals[$timestamp + $seconds] = $value;
            }
        }

        return new Series($vals);
    }
}