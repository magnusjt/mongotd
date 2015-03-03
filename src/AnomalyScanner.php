<?php
namespace Mongotd;

abstract class AnomalyScanner{
    /** @var Connection  */
    protected $conn;

    /**
     * @param $conn Connection
     */
    public function __construct($conn){
        $this->conn = $conn;
    }

    /**
     * @param $cvs CounterValue[]
     *
     * @return
     */
    abstract public function scan($cvs);

    /**
     * @param $nid       int
     * @param $sid       int
     * @param $datetime  \DateTime
     * @param $predicted number
     * @param $actual    number
     */
    protected function storeAnomaly($nid, $sid, \DateTime $datetime, $predicted, $actual){
        $this->conn->col('anomalies')->insert(array(
            'nid' => $nid,
            'sid' => $sid,
            'predicted' => $predicted,
            'actual' => $actual,
            'mongodate' => new \MongoDate($datetime->getTimestamp())
        ));
    }

    /**
     * @param $nid                   int
     * @param $sid                   int
     * @param $datetimeStart         \DateTime
     * @param $datetimeEnd           \DateTime
     * @param $windowLengthInSeconds int
     * @param $windowEndPosition     int
     *
     * @return number[]
     */
    protected function getValsWithinWindows($nid, $sid, $datetimeStart, $datetimeEnd, $windowLengthInSeconds, $windowEndPosition){
        $tsStart = $datetimeStart->getTimestamp();
        $tsEnd = $datetimeEnd->getTimestamp();
        $mongodateStart = new \MongoDate($tsStart-$tsStart%86400);
        $mongodateEnd = new \MongoDate($tsEnd-$tsEnd%86400);

        $cursor = $this->conn->col('cv')->find(array(
           'mongodate' => array('$gte' => $mongodateStart, '$lte' => $mongodateEnd),
           'nid' => $nid,
           'sid' => $sid,
           'vals' => array('$ne' => null)
        ), array('vals' => 1));

        $offset = 0;
        $windowStartPosition = $windowEndPosition - $windowLengthInSeconds;

        // Move window into positive range if needed
        if($windowStartPosition < 0){
            $offset = -1*$windowStartPosition;
            $windowEndPosition += $offset;
            $windowStartPosition = 0;
        }

        $vals = array();
        foreach($cursor as $doc){
            foreach($doc['vals'] as $second => $val){
                $secondOffset = ($second+$offset)%86400;
                if($val !== null and $secondOffset <= $windowEndPosition and $secondOffset >= $windowStartPosition){
                    $vals[] = $val;
                }
            }
        }

        return $vals;
    }

    /**
     * @param $windowLengthInSeconds int
     * @param $windowEndPosition     int
     *
     * @return string[]
     */
    private function getProjection($windowLengthInSeconds, $windowEndPosition){
        $projection = array();
        for($i = 0; $i < $windowLengthInSeconds; $i++){
            if($windowEndPosition - $i < 0){
                $windowEndPosition = 86400 + $i;
            }
            $projection['vals.' . ($windowEndPosition - $i)] = 1;
        }

        return $projection;
    }

}