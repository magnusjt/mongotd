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
        //!! NBNBNB: This method is probably just wrong, due to difficulties windowing values spanning border between days
        $tsStart = $datetimeStart->getTimestamp();
        $tsStart -= $windowLengthInSeconds;
        $tsEnd = $datetimeEnd->getTimestamp();
        $mongodateStart = new \MongoDate($tsStart-$tsStart%86400);
        $mongodateEnd = new \MongoDate($tsEnd-$tsEnd%86400);

        $cursor = $this->conn->col('cv')->find(array(
           'mongodate' => array('$gte' => $mongodateStart, '$lte' => $mongodateEnd),
           'nid' => $nid,
           'sid' => $sid,
           'vals' => array('$ne' => null)
        ), array('vals' => 1));

        $upper1 = $windowEndPosition;
        $lower1 = $windowEndPosition - $windowLengthInSeconds;
        $upper2 = $upper1;
        $lower2 = $lower1;

        if($lower2 < 0){
            // Window spans two days, so create one window for each of the two days
            $lower1 = 0;
            $lower2 = 86400 + $lower2;
            $upper2 = 0;
        }

        $vals = array();
        foreach($cursor as $doc){
            foreach($doc['vals'] as $second => $val){
                if($val !== null and
                   (($second >= $lower1 and $second <= $upper1) or ($second >= $lower2 and $second <= $upper2))){
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