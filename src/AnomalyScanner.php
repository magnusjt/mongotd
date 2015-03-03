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
     * @param $datetime \DateTime
     *
     * @return
     */
    abstract public function scan($cvs, \DateTime $datetime);

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
     * @param $mongodateStart        \MongoDate
     * @param $mongodateEnd          \MongoDate
     * @param $windowLengthInSeconds int
     * @param $windowEndPosition     int
     *
     * @return number[]
     */
    protected function getValsWithinWindow($nid, $sid, \MongoDate $mongodateStart, \MongoDate $mongodateEnd, $windowLengthInSeconds, $windowEndPosition){
        $projection = $this->getProjection($windowLengthInSeconds, $windowEndPosition);

        $cursor = $this->conn->col('cv')->find(array(
           'mongodate' => array('$gte' => $mongodateStart, '$lte' => $mongodateEnd),
           'nid' => $nid,
           'sid' => $sid
        ), $projection);

        $vals = array();
        foreach($cursor as $doc){
            foreach($doc['vals'] as $second => $val){
                if($val !== null){
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
            $projection[] = 'vals.' . ($windowEndPosition - $i);
        }

        return $projection;
    }

}