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
     * @param $datetimeEnd           \DateTime
     * @param $nDays                 int
     * @param $windowLengthInSeconds int
     *
     * @return \number[]
     * @throws \Exception
     */
    protected function getValsWithinWindows($nid, $sid, $datetimeEnd, $nDays, $windowLengthInSeconds){
        if($windowLengthInSeconds >= 86400){
            throw new \Exception('Window length must be less than 86400 seconds');
        }

        $datetimeEnd = clone $datetimeEnd;
        $datetimeStart = clone $datetimeEnd;
        $datetimeStart->sub(\DateInterval::createFromDateString($nDays . ' days'));
        $datetimeStart->sub(\DateInterval::createFromDateString($windowLengthInSeconds . ' seconds'));

        $tsStart = $datetimeStart->getTimestamp();
        $tsEnd = $datetimeEnd->getTimestamp();
        $mongodateStart = new \MongoDate($tsStart-$tsStart%86400);
        $mongodateEnd = new \MongoDate($tsEnd-$tsEnd%86400);

        $cursor = $this->conn->col('cv')->find(array(
           'mongodate' => array('$gte' => $mongodateStart, '$lte' => $mongodateEnd),
           'nid' => $nid,
           'sid' => $sid,
           'vals' => array('$ne' => null)
        ), array('vals' => 1, 'mongodate' => 1));

        $windowEndPosition = $tsEnd%86400;
        $offset = $windowEndPosition - $windowLengthInSeconds;

        $vals = array();
        foreach($cursor as $doc){
            $tsDay = $doc['mongodate']->sec;
            foreach($doc['vals'] as $second => $val){
                if($val === null){
                    continue;
                }

                $secondOffset = ($second - $offset)%86400;
                if($secondOffset >= 0 and
                   $secondOffset <= $windowLengthInSeconds and
                   $second + $tsDay >= $tsStart and
                   $second + $tsDay <= $tsEnd
                ){
                    $vals[] = $val;
                }
            }
        }

        return $vals;
    }
}