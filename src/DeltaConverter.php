<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;

class DeltaConverter{
    /** @var  Connection */
    private $conn;

    /** @var  LoggerInterface */
    private $logger;

    /** @var int  */
    private $intervalInSeconds = 300;

    /**
     * @param $conn Connection
     * @param LoggerInterface $logger
     */
    public function __construct($conn, LoggerInterface $logger = NULL){
        $this->conn = $conn;
        $this->logger = $logger;
    }

    /**
     * @param int $intervalInSeconds Determines what interval the delta-calc will be scaled for
     */
    public function setInterval($intervalInSeconds = 300){
        $this->intervalInSeconds = $intervalInSeconds;
    }

    /**
     * @param $cvs CounterValue[]
     *
     * @return CounterValue[]
     */
    public function convert($cvs){
        $cvsDelta = array();
        $col = $this->conn->col('cv_prev');

        foreach($cvs as $cv){
            $timestamp = $cv->datetime->getTimestamp();
            $mongodate = new \MongoDate($timestamp);

            $doc = $col->findOne(array('sid' => $cv->sid, 'nid' => $cv->nid), array('mongodate' => 1, 'value' => 1));
            if($doc){
                $delta = $this->getDeltaValue($cv->value, $doc['value'], $timestamp, $doc['mongodate']->sec);
                if($delta !== false){
                    $cvsDelta[] = new CounterValue($cv->sid, $cv->nid, $cv->datetime, $delta);
                }
            }

            $col->update(array('sid' => $cv->sid, 'nid' => $cv->nid),
                         array('$set' => array('mongodate' => $mongodate, 'value' => $cv->value)),
                         array('upsert' => true));
        }

        return $cvsDelta;
    }

    /**
     * @param $value number
     * @param $valuePrev number
     * @param $timestamp int
     * @param $timestampPrev int
     * @return number|bool
     *
     * Returns difference between current and previous value.
     * Scales the result so it becomes the difference as it would
     * have been during a single interval. If the values are spaced
     * more than 3 intervals apart, false is returned to avoid
     * using delta-calculations between values too far apart.
     */
    private function getDeltaValue($value, $valuePrev, $timestamp, $timestampPrev){
        $secondsPast = $timestamp - $timestampPrev;

        $deltaValue = false;
        if($secondsPast > 0 and $secondsPast <= 3*$this->intervalInSeconds){
            $deltaValue = ($value - $valuePrev) * ($this->intervalInSeconds / $secondsPast);
        }

        return $deltaValue;
    }
}