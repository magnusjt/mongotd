<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;

class DeltaConverter{
    /** @var  Connection */
    private $conn;

    /** @var  LoggerInterface */
    private $logger;

    /** @var int  */
    private $interval;

    /**
     * @param $conn Connection
     * @param $interval int
     * @param LoggerInterface $logger
     */
    public function __construct($conn, $interval, LoggerInterface $logger = NULL){
        $this->conn   = $conn;
        $this->logger = $logger;
        $this->interval = $interval;
    }

    /**
     * @param $cvs CounterValue[]
     *
     * @return CounterValue[]
     */
    public function convert($cvs){
        $cvsDelta = array();
        $col = $this->conn->col('cv_prev');
        $batchUpdate = new \MongoUpdateBatch($col);

        foreach($cvs as $cv){
            $mongodate = new \MongoDate($cv->datetime->getTimestamp());
            $batchUpdate->add(array(
                                    'q' => array('sid' => $cv->sid, 'nid' => $cv->nid),
                                    'u' => array('$set' => array('mongodate' => $mongodate, 'value' => $cv->value)),
                                    'upsert' => true
                                ));

            $doc = $col->findOne(array('sid' => $cv->sid, 'nid' => $cv->nid), array('mongodate' => 1, 'value' => 1));
            if($doc){
                $datetimePrev = new \DateTime('@'.$doc['mongodate']->sec, new \DateTimeZone('UTC'));
                $delta = $this->getDeltaValue($cv->value, $doc['value'], $cv->datetime, $datetimePrev);
                if($delta !== false){
                    $cvsDelta[] = new CounterValue($cv->sid, $cv->nid, $cv->datetime, $delta);
                }
            }
        }

        $batchUpdate->execute(array('w' => 1));

        return $cvsDelta;
    }

    /**
     * @param $value number
     * @param $valuePrev number
     * @param $datetime \DateTime
     * @param $datetimePrev \DateTime
     * @return number|bool
     */
    private function getDeltaValue($value, $valuePrev, $datetime, $datetimePrev){
        $secondsPast = $datetime->diff($datetimePrev)->s;

        $deltaValue = false;
        // Maximum 3 intervals wait. Any longer and the delta has increasing chance of error.
        if($secondsPast > 0 and $secondsPast <= 3*$this->interval){
            $deltaValue = ($value - $valuePrev) * ($this->interval / $secondsPast);
        }

        return $deltaValue;
    }
}