<?php namespace Mongotd;

use Psr\Log\LoggerInterface;

class GaugeInserter{
    /** @var  Connection */
    private $conn;

    /** @var  LoggerInterface */
    private $logger;

    /** @var int */
    private $intervalInSeconds = 300;

    /** @var int  */
    private $secondsInADay = 86400;

    public function __construct($conn, LoggerInterface $logger = NULL){
        $this->conn = $conn;
        $this->logger = $logger;
    }

    /**
     * @param int $intervalInSeconds Determines how many value-slots are preallocated
     */
    public function setInterval($intervalInSeconds = 300){
        $this->intervalInSeconds = $intervalInSeconds;
    }

    /**
     * @param $cvs CounterValue[]
     */
    public function insert($cvs){
        $col = $this->conn->col('cv');
        $batchUpdate = new \MongoUpdateBatch($col);

        foreach($cvs as $cv){
            $timestamp = $cv->datetime->getTimestamp();
            $secondsIntoThisDay = $timestamp%$this->secondsInADay;
            $mongodate = new \MongoDate($timestamp - $secondsIntoThisDay);

            $this->preAllocateIfNecessary($col, $cv->sid, $cv->nid, $mongodate);

            $secondsClampedToInterval = $secondsIntoThisDay - $secondsIntoThisDay%$this->intervalInSeconds;
            $batchUpdate->add(array(
                'q' => array('sid' => $cv->sid, 'nid' => $cv->nid, 'mongodate' => $mongodate),
                'u' => array('$set' => array('vals.' . $secondsClampedToInterval => $cv->value))
            ));
        }

        $batchUpdate->execute(array('w' => 1));
    }

    /**
     * @param $col \MongoCollection
     * @param $sid int
     * @param $nid int
     * @param $mongodate \MongoDate
     *
     * Preallocate a days worth of data. Only done once a day, so shouldn't be too taxing.
     * The thing that takes time is checking for existing data.
     */
    private function preAllocateIfNecessary($col, $sid, $nid, $mongodate){
        if($col->find(array('sid' => $sid, 'nid' => $nid, 'mongodate' => $mongodate), array('sid' => 1))->limit(1)->count() == 0){
            $valsPerSec = array();
            for($second = 0; $second < $this->secondsInADay; $second += $this->intervalInSeconds){
                $valsPerSec[$second] = null;
            }

            $col->insert(array(
                             'sid'       => $sid,
                             'nid'       => $nid,
                             'mongodate' => $mongodate,
                             'vals'      => $valsPerSec,
                         ));

        }
    }
}