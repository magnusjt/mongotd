<?php namespace Mongotd\StorageMiddleware;

use Mongotd\Connection;
use Mongotd\CounterValue;
use Psr\Log\LoggerInterface;
use MongoDate;
use MongoUpdateBatch;

class InsertCounterValues{
    /** @var  Connection */
    private $conn;

    /** @var  LoggerInterface */
    private $logger;

    /** @var int */
    private $intervalInSeconds = 300;

    /** @var int  */
    private $secondsInADay = 86400;

    public function __construct(Connection $conn, LoggerInterface $logger){
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
     *
     * @return CounterValue[]
     */
    public function run(array $cvs){
        $col = $this->conn->col('cv');
        $batchUpdate = new MongoUpdateBatch($col);

        foreach($cvs as $cv){
            $timestamp = $cv->datetime->getTimestamp();
            $secondsIntoThisDay = $timestamp%$this->secondsInADay;
            $mongodate = new MongoDate($timestamp - $secondsIntoThisDay);

            $secondsClampedToInterval = $secondsIntoThisDay - $secondsIntoThisDay%$this->intervalInSeconds;
            $batchUpdate->add(array(
                'q' => array('sid' => $cv->sid, 'nid' => $cv->nid, 'mongodate' => $mongodate),
                'u' => array('$set' => array('vals.' . $secondsClampedToInterval => $cv->value))
            ));
        }

        $batchUpdate->execute(array('w' => 1));
        return $cvs;
    }
}