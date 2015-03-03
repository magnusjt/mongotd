<?php namespace Mongotd;

use Psr\Log\LoggerInterface;

class Mongotd{
    /** @var Connection  */
    private $conn;

    /** @var LoggerInterface  */
    private $logger;

    /**
     * @param $conn Connection
     * @param $logger LoggerInterface
     */
    public function __construct($conn, LoggerInterface $logger = null){
        $this->conn = $conn;
        $this->logger = $logger;
    }

    /**
     * @param $interval               int     Number of seconds between each collected sample. Default 300 seconds = 5 minutes
     * @param $doAnomalyDetection     bool
     * @param $anomalyDetectionMethod string (ks|hw|sigma)
     *
     * @return Inserter Interval is the expected number of seconds between each collected sample.
     * @throws \Exception
     *
     * Interval is the expected number of seconds between each collected sample.
     * Incremental values are scaled to represent the change in interval seconds.
     * Space in database is preallocated for values at the interval, and the time for a data point is rounded
     * down to the nearest interval point.
     *
     */
    public function getInserter($interval = Resolution::FIVE_MINUTES, $doAnomalyDetection = false, $anomalyDetectionMethod = 'ks'){
        $gaugeInserter = new GaugeInserter($this->conn, $interval, $this->logger);
        $deltaConverter = new DeltaConverter($this->conn, $interval, $this->logger);

        if($anomalyDetectionMethod == 'ks'){
            $scanner = new AnomalyScannerKs($this->conn);
        }else if($anomalyDetectionMethod == 'hw'){
            $scanner = new AnomalyScannerHw($this->conn);
        }else if($anomalyDetectionMethod == 'sigma'){
            $scanner = new AnomalyScanner3Sigma($this->conn);
        }else{
            throw new \Exception('Unknown anomaly scan method');
        }

        return new Inserter($gaugeInserter, $deltaConverter, $doAnomalyDetection, $scanner);
    }

    /**
     * @return Retriever
     */
    public function getRetriever(){
        return new Retriever($this->conn, $this->logger);
    }

    /**
     * Adds indexes to collections. Should be run only once.
     */
    public function ensureIndexes(){
        $this->conn->col('cv_prev')->ensureIndex(array('sid' => 1, 'nid' => 1),              array('unique' => true));
        $this->conn->col('cv')->ensureIndex(array('mongodate' => 1, 'sid' => 1, 'nid' => 1), array('unique' => true));
        $this->conn->col('hwcache')->ensureIndex(array('sid' => 1, 'nid' => 1),              array('unique' => true));

        # Expire data after some time
        $this->conn->col('cv_prev')->ensureIndex(array("mongodate" => 1),   array('expireAfterSeconds' => 60*60*24*1));
        $this->conn->col('cv')->ensureIndex(array("mongodate" => 1),        array('expireAfterSeconds' => 60*60*24*120));
        $this->conn->col('anomalies')->ensureIndex(array("mongodate" => 1), array('expireAfterSeconds' => 60*60*24*120));
    }
}