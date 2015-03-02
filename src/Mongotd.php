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
     * @param int $interval Number of seconds between each collected sample. Default 300 seconds = 5 minutes
     * @param bool $doAnomalyDetection
     * @return Inserter Interval is the expected number of seconds between each collected sample.
     *
     * Interval is the expected number of seconds between each collected sample.
     * Incremental values are scaled to represent the change in interval seconds.
     * Space in database is preallocated for values at the interval, and the time for a data point is rounded
     * down to the nearest interval point.
     */
    public function getInserter($interval = Resolution::FIVE_MINUTES, $doAnomalyDetection = false){
        $gaugeInserter = new GaugeInserter($this->conn, $interval, $this->logger);
        $deltaConverter = new DeltaConverter($this->conn, $interval, $this->logger);
        $anomalyDetector = new AnomalyDetector($this->conn);
        return new Inserter($gaugeInserter, $deltaConverter, $anomalyDetector, $doAnomalyDetection);
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
        $this->conn->col('cv_prev')->ensureIndex(array('sid' => 1, 'nid' => 1), array('unique' => true));
        $this->conn->col('cv')->ensureIndex(array('mongodate' => 1, 'sid' => 1, 'nid' => 1), array('unique' => true));
        $this->conn->col('acache')->ensureIndex(array('sid' => 1, 'nid' => 1), array('unique' => true));

        # Expire data after some time
        $this->conn->col('cv_prev')->ensureIndex(array("mongodate" => 1), array('expireAfterSeconds' => 60*60*24*1));
        $this->conn->col('cv')->ensureIndex(array("mongodate" => 1), array('expireAfterSeconds' => 60*60*24*120));
    }
}