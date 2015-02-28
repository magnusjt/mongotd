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
     * @return Inserter
     *
     * Interval is the expected number of seconds between each collected sample.
     * Incremental values are scaled to represent the change in interval seconds.
     * Space in database is preallocated for values at the interval, and the time for a data point is rounded
     * down to the nearest interval point.
     */
    public function getInserter($interval = Resolution::FIVE_MINUTES){
        $gaugeInserter = new GaugeInserter($this->conn, $interval, $this->logger);
        $deltaConverter = new DeltaConverter($this->conn, $interval, $this->logger);
        $anomalyDetector = new AnomalyDetector($this->conn);
        return new Inserter($gaugeInserter, $deltaConverter, $anomalyDetector);
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
    }

    public function pruneOldData(){
        $this->conn->col('cv_prev')->remove(array(
            'mongodate' => array(
                '$lt' => new \MongoDate((new \DateTime('-1 day'))->getTimestamp())
            )
        ));
    }
}