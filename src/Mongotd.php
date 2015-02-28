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
     * Incremental values are scaled to reflect this. Also, space in database is
     * preallocated for values at the interval, and the time for a data point is rounded
     * down to the nearest interval point.
     */
    public function getInserter($interval = 300){
        $gauge_inserter = new GaugeInserter($this->conn, $interval, $this->logger);
        $delta_converter = new DeltaConverter($this->conn, $interval, $this->logger);
        $anomaly_detector = new AnomalyDetector($this->conn);
        return new Inserter($gauge_inserter, $delta_converter, $anomaly_detector);
    }

    /**
     * @return Retriever
     */
    public function getRetriever(){
        $aggregator_sub_hour = new AggregatorSubHour($this->logger);
        $aggregator_hour = new AggregatorHour($this->logger);
        $aggregator_day = new AggregatorDay($this->logger);

        $aggregator = new Aggregator($this->conn, $aggregator_day, $aggregator_hour, $aggregator_sub_hour);
        return new Retriever($this->conn, $aggregator, $this->logger);
    }

    /**
     * Adds indexes to collections. Should be run only once.
     */
    public function ensureIndexes(){
        $this->conn->col('cv_prev')->ensureIndex(array('sid' => 1, 'nid' => 1), array('unique' => true));
        $this->conn->col('cv')->ensureIndex(array('mongodate' => 1, 'sid' => 1, 'nid' => 1), array('unique' => true));
        $this->conn->col('acache')->ensureIndex(array('sid' => 1, 'nid' => 1), array('unique' => true));
    }
}