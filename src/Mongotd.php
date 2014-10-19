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
        $this->ensureIndexes();
    }

    public function getInserter($resolution = Resolution::FIFTEEEN_MINUTES){
        $gauge_inserter = new GaugeInserter($this->conn, $resolution, $this->logger);
        $delta_converter = new DeltaConverter($this->conn, $this->logger);
        $anomaly_detector = new AnomalyDetector($this->conn);
        return new Inserter($gauge_inserter, $delta_converter, $anomaly_detector);
    }

    public function getRetriever(){
        $aggregator_sub_hour = new AggregatorSubHour($this->logger);
        $aggregator_hour = new AggregatorHour($this->logger);
        $aggregator_day = new AggregatorDay($this->logger);

        $aggregator = new Aggregator($this->conn, $aggregator_day, $aggregator_hour, $aggregator_sub_hour);
        return new Retriever($this->conn, $aggregator, $this->logger);
    }

    public function getCurrentAbnormalSids(){
        $anomaly_detector = new AnomalyDetector($this->conn);
        return $anomaly_detector->getAbnormalSids();
    }

    private function ensureIndexes(){
        $this->conn->col('cv_prev')->ensureIndex(array('sid' => 1), array('unique' => true));
        $this->conn->col('cv')->ensureIndex(array('mongodate' => 1, 'sid' => 1), array('unique' => true));
        $this->conn->col('acache')->ensureIndex(array('sid' => 1), array('unique' => true));
       // $this->conn->col('cv')->ensureIndex(array('mongodate' => 1));
       // $this->conn->col('cv')->ensureIndex(array('hours' => 1));
    }
}