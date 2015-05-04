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
    public function __construct(Connection $conn, LoggerInterface $logger = null){
        $this->conn = $conn;
        $this->logger = new Logger($logger);
    }

    /**
     * @return Inserter Interval is the expected number of seconds between each collected sample.
     * @throws \Exception
     */
    public function getInserter(){
        $gaugeInserter = new GaugeInserter($this->conn, $this->logger);
        $deltaConverter = new DeltaConverter($this->conn, $this->logger);
        return new Inserter($gaugeInserter, $deltaConverter, $this->logger);
    }

    /**
     * @return Retriever
     */
    public function getRetriever(){
        $kpiParser = new KpiParser();
        $astEvaluator = new AstEvaluator();
        return new Retriever($this->conn, $this->logger, $kpiParser, $astEvaluator);
    }

    /**
     * @return AnomalyScanner3Sigma
     */
    public function getAnomalyScanner3Sigma(){
        return new AnomalyScanner3Sigma($this->conn, $this->logger);
    }

    /**
     * @return AnomalyScannerHw
     */
    public function getAnomalyScannerHw(){
        return new AnomalyScannerHw($this->conn, $this->logger);
    }

    /**
     * @return AnomalyScannerKs
     */
    public function getAnomalyScannerKs(){
        return new AnomalyScannerKs($this->conn, $this->logger);
    }

    /**
     * Adds indexes to collections. Should be run only once.
     */
    public function ensureIndexes(){
        $this->conn->col('cv_prev')->ensureIndex(array('sid' => 1, 'nid' => 1),                     array('unique' => true));
        $this->conn->col('cv')->ensureIndex(array('mongodate' => 1, 'sid' => 1, 'nid' => 1),        array('unique' => true));
        $this->conn->col('anomalies')->ensureIndex(array('mongodate' => 1, 'sid' => 1, 'nid' => 1), array('unique' => true));
        $this->conn->col('hwcache')->ensureIndex(array('sid' => 1, 'nid' => 1),                     array('unique' => true));

        # Expire data after some time
        $this->conn->col('cv_prev')->ensureIndex(array("mongodate" => 1),   array('expireAfterSeconds' => 60*60*24*1));
        $this->conn->col('cv')->ensureIndex(array("mongodate" => 1),        array('expireAfterSeconds' => 60*60*24*120));
        $this->conn->col('anomalies')->ensureIndex(array("mongodate" => 1), array('expireAfterSeconds' => 60*60*24*120));
    }
}