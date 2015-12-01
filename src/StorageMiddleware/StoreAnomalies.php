<?php namespace Mongotd\StorageMiddleware;

use MongoDate;
use Mongotd\Anomaly;
use Mongotd\Connection;

class StoreAnomalies{
    public $conn;

    public function __construct(Connection $conn){
        $this->conn = $conn;
    }

    public function run(array $mix){
        /** @var Anomaly $anomaly */
        foreach($mix['anomalies'] as $anomaly){
            $this->conn->col('anomalies')->insert(array(
                'nid' => $anomaly->cv->nid,
                'sid' => $anomaly->cv->sid,
                'predicted' => $anomaly->predicted,
                'actual' => $anomaly->cv->value,
                'mongodate' => new MongoDate($anomaly->cv->datetime->getTimestamp())
            ));
        }

        return $mix['cvs'];
    }
}