<?php namespace Mongotd;

use MongoClient;
use MongoDB;
use MongoCollection;

class Connection{
    /** @var  MongoClient */
    private $client;

    /** @var  \MongoDB */
    private $db;

    /** @var string  */
    private $collectionPrefix = "mongotd";

    public function __construct($host = 'localhost', $dbName = 'mongotd', $collectionPrefix = 'mongotd'){
        $this->client = new MongoClient("mongodb://{$host}");
        $this->collectionPrefix = $collectionPrefix;
        $this->db = $this->client->selectDB($dbName);
    }

    /**
     * @return MongoClient
     */
    public function client(){
        return $this->client;
    }

    /**
     * @return MongoDB
     */
    public function db(){
        return $this->db;
    }

    /**
     * @param $collection string
     *
     * @return MongoCollection
     */
    public function col($collection){
        return $this->db()->selectCollection($this->collectionPrefix . "_" . $collection);
    }

    /**
     * @return string
     */
    public function lastError(){
        $lastError = $this->db()->lastError();
        return $lastError['err'];
    }

    public function createIndexes(
        $expireValuesAfterDays = 120,
        $expireAnomaliesAfterDays = 120,
        $expireDeltaCacheAfter = 1
    ){
        $this->col('cv_prev')  ->createIndex(['sid' => 1, 'nid' => 1],                   ['unique' => true]);
        $this->col('cv')       ->createIndex(['sid' => 1, 'nid' => 1, 'mongodate' => 1], ['unique' => true]);
        $this->col('anomalies')->createIndex(['mongodate' => 1, 'sid' => 1, 'nid' => 1], ['unique' => true]);
        $this->col('hwcache')  ->createIndex(['sid' => 1, 'nid' => 1],                   ['unique' => true]);

        # Expire data after some time
        $this->col('cv_prev')  ->createIndex(["mongodate" => 1], ['expireAfterSeconds' => 60*60*24*$expireDeltaCacheAfter]);
        $this->col('cv')       ->createIndex(["mongodate" => 1], ['expireAfterSeconds' => 60*60*24*$expireValuesAfterDays]);
        $this->col('anomalies')->createIndex(["mongodate" => 1], ['expireAfterSeconds' => 60*60*24*$expireAnomaliesAfterDays]);
    }
}