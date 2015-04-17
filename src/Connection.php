<?php namespace Mongotd;

use \MongoClient;

class Connection{
    /** @var  MongoClient */
    private $client;

    /** @var  \MongoDB */
    private $db;

    /** @var string  */
    private $dbName = "mongotd";

    /** @var string  */
    private $collectionPrefix = "mongotd";

    private function __construct($client, $dbName = 'mongotd', $collectionPrefix = 'mongotd'){
        $this->client = $client;
        $this->dbName = $dbName;
        $this->collectionPrefix = $collectionPrefix;
        $this->db = $this->client->selectDB($dbName);
    }

    /**
     * @param string $host
     * @param string $dbName
     * @param string $collectionPrefix
     *
     * @return Connection
     */
    public static function fromParameters($host = 'localhost', $dbName = 'mongotd', $collectionPrefix = 'mongotd'){
        return new Connection(new MongoClient("mongodb://{$host}"), $dbName, $collectionPrefix);
    }

    /**
     * @param MongoClient $mongoClient
     * @param string      $dbName
     * @param string      $collectionPrefix
     *
     * @return Connection
     */
    public static function fromClientConnection(MongoClient $mongoClient, $dbName = 'mongotd', $collectionPrefix = 'mongotd'){
        return new Connection($mongoClient, $dbName, $collectionPrefix);
    }

    /**
     * @return MongoClient
     */
    public function getClient(){
        return $this->client;
    }

    public function dropDb(){
        $this->client->dropDB($this->dbName);
    }

    public function db(){
        return $this->db;
    }

    public function col($collection){
        return $this->db()->selectCollection($this->collectionPrefix . "_" . $collection);
    }
}