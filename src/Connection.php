<?php namespace Mongotd;

use \MongoClient;
use \MongoDB;
use \MongoCollection;

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
}