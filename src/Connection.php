<?php namespace Mongotd;

use \MongoClient;

class Connection{
    private $client = null;

    private $db = null;

    private $host = "localhost";

    private $dbName = "mongotd";

    private $collectionPrefix = "mongotd";

    public function __construct($host = 'localhost', $dbName = 'mongotd', $collectionPrefix = 'mongotd'){
        $this->host              = $host;
        $this->dbName           = $dbName;
        $this->collectionPrefix = $collectionPrefix;

        $this->client = new MongoClient("mongodb://{$this->host}");
        $this->db = $this->client->selectDB($this->dbName);
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