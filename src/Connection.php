<?php namespace Mongotd;

use \MongoClient;

class Connection{
    private $client = null;

    private $db = null;

    private $host = "localhost";

    private $db_name = "mongotd";

    private $collection_prefix = "mongotd";

    public function __construct($host = 'localhost', $db_name = 'mongotd', $collection_prefix = 'mongotd'){
        $this->host              = $host;
        $this->db_name           = $db_name;
        $this->collection_prefix = $collection_prefix;

        $this->client = new MongoClient("mongodb://{$this->host}");
        $this->db = $this->client->selectDB($this->db_name);
    }

    public function dropDb(){
        $this->client->dropDB($this->db_name);
    }

    public function db(){
        return $this->db;
    }

    public function col($collection){
        return $this->db()->selectCollection($this->collection_prefix . "_" . $collection);
    }
}