<?php namespace Mongotd;

use Psr\Log\LoggerInterface;

class Storage{
    public $middleware;

    public function __construct(array $middleware = []){
        $this->middleware = $middleware;
    }

    public function setDefaultMiddleware(Connection $conn, LoggerInterface $logger, $interval = 300){
        $this->middleware = [
            new StorageMiddleware\FilterCounterValues(),
            new StorageMiddleware\CalculateDeltas($conn, $logger, $interval),
            new StorageMiddleware\InsertCounterValues($conn),
            new StorageMiddleware\FindAnomaliesUsingSigmaTest($conn),
            new StorageMiddleware\StoreAnomalies($conn)
        ];
    }

    public function addMiddleware($middleware){
        $this->middleware[] = $middleware;
    }

    public function store(array $cvs){
        $output = $cvs;
        foreach($this->middleware as $middleware){
            $output = $middleware->run($output);
        }

        return $output;
    }
}