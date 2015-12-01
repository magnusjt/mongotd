<?php namespace Mongotd\StorageMiddleware;

use Mongotd\Connection;
use Psr\Log\LoggerInterface;

class Storage{
    public $middleware;

    public function __construct(array $middleware = []){
        $this->middleware = $middleware;
    }

    public function setDefaultMiddleware(Connection $conn, LoggerInterface $logger){
        $this->middleware = [
            new FilterCounterValues(),
            new CalculateDeltas($conn, $logger),
            new InsertCounterValues($conn, $logger),
            new FindAnomaliesUsingSigmaTest($conn),
            new StoreAnomalies($conn)
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