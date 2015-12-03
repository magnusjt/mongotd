<?php namespace Mongotd\StorageMiddleware;

use Mongotd\Connection;
use Mongotd\CounterValue;
use Psr\Log\LoggerInterface;
use MongoDate;
use MongoUpdateBatch;

/**
 * Inserts a list of CounterValue into the database.
 * There is one document per nid per sid per day.
 * This document is updated with new values as we
 * go along.
 *
 * There is no pre-allocation, as we assume the
 * WiredTiger storage engine, where pre-allocation
 * is not helpful.
 */
class InsertCounterValues{
    /** @var  Connection */
    private $conn;

    public function __construct(Connection $conn){
        $this->conn = $conn;
    }

    /**
     * @param $cvs CounterValue[]
     *
     * @return CounterValue[]
     */
    public function run(array $cvs){
        if(count($cvs) == 0){
            return $cvs;
        }

        $col = $this->conn->col('cv');
        $batchUpdate = new MongoUpdateBatch($col);

        foreach($cvs as $cv){
            $timestamp = $cv->datetime->getTimestamp();
            $secondsIntoThisDay = $timestamp%86400;
            $mongodate = new MongoDate($timestamp - $secondsIntoThisDay);

            $batchUpdate->add([
                'q' => ['sid' => $cv->sid, 'nid' => $cv->nid, 'mongodate' => $mongodate],
                'u' => ['$set' => ['vals.' . $secondsIntoThisDay => $cv->value]],
                'upsert' => true
            ]);
        }

        $batchUpdate->execute(['w' => 1]);
        return $cvs;
    }
}