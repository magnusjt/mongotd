<?php namespace Mongotd;

use Psr\Log\LoggerInterface;

class GaugeInserter{
    /** @var  Connection */
    private $conn;

    /** @var  LoggerInterface */
    private $logger;

    /** @var int  */
    private $resolution;

    public function __construct($conn, $resolution, LoggerInterface $logger = NULL){
        $this->conn       = $conn;
        $this->logger     = $logger;
        $this->resolution = $resolution;

        if($this->resolution > Resolution::HOUR){
            $this->resolution = Resolution::HOUR;
        }
    }

    /**
     * @param $vals_by_sid number[]
     * @param $datetime \DateTime
     */
    public function addBatch($vals_by_sid, $datetime){
        $col            = $this->conn->col('cv');
        $batch_inserter = new \MongoInsertBatch($col);
        $batch_updater  = new \MongoUpdateBatch($col);

        $date_only            = clone $datetime;
        $date_only            = $date_only->setTime(0, 0, 0);
        $mongodate            = new \MongoDate($date_only->getTimestamp());
        $existing_sids_lookup = $this->getExistingSidsLookup($col, $mongodate);
        $empty_doc            = $this->getEmptyDoc($mongodate);

        $hour   = (int)$datetime->format('H');
        $minute = (int)$datetime->format('i');
        $minute -= $minute%$this->resolution; // Clamp minute to nearest minute

        $n_insert_jobs = 0;
        $n_update_jobs = 0;
        foreach($vals_by_sid as $sid => $val){
            $update_item = $this->getUpdateItem($sid, $val, $mongodate, $hour, $minute);
            $batch_updater->add($update_item);
            $n_update_jobs++;

            if(!isset($existing_sids_lookup[$sid])){
                $empty_doc['sid'] = $sid;
                unset($empty_doc['_id']); // Batch inserter adds an ID to the document which we don't want to keep
                $batch_inserter->add($empty_doc);
                $n_insert_jobs++;
            }

            if($n_update_jobs > 100){
                if($n_insert_jobs > 0){
                    $batch_inserter->execute(array('w' => 1));
                    $n_insert_jobs = 0;
                }

                $batch_updater->execute(array('w' => 1));
                $n_update_jobs = 0;
            }
        }

        if($n_insert_jobs > 0){
            $batch_inserter->execute(array('w' => 1));
        }

        if($n_update_jobs > 0){
            $batch_updater->execute(array('w' => 1));
        }
    }

    /**
     * @param $col \MongoCollection
     * @param $mongodate \MongoDate
     * @return array
     */
    private function getExistingSidsLookup($col, $mongodate){
        $cursor = $col->find(array('mongodate' => $mongodate), array('sid' => 1));
        $sids   = array();
        foreach($cursor as $doc){
            $sids[$doc['sid']] = $doc['sid'];
        }

        return $sids;
    }

    /**
     * @param $mongodate \MongoDate
     * @return array
     */
    private function getEmptyDoc($mongodate){
        $minutes = array();
        for($minute = 0; $minute < 60; $minute += $this->resolution){
            $minutes[$minute] = false;
        }

        $hours = array();
        for($hour = 0; $hour < 24; $hour++){
            $hours[$hour] = $minutes;
        }

        return array(
            'sid'       => 0,
            'mongodate' => $mongodate,
            'hours'     => $hours,
        );
    }

    /**
     * @param $sid mixed
     * @param $val number
     * @param $mongodate \MongoDate
     * @param $hour int
     * @param $minute int
     * @return array
     */
    private function getUpdateItem($sid, $val, $mongodate, $hour, $minute){
        $q = array('sid' => $sid, 'mongodate' => $mongodate);
        $u = array(
            '$set' => array('hours.' . $hour . '.' . $minute => $val)
        );

        return array("q" => $q, "u" => $u);
    }
}