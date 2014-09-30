<?php namespace Mongotd;

use \Psr\Log\LoggerInterface;

class DeltaConverter{
    /** @var  Connection */
    private $conn;

    /** @var  LoggerInterface */
    private $logger;

    /** @var int  */
    private $resolution;

    public function __construct($conn, $resolution, LoggerInterface $logger = NULL){
        $this->conn   = $conn;
        $this->logger = $logger;
        $this->resolution = $resolution;

        if($this->resolution > Resolution::HOUR){
            $this->resolution = Resolution::HOUR;
        }
    }

    /**
     * @param $vals_by_sid array
     * @param $datetime \DateTime
     *
     * @return array
     */
    public function convert($vals_by_sid, $datetime){
        $col = $this->conn->col('cv_prev');
        $batch_inserter = new \MongoInsertBatch($col);
        $batch_updater  = new \MongoUpdateBatch($col);

        $mongodate_new = new \MongoDate($datetime->getTimestamp());
        $val_and_mongodate_by_sid = $this->getPrevValAndMongoDateBySid($col);

        $vals_by_sid_delta_calculated = array();

        $n_insert_jobs = 0;
        $n_update_jobs = 0;
        foreach($vals_by_sid as $sid => $val){
            if(isset($val_and_mongodate_by_sid[$sid])){
                $batch_updater->add(array(
                                        'q' => array('sid' => $sid),
                                        'u' => array('$set' => array(
                                            'mongodate' => $mongodate_new,
                                            'value'     => $val
                                        )
                                        )
                                    ));
                $n_update_jobs++;

                $datetime_prev = new \DateTime();
                $datetime_prev->setTimezone(new \DateTimeZone('UTC'));
                $datetime_prev->setTimestamp($val_and_mongodate_by_sid[$sid]['mongodate']->sec);
                $val_new = $this->getDeltaValue($val, $val_and_mongodate_by_sid[$sid]['value'], $datetime, $datetime_prev);

                if($val_new !== false){
                    $vals_by_sid_delta_calculated[$sid] = $val_new;
                }
            }else{
                $batch_inserter->add(array('sid'       => $sid,
                                           'mongodate' => $mongodate_new,
                                           'value'     => $val));
                $n_insert_jobs++;
            }

            if($n_insert_jobs > 100){
                $batch_inserter->execute(array('w' => 1));
                $n_insert_jobs = 0;
            }

            if($n_update_jobs > 100){
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

        return $vals_by_sid_delta_calculated;
    }

    /**
     * @param $col \MongoCollection
     * @return array
     */
    private function getPrevValAndMongoDateBySid($col){
        $cursor = $col->find(array(), array('sid' => 1, 'mongodate' => 1, 'value' => 1));
        $val_and_mongodate_by_sid = array();
        foreach($cursor as $doc){
            $val_and_mongodate_by_sid[$doc['sid']] = array('mongodate' => $doc['mongodate'], 'value' => $doc['value']);
        }

        return $val_and_mongodate_by_sid;
    }

    private function getDeltaValue($value, $value_prev, $datetime, $datetime_prev){
        $minutes_past = $datetime->diff($datetime_prev)->i;

        $delta_value = false;
        if($minutes_past > 0 and $minutes_past < 60){
            $delta_value = ($value - $value_prev) * ($this->resolution / $minutes_past);
            if($delta_value < 0){
                $delta_value = false;
            }
        }

        return $delta_value;
    }
}