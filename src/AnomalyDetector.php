<?php namespace Mongotd;

class AnomalyDetector{
    /** @var  Connection */
    private $conn;

    /** @var float Holt winters parameter */
    private $alpha = 0.05;
    /** @var float Holt winters parameter */
    private $beta = 0.005;
    /** @var float Holt winters parameter */
    private $gamma = 0.1;

    /** @var int How much deviation before we call something an anomaly */
    private $scale = 3;

    /** @var int How many minutes one season lasts */
    private $season_length = 1440;

    public function __construct($conn){
        $this->conn = $conn;
    }

    public function detectBatch($vals_by_sid, $datetime){
        $col            = $this->conn->col('acache');
        $batch_inserter = new \MongoInsertBatch($col);
        $batch_updater  = new \MongoUpdateBatch($col);
        $season_index   = $this->getSeasonIndex($datetime);
        $lookup         = $this->getCacheLookup($col, $season_index);

        $n_insert_jobs = 0;
        $n_update_jobs = 0;
        foreach($vals_by_sid as $sid => $val){
            $update = false;
            if(isset($lookup[$sid])){
                $cache  = $lookup[$sid];
                $update = true;
            }else{
                $cache        = new AnomalyCache();
                $cache->sid   = $sid;
                $cache->level = $val;
            }

            $this->updateCache($cache, $val);

            if($update){
                $batch_updater->add($this->getUpdateItemFromCache($cache, $season_index));
                $n_update_jobs++;
            }else{
                $batch_inserter->add($this->getInsertItemFromCache($cache, $season_index));
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
    }

    /**
     * @param $cache AnomalyCache
     * @param $val   number
     */
    private function updateCache($cache, $val){
        $level_prev    = $cache->level;
        $trend_prev    = $cache->trend;
        $seasonal_prev = $cache->s;
        $dev_prev      = $cache->dev;

        $cache->pred = $level_prev + $trend_prev + $seasonal_prev;
        $cache->val  = $val;

        $upper = $cache->pred + $this->scale * $dev_prev;
        $lower = $cache->pred - $this->scale * $dev_prev;

        $is_anomaly = false;
        if($val < $lower or $val > $upper){
            $is_anomaly = true;
        }

        $cache->level = $this->alpha * ($val - $seasonal_prev) + (1 - $this->alpha) * ($level_prev + $trend_prev);
        $cache->trend = $this->beta * ($cache->level - $level_prev) + (1 - $this->beta) * $trend_prev;
        $cache->s     = $this->gamma * ($val - $level_prev - $trend_prev) + (1 - $this->gamma) * $seasonal_prev;
        $cache->dev   = $this->gamma * abs($val - $cache->pred) + (1 - $this->gamma) * $dev_prev;

        if($is_anomaly){
            $cache->anomalies++;
        }else{
            $cache->anomalies = 0;
        }
    }

    /**
     * @param $col          \MongoCollection
     * @param $season_index number
     *
     * @return array
     */
    private function getCacheLookup($col, $season_index){
        $cursor = $col->find();

        $lookup = array();
        foreach($cursor as $doc){
            $cache            = new AnomalyCache();
            $cache->sid       = $doc['sid'];
            $cache->level     = $doc['level'];
            $cache->anomalies = $doc['anomalies'];
            $cache->trend     = $doc['trend'];
            $cache->pred      = $doc['pred'];
            $cache->val       = $doc['val'];
            $cache->s         = 0;
            $cache->dev       = 0;

            if(isset($doc['season'][$season_index])){
                $cache->s   = $doc['season'][$season_index]['s'];
                $cache->dev = $doc['season'][$season_index]['dev'];
            }

            $lookup[$doc['sid']] = $cache;
        }

        return $lookup;
    }

    /**
     * @param $cache        AnomalyCache
     * @param $season_index number
     *
     * @return array
     */
    private function getUpdateItemFromCache($cache, $season_index){
        $q = array('sid' => $cache->sid);
        $u = array('$set' => array(
            'level'                            => $cache->level,
            'trend'                            => $cache->trend,
            'pred'                             => $cache->pred,
            'val'                              => $cache->val,
            'anomalies'                        => $cache->anomalies,
            'season.' . $season_index . '.s'   => $cache->s,
            'season.' . $season_index . '.dev' => $cache->dev,
        ));
        return array('q' => $q, 'u' => $u);
    }

    /**
     * @param $cache        AnomalyCache
     * @param $season_index number
     *
     * @return array
     */
    private function getInsertItemFromCache($cache, $season_index){
        return array(
            'sid'       => $cache->sid,
            'level'     => $cache->level,
            'trend'     => $cache->trend,
            'pred'      => $cache->pred,
            'val'       => $cache->val,
            'anomalies' => $cache->anomalies,
            'season'    => array(
                $season_index => array(
                    's'   => $cache->s,
                    'dev' => $cache->dev
                )
            )
        );
    }

    /**
     * @param $datetime \DateTime
     *
     * @return int
     */
    private function getSeasonIndex($datetime){
        $minutes = floor($datetime->getTimestamp() / 60);
        return $minutes % $this->season_length;
    }
}