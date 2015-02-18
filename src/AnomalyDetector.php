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

    /**
     * @param $cvs CounterValue[]
     */
    public function detectBatch($cvs){
        $col = $this->conn->col('acache');
        $batch_updater  = new \MongoUpdateBatch($col);
        foreach($cvs as $cv){
            $cache = new AnomalyCache();
            $cache->sid = $cv->sid;
            $cache->nid = $cv->nid;

            $season_index = $this->getSeasonIndex($cv->datetime);
            $doc = $col->findOne(array('sid' => $cv->sid, 'nid' => $cv->nid));

            if($doc){
                $cache->level = $doc['level'];
                $cache->trend = $doc['trend'];
                $cache->pred = $doc['pred'];
                $cache->val = $doc['val'];
                $cache->anomalies = $doc['anomalies'];
                if(isset($doc['season'][$season_index])){
                    $cache->s = $doc['season'][$season_index]['s'];
                    $cache->dev = $doc['season'][$season_index]['dev'];
                }
            }else{
                $col->insert(array(
                                 'sid'       => $cache->sid,
                                 'nid'       => $cache->nid,
                                 'level'     => $cache->level,
                                 'trend'     => $cache->trend,
                                 'pred'      => $cache->pred,
                                 'val'       => $cache->val,
                                 'anomalies' => $cache->anomalies,
                                 'season'    => array($season_index => array('s' => $cache->s, 'dev' => $cache->dev))
                             )
                );
            }

            $this->updateAnomalyCache($cache, $cv->value);

            $batch_updater->add(array(
                'q' => array('sid' => $cv->sid, 'nid' => $cv->nid),
                'u' => array('$set' => array(
                    'level'                            => $cache->level,
                    'trend'                            => $cache->trend,
                    'pred'                             => $cache->pred,
                    'val'                              => $cache->val,
                    'anomalies'                        => $cache->anomalies,
                    'season.' . $season_index . '.s'   => $cache->s,
                    'season.' . $season_index . '.dev' => $cache->dev,
                ))
            ));
        }

        $batch_updater->execute(array('w' => 1));
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

    /**
     * @param $cache AnomalyCache
     * @param $val   number
     */
    private function updateAnomalyCache($cache, $val){
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
}