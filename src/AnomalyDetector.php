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
    private $seasonLength = 1440;

    public function __construct($conn){
        $this->conn = $conn;
    }

    /**
     * @param $cvs CounterValue[]
     */
    public function detect($cvs){
        $col = $this->conn->col('acache');
        $batchUpdate  = new \MongoUpdateBatch($col);
        foreach($cvs as $cv){
            $cache = new AnomalyCache();
            $cache->sid = $cv->sid;
            $cache->nid = $cv->nid;

            $seasonIndex = $this->getSeasonIndex($cv->datetime);
            $doc = $col->findOne(array('sid' => $cv->sid, 'nid' => $cv->nid));

            if($doc){
                $cache->level = $doc['level'];
                $cache->trend = $doc['trend'];
                $cache->pred = $doc['pred'];
                $cache->val = $doc['val'];
                $cache->anomalies = $doc['anomalies'];
                if(isset($doc['season'][$seasonIndex])){
                    $cache->s = $doc['season'][$seasonIndex]['s'];
                    $cache->dev = $doc['season'][$seasonIndex]['dev'];
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
                                 'season'    => array($seasonIndex => array('s' => $cache->s, 'dev' => $cache->dev))
                             )
                );
            }

            $this->updateAnomalyCache($cache, $cv->value);

            $batchUpdate->add(array(
                'q' => array('sid' => $cv->sid, 'nid' => $cv->nid),
                'u' => array('$set' => array(
                    'level'                            => $cache->level,
                    'trend'                            => $cache->trend,
                    'pred'                             => $cache->pred,
                    'val'                              => $cache->val,
                    'anomalies'                        => $cache->anomalies,
                    'season.' . $seasonIndex . '.s'   => $cache->s,
                    'season.' . $seasonIndex . '.dev' => $cache->dev,
                ))
            ));
        }

        $batchUpdate->execute(array('w' => 1));
    }

    /**
     * @param $datetime \DateTime
     *
     * @return int
     */
    private function getSeasonIndex($datetime){
        $minutes = floor($datetime->getTimestamp() / 60);
        return $minutes % $this->seasonLength;
    }

    /**
     * @param $cache AnomalyCache
     * @param $val   number
     */
    private function updateAnomalyCache($cache, $val){
        $levelPrev    = $cache->level;
        $trendPrev    = $cache->trend;
        $seasonalPrev = $cache->s;
        $devPrev      = $cache->dev;

        $cache->pred = $levelPrev + $trendPrev + $seasonalPrev;
        $cache->val  = $val;

        $upper = $cache->pred + $this->scale * $devPrev;
        $lower = $cache->pred - $this->scale * $devPrev;

        $isAnomaly = false;
        if($val < $lower or $val > $upper){
            $isAnomaly = true;
        }

        $cache->level = $this->alpha * ($val - $seasonalPrev) + (1 - $this->alpha) * ($levelPrev + $trendPrev);
        $cache->trend = $this->beta * ($cache->level - $levelPrev) + (1 - $this->beta) * $trendPrev;
        $cache->s     = $this->gamma * ($val - $levelPrev - $trendPrev) + (1 - $this->gamma) * $seasonalPrev;
        $cache->dev   = $this->gamma * abs($val - $cache->pred) + (1 - $this->gamma) * $devPrev;

        if($isAnomaly){
            $cache->anomalies++;
        }else{
            $cache->anomalies = 0;
        }
    }
}