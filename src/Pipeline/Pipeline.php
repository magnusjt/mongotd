<?php namespace Mongotd\Pipeline;

class Pipeline{
    public function run(array $sequence, $output = null){
        foreach($sequence as $item){
            if(is_array($item)){
                $output = $this->run($item, $output);
            }else{
                $output = $item->run($output);
            }
        }

        return $output;
    }
}