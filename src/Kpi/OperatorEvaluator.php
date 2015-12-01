<?php namespace Mongotd\Kpi;

/**
 * A helper class to evaluate operators on array<->array, array<->number, and number<->array
 */
class OperatorEvaluator{
    private $funcs = array(
        '+' => 'self::plus',
        '-' => 'self::minus',
        '*' => 'self::multiply',
        '/' => 'self::divide'
    );

    /**
     * @param $left    array|number
     * @param $right   array|number
     * @param $op      string       +-/*
     * @param $padding mixed        Value signifying that there is missing data
     *
     * @return mixed
     */
    public function evaluate($left, $right, $op, $padding){
        if(is_array($left) and is_array($right)){
            return call_user_func($this->funcs[$op], $left, $right, $padding);
        }elseif(is_array($left)){
            return call_user_func($this->funcs[$op], $left, $this->numToArray($left, $right), $padding);
        }elseif(is_array($right) and !is_array($left)){
            return call_user_func($this->funcs[$op], $this->numToArray($right, $left), $right, $padding);
        }else{
            $res = call_user_func($this->funcs[$op], array($left), array($right), $padding);
            return $res[0];
        }
    }

    private function numToArray($template, $num){
        $res = array();
        foreach($template as $key => $val){
            $res[$key] = $num;
        }

        return $res;
    }

    private function plus($left, $right, $padding){
        foreach($left as $key => $value){
            if($value === $padding or $right[$key] === $padding){
                $left[$key] = $padding;
            }else{
                $left[$key] += $right[$key];
            }
        }

        return $left;
    }

    private function minus($left, $right, $padding){
        foreach($left as $key => $value){
            if($value === $padding or $right[$key] === $padding){
                $left[$key] = $padding;
            }else{
                $left[$key] -= $right[$key];
            }
        }

        return $left;
    }

    private function multiply($left, $right, $padding){
        foreach($left as $key => $value){
            if($value === $padding or $right[$key] === $padding){
                $left[$key] = $padding;
            }else{
                $left[$key] *= $right[$key];
            }
        }

        return $left;
    }

    private function divide($left, $right, $padding){
        foreach($left as $key => $value){
            if($value === $padding or $right[$key] === $padding){
                $left[$key] = $padding;
            }else{
                if($right[$key] == 0){
                    $left[$key] = $padding;
                }else{
                    $left[$key] /= $right[$key];
                }
            }
        }

        return $left;
    }
}