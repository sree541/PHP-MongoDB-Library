<?php if (!defined('BASEPATH')) exit('No direct script access allowed');

ini_set('memory_limit', '1024M');
ini_set('max_execution_time', '300');

require_once __DIR__ . "/../../vendor/autoload.php";

Class Mongodb
{

	private $CI;
	private $config = array();
	private $param = array();
	private $activate;
	private $connect;
	private $db;
	private $hostname;
	private $port;
	private $database;
	private $username;
	private $password;
	private $debug;
	private $write_concerns = true;
	private $journal;
	private $selects = array();
	private $updates = array();
	private $wheres	= array();
	private $limit	= 0;
	private $offset	= 0;
	private $sorts	= array();
	private $return_as = 'array';
	public $benchmark = array();
	public $options = array();

	//aggregation parameters
	public $match;
	public $group;
	public $sort;
	public $lookup = array();
	public $relation = 'one-many';
	public $agg_order = array();
	public $unwind = FALSE;


	/**
	 *  CONFIG_FILE_NAME - config file name [/config/mongo_db.php].
	 *  CONFIG_ARRAY_NAME - array name in config file. Array contains list of groups
	 *  and other parameters (for example, active_config_group).
	 */
	const CONFIG_FILE_NAME  = 'mongodb';
	const CONFIG_ARRAY_NAME = 'mongodb';

	function __construct(array $config = ['config_group' => 'default'])
	{

		$this->CI = &get_instance();

		$this->CI->load->config(self::CONFIG_FILE_NAME);

		$this->CI->load->library('session');

		$config_array = $this->CI->config->item(self::CONFIG_ARRAY_NAME);

		$config_item = $config_array[$config['config_group']];


		$conf = $config_item['connection'];

		$connection_string = "mongodb://".$conf['host'].":".$conf['port'];

		$this->connection = (new MongoDB\Client($connection_string));

		$this->db = $this->connection->{$conf['db_name']};


		$settings = $config_item['settings'];

		$this->debug = $settings['debug'];

		$this->logging = $settings['logging'];
	

		$this->logger = $this->CI->logging->get_logger('simple');

	}

	public function query($query,$collection,$method='find',$options=array()){

		try{
			if(!empty($options))
				$cursor = $this->db->{$collection}->{$method}($query,$options);
			else
				$cursor = $this->db->{$collection}->{$method}($query);

			$this->_clear();
			
			if(empty($cursor))
				return array();

			$documents = array();

			foreach ($cursor as $doc) {
				$documents[] = json_decode(json_encode($doc),true);
			}

			return $documents;

		}catch(Exception $e){
			$this->error("Error : ".$e->getMessage());
		}

	}

	public function select($includes = array(), $excludes = array()){
		
		$projection  = array();

		foreach ($includes as $value) {
			$projection[$value] = 1;
		}

		foreach ($excludes as $value) {
			$projection[$value] = 0;
		}

		$this->options['projection'] = $projection;
	
		return $this;
		
	}

	public function project($project){
		

		$this->options['projection'] = $project;
	
		return $this;
		
	}


	public function get($collection,$clear = true){

		try{
			$cursor = $this->db->{$collection}->find($this->wheres,$this->options);

			$this->CI->session->set_userdata('last_query',"db.".$collection.".find(".json_encode($this->wheres).",".json_encode($this->options).");");

			if($clear)
				$this->_clear();
			
			if(empty($cursor))
				return array();

			$documents = array();

			foreach ($cursor as $doc) {
				$documents[] = json_decode(json_encode($doc),true);
			}

			return $documents;

		}catch(Exception $e){
			$this->error("Error : ".$e->getMessage());
		}


	}

	public function getOne($collection,$clear = true){

		try{

			$cursor =  $this->db->{$collection}->findOne($this->wheres,$this->options);

			$this->CI->session->set_userdata('last_query',"db.".$collection.".findOne(".json_encode($this->wheres).",".json_encode($this->options).");");

			if($clear)
			$this->_clear();

			if(empty($cursor))
				return array();

			$document = json_decode(json_encode($cursor),true);

			return $document;

		}catch(Exception $e){
			$this->error("Error : ".$e->getMessage());
		}


	}

	public function getDistinct($field,$collection,$clear = true){

		try{
			$cursor = $this->db->{$collection}->distinct($field,$this->wheres,$this->options);

			$this->CI->session->set_userdata('last_query',"db.".$collection.".distinct(".json_encode($this->wheres).",".json_encode($this->options).");");

			if($clear)
				$this->_clear();
			
			if(empty($cursor))
				return array();
			else
				return $cursor;

		}catch(Exception $e){
			$this->error("Error : ".$e->getMessage());
		}


	}

	public function limit(int $limit){

		if($limit>0)
			$this->options['limit'] = $limit;
			
		return $this;
		
	}

	public function offset(int $offset){
		
		$this->options['skip'] = $offset;
			
		return $this;
		
	}

	public function where($where,$value = NULL){
		
		if(is_array($where)){
			$this->wheres = $where;
		}else{
			$this->wheres[$where] = $value;
		}
		return $this;
		
	}

	public function or_where($where = array(),$loop_inside = TRUE){
		
		if(!empty($where)){
			$this->wheres['$or'] = array();

			foreach ($where as $key => $value) {
				if(is_array($value) && $loop_inside){
					foreach ($value as $item) {
						$this->wheres['$or'][] = array($key => $item); 
					}
				}else{
					$this->wheres['$or'][] = array($key => $value); 
				}
				
			}

		}
		return $this;
		
	}

	public function and_where($where = array(),$loop_inside = TRUE){
		
		if(!empty($where)){
			$this->wheres['$and'] = array();

			foreach ($where as $key => $value) {
				if(is_array($value) && $loop_inside){
					foreach ($value as $item) {
						$this->wheres['$and'][] = array($key => $item); 
					}
				}else{
					$this->wheres['$and'][] = array($key => $value); 
				}
			}

		}
		return $this;
		
	}

	public function where_not($field,$value){
		
		$this->wheres[$field] = array('$ne' => $value);
		
		return $this;
		
	}

	public function where_in($field,$list){
		
		
		$this->wheres[$field] = array('$in' => $list);
		
		return $this;
		
	}

	public function where_not_in($field,$list){
		
		
		$this->wheres[$field] = array('$nin' => $list);
		
		return $this;
		
	}

	public function like($field,$value = NULL){
		
		if(!empty($field)){
			if(is_array($field)){
				foreach ($field as $key => $item) {
					$this->wheres[$key] = array('$regex' => new MongoDB\BSON\Regex("^$item",'i'));
				}
			}else{
				$this->wheres[$field] = array('$regex' => new MongoDB\BSON\Regex("^$value",'i'));
			}
		}
		return $this;
		
	}

	public function or_like($fields){
		
		if(is_array($fields)){
			foreach ($fields as $key => $item) {

				$this->wheres['$or'][] = array($key => array('$regex' => new MongoDB\BSON\Regex("^$item",'i'))); 
			}
		}

		return $this;
		
	}

	//Get total number of documnets in the collection

	public function count($collection, $clear = true) 
	{
		try{
			$count = $this->db->{$collection}->count($this->wheres,$this->options);

			if($clear)
			$this->_clear();

			return $count;
		}catch(Exception $e){
			$this->error("Error : ".$e->getMessage());
		}
	}

	public function inc(string $field,int $value = 1){

		$this->updates['$inc'] =  array( $field => $value);

		return $this;

	}

	public function concat($field1,$field2,$glue="",$string = 'string',$return = false){

		$concatinate = ['$concat' => ['$'.$field1,$glue,'$'.$field2]];

		if($return)
			return $concatinate;

		$this->options['projection'][$string] = $concatinate;

		return $this;

	}

	public function insert($collection,$data = array(),$clear = true){


		try{

			$res = $this->db->{$collection}->insertOne($data);

			if($clear)
			$this->_clear();
			
			return $res->getInsertedId();

		}catch(Exception $e){
			$this->error("Error : ".$e->getMessage());
		}

	}

	public function insertMany($collection,$data = array(),$clear = true){

		try{

			$res = $this->db->{$collection}->insertMany($data);

			if($clear)
			$this->_clear();
			
			return $res->getInsertedIds();

		}catch(Exception $e){
			$this->error("Error : ".$e->getMessage());
		}

	}

	public function set($field,$value = NULL){

		if(is_array($field)){

			$this->updates['$set'] = array();
			
			foreach ($field as $key => $value) {
				$this->updates['$set'][$key] = $value;
			}

		}else{
			$this->updates['$set'][$field] = $value;
		}

		return $this;

	}

	public function update($collection,$options = array(),$modified_count = false,$clear = true){

		try{

			$result = $this->db->{$collection}->updateMany($this->wheres,$this->updates);

			$this->CI->session->set_userdata('last_query',"db.".$collection.".update(".json_encode($this->wheres).",".json_encode($this->updates).");");


			if($clear)
			$this->_clear();

			return $modified_count ? $result->getModifiedCount() : $result->getMatchedCount() ;

		}catch(Exception $e){

			$this->error("Error : ".$e->getMessage());
		}

	}

	public function delete($collection,$clear = true){
		try{

			$result = $this->db->{$collection}->deleteMany($this->wheres);

			if($clear)
			$this->_clear();
			
			return $result->getDeletedCount();

		}catch(Exception $e){
			$this->error("Error : ".$e->getMessage());
		}
	}


	public function gte($field,$value){

		$this->wheres[$field] = array( '$gte' => $value);

		return $this;
	}

	public function lte($field,$value){

		$this->wheres[$field] = array( '$lte' => $value);

		return $this;
	}

	public function gt($field,$value){

		$this->wheres[$field] = array( '$gt' => $value);

		return $this;
	}

	public function lt($field,$value){

		$this->wheres[$field] = array( '$lt' => $value);

		return $this;
	}

	public function between($field,$start, $end)
	{
		
		$result = [
			'$gte' =>  $start,
			'$lte' =>  $end
		];

		$this->wheres[$field] = $result;
			
		return $this;
	}

	public function date($date){

		return new MongoDB\BSON\UTCDateTime(strtotime($date)*1000);

	}


	public function date_between($field,$date_start, $date_end,$return = FALSE)
	{
		
		$result = [
			'$gte' =>  new MongoDB\BSON\UTCDateTime(strtotime($date_start)*1000),
			'$lte' =>  new MongoDB\BSON\UTCDateTime(strtotime($date_end)*1000)
		];

		if($field=='')
			return $result;

		$this->wheres[$field] = $result;
			
		return $this;
	}

	//aggregation functions starts..

	public function group($field,$select = array()){

		if(!is_array($field))
			$group =  array('_id'=>'$'.$field,'count' => array('$sum'=> 1));
		else{

			$_id = array();

			foreach ($field as $v) {
				$_id[$v] = '$'.$v;	
			}
			$group =  array('_id'=>$_id,'count' => array('$sum'=> 1));

		}

		if(!empty($select)){
			foreach ($select as $col) {
				$group[$col] = array('$first' => '$'.$col);
			}
		}

		$this->group = $group;

		return $this;

	}

	public function sort($asc, $desc = array()){

		if(!is_array($asc) && !is_array($desc)){
			$this->error("Error: Sort expect array of values!");
		}

		if(is_array($asc)){
			foreach ($asc as $value) {
				$sort_arr[$value] = 1;
			}
		}

		if(is_array($desc)){
			foreach ($desc as $value) {
				$sort_arr[$value] = -1;
			}
		}

		$this->sort = $sort_arr;
		$this->options['sort'] = $sort_arr;

		return $this;
	}

	public function aggorder($order){
		$this->agg_order = $order;
	}

	public function aggregate($collection,$clear = true){

		try{
			$documents = array();


			if(!empty($this->agg_order)){
				if(!empty($this->wheres))
					$aggregate[(int)array_search('match', $this->agg_order)] = ['$match'=>$this->wheres];
				if(!empty($this->group))
					$aggregate[(int)array_search('group', $this->agg_order)] = ['$group'=>$this->group];
				if(!empty($this->lookup))
					$aggregate[(int)array_search('lookup', $this->agg_order)] = ['$lookup'=>$this->lookup];
				if(isset($this->options['limit']))
					$aggregate[(int)array_search('limit', $this->agg_order)] = ['$limit' => $this->options['limit'] ];
				if(isset($this->options['projection']))
					$aggregate[(int)array_search('project', $this->agg_order)] = ['$project' => $this->options['projection'] ];
				if(!empty($this->sort))
					$aggregate[(int)array_search('sort', $this->agg_order)] = ['$sort'=>$this->sort];

				if($this->unwind)
					$aggregate[(int)array_search('unwind', $this->agg_order)] = ['$unwind'=>$this->unwind];

				ksort($aggregate);
			}else{
				if(!empty($this->wheres))
				$aggregate[] = ['$match'=>$this->wheres];
				if(!empty($this->group))
					$aggregate[] = ['$group'=>$this->group];
				if(!empty($this->lookup))
					$aggregate[] = ['$lookup'=>$this->lookup];
				if(isset($this->options['limit']))
					$aggregate[] = ['$limit' => $this->options['limit'] ];
				if(isset($this->options['skip']))
					$aggregate[] = ['$skip' => $this->options['skip'] ];
				if(isset($this->options['projection']))
					$aggregate[] = ['$project' => $this->options['projection'] ];
				if(!empty($this->sort))
					$aggregate[] = ['$sort'=>$this->sort];
				if($this->unwind)
					$aggregate[] = ['$unwind'=>$this->unwind];
			}
			

			$cursor = $this->db->{$collection}->aggregate($aggregate);

			$this->CI->session->set_userdata('last_query',"db.".$collection.".aggregate(".json_encode($aggregate).");");

			if($clear)
			$this->_clear();

			$documents = array();

			foreach ($cursor as $doc) {
				$documents[] = json_decode(json_encode($doc),true);
			}

			return $documents;
			

		}catch(Exception $e){
			$this->error("Error : ".$e->getMessage());
		}

	}

	public function join($collection,$local,$foreign,$as='join',$relation="one-many"){

		try{
			$this->lookup = array(
				'from'=>$collection,
		    	'localField'=>$local,
		    	'foreignField'=>$foreign,
		    	'as'=> $as
			);

			$this->relation = $relation;

			return $this;
		}catch(Exception $e){
			$this->error("Error : ".$e->getMessage());
		}
	}

	public function unwind($as = true,$field = false){
		if(isset($this->lookup['as']) && $as == true)
			$this->unwind = '$'.$this->lookup['as'];
		else if(!$as ){
			$this->unwind = $field;
		}

		return $this;
	}

	public function last_query(){

		return $this->CI->session->has_userdata('last_query') ? $this->CI->session->userdata('last_query') : "No query found!";
	}



	private function _clear()
	{
		$this->selects	= array();
		$this->updates	= array();
		$this->limit	= 0;
		$this->offset	= 0;
		$this->sorts	= array();

		$this->wheres	= array();
		$this->options  = array();

		$this->match 	= array();
		$this->group 	= array();
		$this->sort  	= array();
		$this->lookup   = array();
	}

	private function error(string $text = '', string $method = '', int $code = 500)
	{
		// Log errors only during debug.
		
		if ($this->debug === TRUE) 
		{
			$message = $text;

			// Show method where error occurred.
			if ($method != '')
			{
				$message = "{$method}()$message";
			}

			echo $message;
			//show_error($message, $code);
		}
		if ($this->logging === TRUE) 
		{
			$message = $text;

			if ($method != '')
			{
				$message = "{$method}()$message";
			}

			$this->logger->error($message);

		}
		
		return $code;


	}
}