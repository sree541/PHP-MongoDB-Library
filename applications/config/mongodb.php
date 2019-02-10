<?php if (!defined('BASEPATH')) exit('No direct script access allowed');


$config['mongodb']['default'] = [
	'settings' => [
		'auth'             => FALSE,
		'debug'            => TRUE,
		'logging'		   => TRUE,	
		'return_as'        => 'array',
		'auto_reset_query' => TRUE
	],

	'connection_string' => '',

	'connection' => [
		'host'          => 'localhost',  // Your host name
		'port'          => '27017', // Your monogdb port number, default 27017
		'user_name'     => 'admin',
		'user_password' => '',
		'db_name'       => '',
		'db_options'    => []
	],

	'driver' => []
];

