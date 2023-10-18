<?php declare(strict_types=1);

/*
  Copyright (c) 2022, Manticore Software LTD (https://manticoresearch.com)

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License version 2 or any later
  version. You should have received a copy of the GPL license along with this
  program; if you did not, you can find it at http://www.gnu.org/
*/

namespace Manticoresearch\Buddy\Plugin\Show;

use Exception;
use Manticoresearch\Buddy\Core\Error\QueryParseError;
use Manticoresearch\Buddy\Core\Network\Request;
use Manticoresearch\Buddy\Core\Plugin\BasePayload;

/**
 * Request for Backup command that has parsed parameters from SQL
 */
final class Payload extends BasePayload {

	/**
	 * @var string $type Type of show queries, basicly what is followed after show
	 */
	public static string $type = 'full tables';

	/**
	 * @var string $database Manticore single database with no name
	 *  so it does not matter but for future usage maybe we also parse it
	 */
	public string $database = 'Manticore';

	/** @var ?string $table */
	public ?string $table = null;

	/**
	 * @var string $like
	 * 	It contains match pattern from LIKE statement if its presented
	 */
	public string $like = '';

	public string $path;
	public bool $hasCliEndpoint;

	/**
	 * @param Request $request
	 * @return static
	 */
	public static function fromRequest(Request $request): static {
		return match (static::$type) {
			'full tables' => static::fromFullTablesRequest($request),
			'schemas' => static::fromSchemasRequest($request),
			'queries' => static::fromQueriesRequest($request),
			'create table' => static::fromCreateTableRequest($request),
			'index', 'function status' => static::fromSimpleRequest($request),
			'full columns' => static::fromColumnsRequest($request),
			default => throw new Exception('Failed to match type of request: ' . static::$type),
		};
	}

	/**
	 * @param Request $request
	 * @return static
	 */
	protected static function fromFullTablesRequest(Request $request): static {
		$pattern = '#^'
			. 'show full tables'
			. '(\s+from\s+`?(?P<database>([a-z][a-z0-9\_]*))`?)?'
			. '(\s+like\s+\'(?P<like>([^\']+))\')?'
			. '$#ius';

		if (!preg_match($pattern, $request->payload, $m)) {
			throw QueryParseError::create('You have an error in your query. Please, double-check it.');
		}

		$self = new static();
		if ($m['database'] ?? '') {
			$self->database = $m['database'];
		}
		if ($m['like'] ?? '') {
			$self->like = $m['like'];
		}
		[$self->path, $self->hasCliEndpoint] = self::getEndpointInfo($request);
		return $self;
	}

	/**
	 * @param Request $request
	 * @return static
	 */
	protected static function fromSchemasRequest(Request $request): static {
		$self = new static();
		[$self->path, $self->hasCliEndpoint] = self::getEndpointInfo($request);
		return $self;
	}

	/**
	 * @param Request $request
	 * @return static
	 */
	protected static function fromQueriesRequest(Request $request): static {
		$self = new static();
		[$self->path, $self->hasCliEndpoint] = self::getEndpointInfo($request);
		return $self;
	}

	/**
	 * @param Request $request
	 * @return static
	 */
	protected static function fromCreateTableRequest(Request $request): static {
		$pattern = '/(?:SHOW\s+CREATE\s+TABLE\s+`?)([\w]+)(?:`?\.)`?([\w]+)(?:`?)/i';

		if (!preg_match($pattern, $request->payload, $m)) {
			throw QueryParseError::create('You have an error in your query. Please, double-check it.');
		}

		$self = new static();
		$self->database = $m[1];
		$self->table = $m[2];

		[$self->path, $self->hasCliEndpoint] = self::getEndpointInfo($request);
		return $self;
	}

	/**
	 * @param Request $request
	 * @return static
	 */
	protected static function fromSimpleRequest(Request $request): static {
		$self = new static();
		[$self->path, $self->hasCliEndpoint] = self::getEndpointInfo($request);
		return $self;
	}

	/**
	 * @param Request $request
	 * @return static
	 */
	protected static function fromColumnsRequest(Request $request): static {
		$pattern = '/(?:SHOW\s+FULL\s+COLUMNS\s+FROM\s+`?)([\w]+)(?:`?\s+FROM\s+`?)([\w]+)(?:`?\s+LIKE\s+\'%\'?)/i';
		if (!preg_match($pattern, $request->payload, $m)) {
			throw QueryParseError::create('You have an error in your query. Please, double-check it.');
		}
		$self = new static();
		$self->database = $m[2];
		$self->table = $m[1];
		[$self->path, $self->hasCliEndpoint] = self::getEndpointInfo($request);
		return $self;
	}
	/**
	 * @param Request $request
	 * @return bool
	 */
	public static function hasMatch(Request $request): bool {
		if (stripos($request->payload, 'show full tables') === 0) {
			static::$type = 'full tables';
			return true;
		}

		$payloadLen = strlen($request->payload);
		if ($payloadLen === 12 && stripos($request->payload, 'show schemas') === 0) {
			static::$type = 'schemas';
			return true;
		}

		if ($payloadLen === 12 && stripos($request->payload, 'show queries') === 0) {
			static::$type = 'queries';
			return true;
		}

		if (stripos($request->payload, 'show create table') === 0) {
			static::$type = 'create table';
			return true;
		}

		if (stripos($request->payload, 'show full columns') === 0) {
			static::$type = 'full columns';
			return true;
		}

		if (stripos($request->payload, 'show index') === 0
			|| stripos($request->payload, 'show keys') === 0) {
			static::$type = 'index';
			return true;
		}

		if (stripos($request->payload, 'show function status') === 0
		|| stripos($request->payload, 'show procedure status') === 0) {
			static::$type = 'function status';
			return true;
		}

		return false;
	}

	/**
	 * @return string
	 */
	public function getHandlerClassName(): string {
		return __NAMESPACE__ . '\\' . match (static::$type) {
			'full tables' => 'FullTablesHandler',
			'create table' => 'CreateTableHandler',
			'schemas' => 'SchemasHandler',
			'queries' => 'QueriesHandler',
			'index' => 'EmptyHandler',
			'function status' => 'FunctionStatusHandler',
			'full columns' => 'FullColumnsHandler',
			default => throw new Exception('Cannot find handler for request type: ' . static::$type),
		};
	}
}
