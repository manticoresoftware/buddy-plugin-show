<?php declare(strict_types=1);

/*
 Copyright (c) 2023, Manticore Software LTD (https://manticoresearch.com)

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License version 2 or any later
 version. You should have received a copy of the GPL license along with this
 program; if you did not, you can find it at http://www.gnu.org/
 */

namespace Manticoresearch\Buddy\Plugin\Show;

use Manticoresearch\Buddy\Core\Plugin\BaseHandlerWithTableFormatter;
use Manticoresearch\Buddy\Core\Task\Column;
use Manticoresearch\Buddy\Core\Task\Task;
use Manticoresearch\Buddy\Core\Task\TaskResult;
use RuntimeException;

/**
 * This is the parent class to handle erroneous Manticore queries
 */
class FunctionStatusHandler extends BaseHandlerWithTableFormatter {
	/**
	 *  Initialize the executor
	 *
	 * @param Payload $payload
	 * @return void
	 */
	public function __construct(public Payload $payload) {
	}

	/**
	 * Process the request and return self for chaining
	 *
	 * @return Task
	 * @throws RuntimeException
	 */
	public function run(): Task {
		$this->manticoreClient->setPath($this->payload->path);

		// We run in a thread anyway but in case if we need blocking
		// We just waiting for a thread to be done
		$taskFn = static function (): TaskResult {
			return TaskResult::withTotal(0)
				->column('Db', Column::String)
				->column('Name', Column::String)
				->column('Type', Column::String)
				->column('Definer', Column::String)
				->column('Modified', Column::String)
				->column('Created', Column::String)
				->column('Security_type', Column::String)
				->column('Invoker', Column::String)
				->column('character_set_client', Column::String)
				->column('collation_connection', Column::String)
				->column('Database Collation', Column::String);
		};

		return Task::create($taskFn, [])->run();
	}
}
