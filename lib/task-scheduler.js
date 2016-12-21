// Copyright 2016, EMC, Inc.

'use strict';

var di = require('di');
var grpc = require('grpc');
var url = require('url');

module.exports = taskSchedulerFactory;
di.annotate(taskSchedulerFactory, new di.Provide('TaskGraph.TaskScheduler'));
di.annotate(taskSchedulerFactory,
    new di.Inject(
        'Protocol.Events',
        'TaskGraph.TaskGraph',
        'TaskGraph.Store',
        'TaskGraph.LeaseExpirationPoller',
        'Constants',
        'Logger',
        'Promise',
        'uuid',
        'Assert',
        '_',
        'Rx.Mixins',
        'Task.Messenger',
        'Services.Configuration',
        'TaskGraph.TaskScheduler.Server',
        'consul'
    )
);

function taskSchedulerFactory(
    eventsProtocol,
    TaskGraph,
    store,
    LeaseExpirationPoller,
    Constants,
    Logger,
    Promise,
    uuid,
    assert,
    _,
    Rx,
    taskMessenger,
    configuration,
    SchedulerServer,
    Consul
) {
    var logger = Logger.initialize(taskSchedulerFactory);
    var urlObject = url.parse(configuration.get('consulUrl') || 'consul://127.0.0.1:8500');

    // Create a promisified Consul interface
    var consulOpts = {
        host: urlObject.hostname,
        port: urlObject.port || 8500,
    };
    var consul = new Consul(_.merge({}, consulOpts, {
        promisify: function(fn) {
          return new Promise(function(resolve, reject) {
            try {
              return fn(function(err, data, res) {
                if (err) {
                  err.res = res;
                  return reject(err);
                }
                return resolve([data, res]);
              });
            } catch (err) {
              return reject(err);
            }
          });
        }
    }));

    var grpcRescheduleErrors = [
        grpc.status.RESOURCE_EXHAUSTED,
        grpc.status.FAILED_PRECONDITION,
        grpc.status.ABORTED,
        grpc.status.OUT_OF_RANGE,
        grpc.status.INTERNAL,
        grpc.status.UNAVAILABLE,
        grpc.status.NOT_FOUND
    ];

    var proto = grpc.load(__dirname + '/../runner.proto').runner;
    /**
     * The TaskScheduler handles all graph and task evaluation, and is
     * the decision engine for scheduling new tasks to be run within a graph.
     *
     * @param {Object} options
     * @constructor
     */
    function TaskScheduler(options) {
        options = options || {};
        this.running = false;
        this.schedulerId = options.schedulerId || uuid.v4();
        this.domain = options.domain || Constants.Task.DefaultDomain;
        this.rpcPort = options.rpcPort;
        this.evaluateTaskStream = new Rx.Subject();
        this.evaluateGraphStream = new Rx.Subject();
        this.checkGraphFinishedStream = new Rx.Subject();
        this.pollInterval = options.pollInterval || 1000;
        this.concurrencyMaximums = this.getConcurrencyMaximums(options.concurrent);
        this.findUnevaluatedTasksLimit = options.findUnevaluatedTasksLimit || 200;
        this.subscriptions = [];
        //this.leasePoller = null;
        this.debug = _.has(options, 'debug') ? options.debug : false;
    }

    /**
     * Generate a concurrency counter object. This is used with the
     * Rx.Observable.prototype.mergeLossy function from Rx.Mixins to keep
     * ensure that a specified maximum of the same type of asynchronous
     * call be able to be unresolved at the same time (for example, only
     * wait on a max of 100 database calls to resolve at any point in time).
     *
     * @param {Number} max
     * @returns {Object} concurrency counter object
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.concurrentCounter = function(max) {
        assert.number(max);
        return {
            count: 0,
            max: max
        };
    };

    /**
     * Generate concurrency counter objects for the different IO calls we make
     * to the store and messenger. Basically a rudimentary method of adding throttling.
     *
     * @param {Object} concurrentOptions
     * @returns {Object} set of concurrency counter objects
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.getConcurrencyMaximums = function(concurrentOptions) {
        var self = this;
        var _options = _.defaults(concurrentOptions || {}, {
            // Favor evaluateGraphStream, since it's what results in actual
            // scheduling. If we're at load, defer evaluation in favor of scheduling.
            // TODO: Probably better as a priority queue, evaluateGraphStream events
            // always come up first?
            findReadyTasks: 100,
            updateTaskDependencies: 100,
            handleScheduleTaskEvent: 100,
            completeGraphs: 100,
            findUnevaluatedTasks: 1
        });
        return _.transform(_options, function(result, v, k) {
            result[k] = self.concurrentCounter(v);
        }, {});
    };

    /**
     * Create and start the Rx observables (streams) that do all the scheduling work.
     *
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.initializePipeline = function() {
        /*
         * Before setting up the stream, make sure it is running, otherwise
         * this will create a stream that will never run and immediately complete.
         * This is basically defensive programming to try to prevent accidents where the
         * startup code is executed in the wrong order (e.g. pollTasks() then
         * this.running = true, that would be buggy because pollTasks() would
         * stop execution before this.running = true was set).
         */
        assert.ok(this.running, 'scheduler is running');

        // Inputs from this.evaluateTaskStream
        // Outputs to this.evaluateGraphStream
        // Outputs to this.checkGraphFinishedStream
        this.createUpdateTaskDependenciesSubscription(
                this.evaluateTaskStream, this.evaluateGraphStream, this.checkGraphFinishedStream)
        .subscribe(
            this.handleStreamDebug.bind(this, 'Task evaluated'),
            this.handleStreamError.bind(this, 'Error at update task dependencies stream')
        );
        // Outputs to this.evaluateTaskStream
        /*
        BJP: Remove pollers from the pipeline for benchmarking.
             Error cases handled by these pollers should now be handled
             when tasks are sent to runners via gRPC
        this.createUnevaluatedTaskPollerSubscription(this.evaluateTaskStream)
        .subscribe(
            this.handleStreamDebug.bind(this, 'Triggered evaluate task event'),
            this.handleStreamError.bind(this, 'Error polling for tasks')
        );
        // Outputs to this.evaluateGraphStream
        this.createEvaluatedTaskPollerSubscription(this.evaluateGraphStream)
        .subscribe(
            this.handleStreamDebug.bind(this, 'Triggered evaluate graph event'),
            this.handleStreamError.bind(this, 'Error polling for tasks')
        ); */
        // Outputs to evaluateGraphStream
        this.createRunnerServicePollerSubscription(this.evaluateGraphStream)
        .subscribe(
            this.handleStreamDebug.bind(this, 'Triggered evaluate graph event'),
            this.handleStreamError.bind(this, 'Error polling for runner services')
        );
        // Inputs from this.evaluateGraphStream
        this.createTasksToScheduleSubscription(this.evaluateGraphStream)
        .subscribe(
            this.handleStreamSuccess.bind(this, 'Task scheduled'),
            this.handleStreamError.bind(this, 'Error at task scheduling stream')
        );
        // Inputs from this.checkGraphFinishedStream
        this.createCheckGraphFinishedSubscription(this.checkGraphFinishedStream)
        .subscribe(
            this.handleStreamSuccess.bind(this, 'Graph finished'),
            this.handleStreamError.bind(this, 'Error at check graph finished stream')
        );
    };

    /**
     * This is used with Rx.Observable.prototype.takeWhile in each Observable
     * created by TaskScheduler.prototype.initializePipeline. When isRunning()
     * returns false, all the observables will automatically dispose.
     *
     * @returns {Boolean}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.isRunning = function() {
        return this.running;
    };

    /**
     * This finds all tasks that are ready to run. If a graphId is
     * specified in the data object, then it will only find tasks ready
     * to run that are within that graph.
     *
     * @param {Object} data
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.findReadyTasks = function(data) {
        assert.object(data);
        var self = this;

        return Rx.Observable.just(data)
        .flatMap(function() {
            return store.findReadyTasks(self.domain, data.graphId);
        })
        .catch(self.handleStreamError.bind(self, 'Error finding ready tasks'));
    };

    /**
     * This handles task finished events, and updates all other tasks that
     * have a waitingOn dependency on the finished task.
     *
     * @param {Object} taskHandlerStream
     * @param {Object} evaluateGraphStream
     * @param {Object} checkGraphFinishedStream
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.createUpdateTaskDependenciesSubscription =
        function(taskHandlerStream, evaluateGraphStream, checkGraphFinishedStream) {

        var self = this;

        return taskHandlerStream
        .takeWhile(self.isRunning.bind(self))
        .tap(self.handleStreamDebug.bind(self, 'Received evaluate task event'))
        .map(self.updateTaskDependencies.bind(self))
        .mergeLossy(self.concurrencyMaximums.updateTaskDependencies)
        .tap(function(task) {
            var _task = _.pick(task, ['domain', 'graphId', 'taskId']);
            self.handleStreamDebug('Updated dependencies for task', _task);
        })
        .filter(function(data) { return data; })
        .map(self.handleEvaluatedTask.bind(self, checkGraphFinishedStream, evaluateGraphStream));
    };

    /**
     * Once a task has finished and been evaluated (dependendent tasks updated)
     * then check if the task is terminal to determine whether the graph is potentially
     * completed or not. If it is not terminal, emit to evaluateGraphStream which
     * will trigger findReadyTasks for that graph. If it is terminal, check if
     * the graph is finished.
     *
     * @param {Object} checkGraphFinishedStream
     * @param {Object} evaluateGraphStream
     * @param {Object} data
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.handleEvaluatedTask = function(
            checkGraphFinishedStream, evaluateGraphStream, data) {
        if (_.contains(data.terminalOnStates, data.state)) {
            checkGraphFinishedStream.onNext(data);
        } else {
            evaluateGraphStream.onNext({ graphId: data.graphId });
        }
    };

    /**
     * Stream handler that finds tasks that are ready to schedule and schedules
     * them.
     *
     * @param {Object} evaluateGraphStream
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.createTasksToScheduleSubscription = function(evaluateGraphStream) {
        var self = this;
        return evaluateGraphStream
        .takeWhile(self.isRunning.bind(self))
        .map(self.findReadyTasks.bind(self))
        .mergeLossy(self.concurrencyMaximums.findReadyTasks)
        .filter(function(data) { return !_.isEmpty(data.tasks); })
        .pluck('tasks')
        // Ideally this would just be .pluck('tasks').from() but that didn't work.
        // Instead, flatMap the Rx.Observable.from observable below.
        // Take a single stream event with an array data type, and emit
        // multiple new stream events (one for each element in the array)
        // that are all non-blocking on each other, because we want to process
        // handleScheduleTaskEvent for each task independently of the others.
        .flatMap(function(tasks) { return Rx.Observable.from(tasks); })
        .map(self.handleScheduleTaskEvent.bind(self))
        .mergeLossy(self.concurrencyMaximums.handleScheduleTaskEvent)
        .map(function(task) {
            return _.pick(task, ['domain', 'graphId', 'taskId']);
        });
    };

    /**
     * Publish a task schedule event with the messenger.
     *
     * @param {Object} data
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.handleScheduleTaskEvent = function(data) {
        var self = this;
        assert.object(data, 'task data object');

        return Rx.Observable.just(data)
        .flatMap(self.publishScheduleTaskEvent.bind(self))
        .catch(self.handleStreamError.bind(self, 'Error scheduling task'));
    };

    /**
     * Determine whether a graph is finished or failed based on a terminal
     * task state.
     *
     * @param {Object} checkGraphFinishedStream
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.createCheckGraphFinishedSubscription = function(
            checkGraphFinishedStream) {
        var self = this;

        return checkGraphFinishedStream
        .takeWhile(self.isRunning.bind(self))
        .map(function(data) {
            // We already know that the task in question is in a terminal state,
            // otherwise we wouldn't have published data to this stream.
            // If a task is in a failed task state but it is non-terminal, this
            // code will not be reached.
            if (!data.ignoreFailure && _.contains(Constants.Task.FailedStates, data.state)) {
                return self.failGraph(data, Constants.Task.States.Failed);
            } else {
                return self.checkGraphSucceeded(data);
            }
        })
        .mergeLossy(self.concurrencyMaximums.completeGraphs);
    };

    /**
     * Check if a graph is finished. If so, mark it as successful
     * in the store.
     *
     * @param {Object} data
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.checkGraphSucceeded = function(data) {
        assert.object(data, 'graph data object');
        var self = this;

        return Rx.Observable.just(data)
        .flatMap(store.checkGraphSucceeded.bind(store))
        .filter(function(_data) { return _data.done; })
        .flatMap(store.setGraphDone.bind(store, Constants.Task.States.Succeeded))
        .filter(function(graph) { return !_.isEmpty(graph); })
        .map(function(graph) { return _.pick(graph, ['instanceId', '_status']); })
        .tap(self.publishGraphFinished.bind(self))
        .catch(self.handleStreamError.bind(self, 'Error handling graph done event'));
    };

    /**
     * Set a graph to a failed state in the store, and find pending tasks
     * within the graph that should also be failed.
     *
     * @param {Object} data
     * @param {String} graphState
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.failGraph = function(data, graphState) {
        return Rx.Observable.just(data.graphId)
        .flatMap(store.getActiveGraphById)
        .filter(function(graph) {return !_.isEmpty(graph);})
        .map(function(doneGraph) {
            return _.map(doneGraph.tasks, function(taskObj) {
                if(taskObj.state ===  Constants.Task.States.Pending) {
                    taskObj.state = graphState;
                }
                taskObj.taskId = taskObj.instanceId;
                taskObj.graphId = data.graphId;
                return taskObj;
            });
        })
        .flatMap(this.handleFailGraphTasks.bind(this))
        .flatMap(store.setGraphDone.bind(store, graphState, data))
        // setGraphDone can return null if another source has already updated
        // the graph state. Don't publish the same event twice.
        .filter(function(graph) { return graph; })
        .tap(this.publishGraphFinished.bind(this))
        .catch(this.handleStreamError.bind(this, 'Error failing/cancelling graph'));
    };

    /**
     * Fail pending tasks within a graph.
     *
     * @param {Array} tasks
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.handleFailGraphTasks = function(tasks) {
        logger.debug('cancel/failing pending tasks', {data:_.pluck(tasks, 'taskId')});
        return Rx.Observable.just(tasks)
        .flatMap(Promise.map.bind(Promise, tasks, store.setTaskState))
        .flatMap(Promise.map.bind(Promise, tasks, store.markTaskEvaluated))
        .flatMap(Promise.map.bind(Promise, _.pluck(tasks, 'taskId'),
                    taskMessenger.publishCancelTask))
        .flatMap(Promise.map.bind(Promise, tasks, store.setTaskStateInGraph.bind(store)));
    };

    /**
     * Subscribe to messenger events to cancel a graph, and fail the graph on
     * received events.
     *
     * @returns {Promise}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.subscribeCancelGraph = function() {
        var self = this;
        return taskMessenger.subscribeCancelGraph(
            function(data) {
                var resolve;
                var deferred = new Promise(function(_resolve) {
                    resolve = _resolve;
                });

                logger.debug('listener received cancel graph event', {
                    data: data,
                    schedulerId: self.schedulerId
                });

                self.failGraph(data, Constants.Task.States.Cancelled)
                .pluck('instanceId')
                .subscribe(
                    function(graphId) {
                        if (deferred.isPending()) {
                            resolve(graphId);
                        }
                        self.handleStreamSuccess('Graph Cancelled', { graphId: graphId });
                    },
                    self.handleStreamError.bind(self, 'subscribeCancelGraph stream error'),
                    function() {
                        // If there is no active workflow, this.failGraph will trigger
                        // an onCompleted event but not an onNext event. Use this to determine
                        // when a bad request has been made and to respond to the messenger request
                        // with null. This lets the API server handle error codes for the race
                        // condition where there is an active workflow before the messenger
                        // request is made.
                        if (deferred.isPending()) {
                            resolve(null);
                        }
                    }
                );

                return deferred;
            }
        );
    };

    /**
     * Evaluate and update all tasks that have a waitingOn dependency for a finished task,
     * then mark the finished task as evaluated. If a failure occurs before the
     * store.markTaskEvaluated call, then this process (which is idempotent) will be
     * repeated on the next poll interval. This basically enables some sort of
     * an equivalent to a transactional database call in failure cases.
     *
     * @param {Object} data
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.updateTaskDependencies = function(data) {
        assert.object(data, 'task dependency object');
        return Rx.Observable.forkJoin([
            store.setTaskStateInGraph(data)
            //store.updateDependentTasks(data),
            //store.updateUnreachableTasks(data)
        ])
        .flatMap(store.markTaskEvaluated.bind(store, data))
        .catch(this.handleStreamError.bind(this, 'Error updating task dependencies'));
    };

    /**
     * Log handler for observable onNext success events.
     *
     * @param {String} msg
     * @param {Object} tasks
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.handleStreamSuccess = function(msg, data) {
        if (msg) {
            if (data) {
                data.schedulerId = this.schedulerId;
            }
            logger.debug(msg, data);
        }
        return Rx.Observable.empty();
    };

    /**
     * Log handler for observable onError failure events.
     *
     * @param {String} msg
     * @param {Object} err
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.handleStreamError = function(msg, err) {
        logger.error(msg, {
            schedulerId: this.schedulerId,
            // stacks on some error objects (particularly from the assert library)
            // don't get printed if part of the error object so separate them out here.
            error: _.omit(err, 'stack'),
            stack: err.stack
        });
        return Rx.Observable.empty();
    };

    /**
     * Log handler for debug messaging during development/debugging. Only
     * works when this.debug is set to true;
     *
     * @param {String} msg
     * @param {Object} data
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.handleStreamDebug = function(msg, data) {
        if (this.debug) {
            if (data) {
                data.schedulerId = this.schedulerId;
            }
            logger.debug(msg, data);
        }
    };

    /**
     * Receive messenger events for when tasks finish, and kick off the task/graph
     * evaluation via the evaluateTaskStream.
     *
     * @returns {Promise}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.subscribeTaskFinished = function() {
        var self = this;
        return taskMessenger.subscribeTaskFinished(
            this.domain,
            function(data) {
                logger.debug('Listener received task finished event, triggering evaluation', {
                    data: data,
                    schedulerId: self.schedulerId
                });
                self.evaluateTaskStream.onNext(data);
            }
        );
    };

    /**
     * Publish a graph finished event with the messenger.
     *
     * @param {Object} graph
     * @returns {Promise}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.publishGraphFinished = function(graph) {
        return eventsProtocol.publishGraphFinished(graph.instanceId, graph._status)
        .catch(function(error) {
            logger.error('Error publishing graph finished event', {
                graphId: graph.instanceId,
                _status: graph._status,
                error: error
            });
        });
    };

    /**
     * Use gRPC client to start a task on selected runner.
     *
     * @param {Object} data
     * @returns {Promise}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.publishScheduleTaskEvent = function(data) {
        var self = this;
        return Promise.try(function() {
            if (!self.runners || !self.runners.length) {
                throw new Error('no runner available for task: ' + data.taskId);
            }
        })
        .then(function() {
            var runner = self.runners.next();
            logger.debug('Running task ' + data.taskId + ' on ' + runner.ID); 
            return runner.client.runTask({
                taskId: data.taskId,
                graphId: data.graphId
            })
            .then(function() {
                return data;
            })
            .catch(function(err) {
                // Backoff for 1s between retries
                var delay = 1000;

                // Check for maximum retries
                if (data.attempt >= 3) {
                    throw err;
                }
                if (_.contains(grpcRescheduleErrors, err.code)) {
                    // This class of errors gets rescheduled without rediscovery.
                    // This involves a random backoff followed by another attempt to
                    // call the runTask RPC.  If only one runner is available, the rpc
                    // call is simply retried after the delay.  If other runners are available,
                    // the task will be rescheduled on the next runner in the RR sequence.
                    data.attempt = data.attempt !== undefined ? data.attempt + 1 : 0;
                    logger.error('runTask RPC failed with error: ' + err.code.toString() +
                                 ' reschedule attempt ' + data.attempt.toString());
                    return Promise.delay(delay).then(self.publishScheduleTaskEvent.bind(self,data));
                }
                throw err;
            });
        });
    };

    /**
     * Periodically ask consul for its service list and update the list of runners
     * accordingly.  If runners have been lost since services were last discovered,
     * leases associated with them will be expired and the evaluateGraphStream triggered.
     *
     * @param {Object} evaluateTaskStream
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.createRunnerServicePollerSubscription = function(evaluateGraphStream) {
        var self = this;

        return Rx.Observable.interval(self.pollInterval)
        .takeWhile(self.isRunning.bind(self))
        .flatMap(self._getRunnerServices.bind(self))
        .flatMap(function(runners) {
            // Find lost runner services
            var lastRunners = _.map(self.runners ? self.runners.array : [], 'ID');
            var thisRunners = _.map(runners, 'ID');
            var lostRunners = _.difference(lastRunners, thisRunners);

            // store runners in a circular iterator for convenient
            // round-robin scheduling
            self.runners = _circularIterator(runners);

            // Expire leases associated with lost runners
            return Promise.each(lostRunners, function(lost) {
                logger.info('Lost runner with ID: ' + lost);
                return store.expireLease(lost);
            });
        })
        .flatMap(function(lostRunners) { return Rx.Observable.from(lostRunners); })
        .first(evaluateGraphStream.onNext.bind(evaluateGraphStream, {}));
    };

    /**
     * On the case of messenger or scheduler failures, or lossiness caused by high load,
     * poll the database on an interval to pick up dropped work related to
     * updating unevaluated tasks and dependent tasks.
     *
     * @param {Object} evaluateTaskStream
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.createUnevaluatedTaskPollerSubscription = function(evaluateTaskStream) {
        var self = this;

        return Rx.Observable.interval(self.pollInterval)
        .takeWhile(self.isRunning.bind(self))
        .map(self.findUnevaluatedTasks.bind(self, self.domain))
        .mergeLossy(self.concurrencyMaximums.findUnevaluatedTasks)
        .flatMap(function(tasks) { return Rx.Observable.from(tasks); })
        .map(evaluateTaskStream.onNext.bind(evaluateTaskStream));
    };

    /**
     * On the case of messenger or scheduler failures, or lossiness caused by high load,
     * poll the database on an interval to pick up dropped work related to
     * evaluating graph states and scheduling new tasks.
     *
     * @param {Object} evaluateGraphStream
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.createEvaluatedTaskPollerSubscription = function(evaluateGraphStream) {
        var self = this;

        return Rx.Observable.interval(self.pollInterval)
        .takeWhile(self.isRunning.bind(self))
        .map(evaluateGraphStream.onNext.bind(evaluateGraphStream, {}));
    };

    /**
     * Find all tasks in the database that haven't been fully evaluated
     * (see TaskScheduler.prototype.updateTaskDependencies).
     *
     * @param {String} domain
     * @returns {Observable}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.findUnevaluatedTasks = function(domain) {
        return Rx.Observable.just()
        .flatMap(store.findUnevaluatedTasks.bind(store, domain, this.findUnevaluatedTasksLimit))
        .tap(function(tasks) {
            if (tasks && tasks.length) {
                logger.debug('Poller is triggering unevaluated tasks to be evaluated', {
                    tasks: _.map(tasks, 'taskId')
                });
            }
        })
        .catch(this.handleStreamError.bind(this, 'Error finding unevaluated tasks'));
    };

    /**
     * Emit a new graph evaluation event to trigger TaskScheduler.prototype.findReadyTasks.
     *
     * @param {Object} data
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.runTaskGraphCallback = function(data) {
        assert.object(data);
        assert.uuid(data.graphId);
        this.evaluateGraphStream.onNext(data);
    };

    /**
     * Start the task scheduler and its observable pipeline, as well as the expired lease poller.
     * Subscribe to messenger events.
     *
     * @returns {Promise}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.start = function() {
        var self = this;
        return Promise.resolve()
        .then(function() {
            self.running = true;
            self.initializePipeline();
            //self.leasePoller = LeaseExpirationPoller.create(self, {});
            //self.leasePoller.start();
            return [
                self.subscribeTaskFinished(),
                self.subscribeCancelGraph()
            ];
        })
        .spread(function(
            taskFinishedSubscription, cancelGraphSubscription
        ) {
            self.subscriptions.push(taskFinishedSubscription);
            self.subscriptions.push(cancelGraphSubscription);
            logger.info('Task scheduler started', {
                schedulerId: self.schedulerId,
                domain: self.domain
            });
        })
        .then(function() {
            /*
             * Start the gRPC endpoint
             */
            var grpcPort = self.rpcPort || 31000;
            var urlObject = url.parse(
                _.get(configuration.get('taskgraphConfig'), 'url',
                                        'scheduler://127.0.0.1:' + grpcPort.toString())
            );
            self.gRPC = new SchedulerServer({
                hostname: urlObject.hostname,
                port: urlObject.port
            });
            
            return self.gRPC.start();
        })
        .then(self._getRunnerServices.bind(self))
        .tap(function(services) {
            // store runners in a circular iterator for convenient
            // round-robin scheduling
            self.runners = _circularIterator(services);

            //TODO: Check for leases that need to be expired
        })
        .tap(function() {
            // TODO: figure out why watches aren't working
            //self.watch = consul.watch({ method: consul.agent.service.list });

            // TODO: What do we do about lost services???
            //self.watch.on('change', _getRunnerServices.bind(self));
            return consul.agent.service.register({
                name: 'taskgraph',
                id: self.schedulerId,
                tags: [ 'scheduler' ],
                address: self.gRPC.options.hostname,
                port: self.gRPC.options.port
            });
        });
    };

    /**
     * Private function that wraps an array in a circular iterator.  This is used
     * for convenient round-robin scheduling.
     *
     * @param {Object} Array to iterate
     * @returns {Object} Iterator
     * @memberOf TaskScheduler
     */
    function _circularIterator(array) {
        var index = 0;
        var arrayLength;

        return {
            length: array.length,
            array: array,
            next: function() {
                if (arrayLength === undefined) {
                    // first use
                    arrayLength = array.length;
                } else if (arrayLength !== array.length) {
                    // array length has changed, reset index
                    arrayLength = array.length;
                    index = 0;
                }
                var next = array[index];
                index = (index + 1) % array.length;
                return next;
            }
        };
    }

    /**
     * Private function used to decorate gRPC client methods.
     * This adds Promise handling and automatic JSON parsing while
     * retaining the general rpc method semantics (no need to call
     * explicit RPC method wrapper e.g. runRpc(client, 'method'))
     *
     * @param {Object} gRPC client object
     * @param {String} Name of method to decorate
     * @memberOf TaskScheduler
     */
    function _decorateRpc(client, method) {
        var rpcMethod = client[method];
        var wrappedMethod = function() {
            var args = Array.prototype.slice.call(arguments);
            var self = this;
            return new Promise(function(resolve, reject) {
                args.push(function(err, result) {
                    if (err) {
                        return reject(err);
                    }
                    try {
                        var response = JSON.parse(result.response);
                        return resolve(response);
                    } catch(e) {
                        return reject(e);
                    }
                });
                rpcMethod.apply(self, args);
            });
        };
        client[method] = wrappedMethod;
    }

    /**
     * Private function used to discover available runner services.
     *
     * @returns {Promise}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype._getRunnerServices = function() {
        var self = this;

        // get a list of all taskgraph runner services that report a passing health status
        return Promise.join(consul.agent.service.list(),consul.health.checks('taskgraph'),
            function(taskgraphServiceList,taskgraphHealthChecks) {
                var taskgraphArray =  _(taskgraphServiceList[0]).filter({ Service: 'taskgraph',
                                                               Tags: [ 'runner', self.domain ] })
                    .remove(function (service) {
                        var found = _.find(taskgraphHealthChecks[0], function (healthCheck) {
                            console.log('serviceID: ' + healthCheck.ServiceID);
                            return healthCheck.ServiceID === service.ID &&
                                   healthCheck.Status === 'passing';
                        });
                        return found;
                    });
                return taskgraphArray.value();
            })
        .each(function(service) {
            // instantiate a gRPC client for each runner
            // then decorate all rpc client methods
            logger.debug('Found runner ' + service.ID);
            service.client = new proto.Runner(service.Address + ':' + service.Port,
                                              grpc.credentials.createInsecure());
            _.forEach(proto.Runner.service.children, function(child) {
                if (child.className === 'Service.RPCMethod') {
                    _decorateRpc(service.client, child.name);
                }
            });
        });
    };

    /**
     * Clean up any messenger subscriptions. Stop polling for expired leases.
     *
     * @param {Object} data
     * @returns {Promise}
     * @memberOf TaskScheduler
     */
    TaskScheduler.prototype.stop = function() {
        var self = this;
        self.running = false;
        /*
        if (self.leasePoller) {
            self.leasePoller.stop();
        }*/
        // TODO: Restore this code when watches are fixed
        //self.watch.end();
        return consul.agent.service.deregister({id: self.schedulerId})
        .then(function() {
            return self.gRPC.stop();
        })
        .then(function() {
            return Promise.map(self.subscriptions, function() {
                return self.subscriptions.pop().dispose();
            });
        });
    };

    /**
     * @param {Object} options
     * @returns {Object} TaskScheduler instance
     * @memberOf TaskScheduler
     */
    TaskScheduler.create = function(options) {
        return new TaskScheduler(options);
    };

    return TaskScheduler;
}
