MapRedus
=========

Simple MapRedus type framework using redis and resque.

Using MapRedus
---------------

MapRedus uses Resque to handle the processes that it runs,
and redis to keep a store for the values/data produced.

Workers for a MapRedus job, are Resque workers.  Refer to the
Resque worker documentation to see how to load the necessary
environment for your worker to be able to run mapreduce jobs.
An example is also located in the tests.

### Mappers, Reducers, Finalizers
MapRedus needs a mapper, reducer, finalizer to be defined to run, for example:

    class Mapper < MapRedus::Mapper
      def self.map(data_to_map)
        data_to_map.split(" ").each do |data|
          key, value = data.split(",")
          yield( key, value )
        end
      end
    end

In this example, the mapper's map function calls yield to emit the key value pair
for storage in redis.  The reducer's reduce function acts similarly.

The finalizer runs whatever needs to be run when a job completes, an example:

    class Finalizer < MapRedus::Finalizer
      def self.finalize(mapreduce_job_id)
        result = {}
        each_key_value(mapreduce_job_id) do |key, value|
          result[key] = value
        end
        MapRedus::Job.save_result(MapReduce::Support.encode(result), mapreduce_job_id, "test:result")
      end
    end

Job ids and their information are destroyed in the delete call.  In this example
the saved result is saved for as long as needed.

Running Tests
-------------
Run the tests which tests the word counter example
* rake test

Requirements
------------
Redis
RedisSupport
Resque

### Notes
    Instead of calling "emit_intermediate"/"emit" in your map/reduce to
    produce a key value pair/value you call yield, which will call
    emit_intermediate/emit for you.  This gives flexibility in using
    Mapper/Reducer classes especially in testing.

TODO
----
not necessarily in the given order

* if a job fails we do what we are supposed to do
  i.e. add a failure_hook which does something if your job fails

* include functionality for a partitioner, input reader, combiner

* implement this shit (registering of environment shit in resque) so that we can run mapreduce commands from
  the command line.  Defining any arbitrary mapper and reducer.

* implement redundant workers (workers doing the same work in case one of them fails)

* edit emit for when we have multiple workers doing the same reduce
  (redundant workers for fault tolerance might need to change
  the rpush to a lock and setting of just a value)
  even if other workers do work on the same answer, want to make sure
  that the final reduced thing is the same every time

* Add fault tolerance, better tracking of which workers fail, especially
  when we have multiple workers doing the same work
  ... currently is handled by Resque failure auto retry

* if a perform operation fails then we need to have worker recover

* make use of finish_metrics somewhere so that we can have statistics on how
  long map reduce jobs take

* better tracking of work being assigned so we can know when a job is finished
  or in progress and have a trigger to do things when shit finishes
  
    in resque there is functionality for an after hook
    which performs something after your job does it's work

* ensure reducers only do a fixed amount of work?
  See section 3.2 of paper. bookkeeping
  that tells the master when tasks are in-progress or completed.
  this will be important for better paralleziation of tasks

* think about the following logic

    if a reducer starts working on a key after all maps have finished
    then when it is done the work on that key is finished forerver
    
    this would imply a job finishes when all map tasks have finished
    and all reduce tasks that start after the map tasks have finished
    
    if a reducer started before all map tasks were finished, then
    load its reduced result back onto the value list
    
    if the reducer started after all map tasks finished, then emit
    the result

Note on Patches/Pull Requests
-----------------------------
 
* Fork the project.
* Make your feature addition or bug fix.
* Add tests for it. This is important so I don't break it in a
  future version unintentionally.
* Commit, do not mess with rakefile, version, or history.
	(if you want to have your own version, that is fine but bump version in a commit by itself I can ignore when I pull)
* Send me a pull request. Bonus points for topic branches.
    
## Copyright
Copyright (c) 2010 Dolores Labs. See LICENSE for details.
