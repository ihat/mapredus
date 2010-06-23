MapRedus
=========

Simple MapReduce type framework using redis and resque.

Overview
--------

This is an experimental implementation of MapReduce using Ruby for
process definition, Resque for work execution, and Redis for data
storage.

Goals:
 * simple M/R-style programming for existing Ruby projects
 * low cost of entry (no need for a dedicated cluster)

If you are looking for a high-performance MapReduce implementation
that can meet your big data needs, try Hadoop.


Using MapRedus
---------------

MapRedus uses Resque to handle the processes that it runs,
and redis to keep a store for the values/data produced.

Workers for a MapRedus process, are Resque workers.  Refer to the
Resque worker documentation to see how to load the necessary
environment for your worker to be able to run mapreduce processs.
An example is also located in the tests.

### Attaching a mapreduce process to a class
Often times you'll want to define a mapreduce process that does operation on
data within a class.  Here is how this looks.  There is also an example of this
in the tests.

    class Job
      mapreduce_process :word_count, WordCounter, Adder, ToHash, MapRedus::JsonOutputter, "job:store:result"
    end

The mapreduce_process needs a name, mapper, reducer, finalizer, outputter, and
key to store the result.  The operation would then be run on a job calling the following.

    job = Job.new
    job.mapreduce.word_count( data )

The data specifies the data on which this operation is to run.  We are currently
working on a way to allow the result_store_key to change depending on class properties.
For instance in the above example, if the Job class had an id attribute, we may want to 
store the final mapreduce result in "job:store:result:#{id}".  We'll also be looking
to add a Inputter (maybe equivalent to Hadoops InputStream?) which defines how you want
process the input data to provide it to the map.  The inputter will be a queue process
to be processed by the resque queue.

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

The finalizer runs whatever needs to be run when a process completes, an example:

    class Finalizer < MapRedus::Finalizer
      def self.finalize(process)
        result = {}
        process.each_key_value do |key, value|
          result[key] = value
        end
        process.save_result(result)
      end
    end

If you are dealing with a lot of keys and values, you'll likely not want to store your
data in this manner; this is just an example of what you can do.
In this example the saved result is saved for as long as needed.  Often times will want to encode the
saved result in a way (since it is being saved to redis as a string).  This is
where we'd define an Outputter maybe in this case in the hash format, "key:value\tkey:value".

    class HashOutputter < MapRedus::Outputter
      def encode(o)
        encoding = o.map do |k,v| 
          [k,v].join(":")
        end
        encoding.join("\t")
      end
      def decode(o)
        o.split("\t").inject({}) do |m, kv|
          k, v = kv.split(":")
          m[k]=v
          m
        end
      end
    end

This is likely not what you would want to do but is an example of what can be done.
The default Outputter makes no changes to original result.

Running Tests
-------------
Run the tests which tests the word counter example and some other tests
* bundle exec rake

Requirements
------------
Redis
RedisSupport
Resque
Resque-scheduler

### Notes
    Instead of calling "emit_intermediate"/"emit" in your map/reduce to
    produce a key value pair/value you call yield, which will call
    emit_intermediate/emit for you.  This gives flexibility in using
    Mapper/Reducer classes especially in testing.

TODO
----
not necessarily in the given order

* if a process fails we do what we are supposed to do
  i.e. add a failure_hook which does something if your process fails

* include functionality for a partitioner, input reader, combiner

* implement this shit (registering of environment shit in resque) so that we can run mapreduce commands from
  the command line.  Defining any arbitrary mapper and reducer.

* implement redundant workers (workers doing the same work in case one of them fails)

* if a reducer runs a recoverable fail, then make sure that an attempt to reenslave
  the worker is delayed by some fixed interval

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
  long map reduce processs take

* better tracking of work being assigned so we can know when a process is finished
  or in progress and have a trigger to do things when shit finishes
  
    in resque there is functionality for an after hook
    which performs something after your process does it's work

    might also check out the resque-status plugin for a cheap and
    easy way to plug status and completion-rate into existing resque
    jobs.

* ensure reducers only do a fixed amount of work?
  See section 3.2 of paper. bookkeeping
  that tells the master when tasks are in-progress or completed.
  this will be important for better paralleziation of tasks

* think about the following logic

    if a reducer starts working on a key after all maps have finished
    then when it is done the work on that key is finished forerver
    
    this would imply a process finishes when all map tasks have finished
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
