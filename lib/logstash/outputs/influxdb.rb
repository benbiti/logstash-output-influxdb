# encoding: utf-8
require "logstash/namespace"
require "logstash/outputs/base"
require "logstash/json"
require "stud/buffer"

# This output lets you output Metrics to InfluxDB
#
# The configuration here attempts to be as friendly as possible
# and minimize the need for multiple definitions to write to
# multiple series and still be efficient
#
# the InfluxDB API let's you do some semblance of bulk operation
# per http call but each call is database-specific
#
# You can learn more at http://influxdb.com[InfluxDB homepage]
class LogStash::Outputs::InfluxDB < LogStash::Outputs::Base
  include Stud::Buffer

  config_name "influxdb"

  # The database to write
  config :db, :validate => :string, :default => "stats"

  # The hostname or IP address to reach your InfluxDB instance
  config :host, :validate => :string, :required => true

  # The port for InfluxDB
  config :port, :validate => :number, :default => 8086

  # The user who has access to the named database
  config :user, :validate => :string, :default => nil, :required => true

  # The password for the user who access to the named database
  config :password, :validate => :password, :default => nil, :required => true

  # Series name - supports sprintf formatting
  config :series, :validate => :string, :default => "logstash"

  # Hash of key/value pairs representing data points to send to the named database
  # Example: `{'column1' => 'value1', 'column2' => 'value2'}`
  #
  # Events for the same series will be batched together where possible
  # Both keys and values support sprintf formatting
  config :data_points, :validate => :hash, :default => {}, :required => true

  # Allow the override of the `time` column in the event?
  #
  # By default any column with a name of `time` will be ignored and the time will
  # be determined by the value of `@timestamp`.
  #
  # Setting this to `true` allows you to explicitly set the `time` column yourself
  #
  # Note: **`time` must be an epoch value in either seconds, milliseconds or microseconds**
  config :allow_time_override, :validate => :boolean, :default => false

  # Set the level of precision of `time`
  #
  # only useful when overriding the time value
  config :time_precision, :validate => ["m", "s", "u"], :default => "s"

  # Tags

  config :tags, :validate => :hash, :default => {}

  # Allow value coercion
  #
  # this will attempt to convert data point values to the appropriate type before posting
  # otherwise sprintf-filtered numeric values could get sent as strings
  # format is `{'column_name' => 'datatype'}`
  #
  # currently supported datatypes are `integer` and `float`
  #
  config :coerce_values, :validate => :hash, :default => {}

  # This setting controls how many events will be buffered before sending a batch
  # of events. Note that these are only batched for the same series
  config :flush_size, :validate => :number, :default => 5

  # The amount of time since last flush before a flush is forced.
  #
  # This setting helps ensure slow event rates don't get stuck in Logstash.
  # For example, if your `flush_size` is 100, and you have received 10 events,
  # and it has been more than `idle_flush_time` seconds since the last flush,
  # logstash will flush those 10 events automatically.
  #
  # This helps keep both fast and slow log streams moving along in
  # near-real-time.
  config :idle_flush_time, :validate => :number, :default => 1

  public
  def register
    require "ftw" # gem ftw
    require 'cgi'
    @agent = FTW::Agent.new
    @queue = []

    @query_params = "db=#{@db}&u=#{@user}&p=#{@password.value}&time_precision=#{@time_precision}"
    @base_url = "http://#{@host}:#{@port}/write"
    @url = "#{@base_url}?#{@query_params}"

    buffer_initialize(
        :max_items => @flush_size,
        :max_interval => @idle_flush_time,
        :logger => @logger
    )
  end

  # def register

  public
  def receive(event)
# A batch POST for InfluxDB looks like this:
#measurement[,tag_key1=tag_value1...] field_key=field_value[,field_key2=field_value2] [timestamp]
#disk_free value=11i 14353622999950
#disk_free value=12i 14353623999950

    event_hash = {}
    event_hash['name'] = event.sprintf(@series)

    sprintf_points = Hash[@data_points.map { |k, v| [event.sprintf(k), event.sprintf(v)] }]

    if sprintf_points.has_key?('time')
      unless @allow_time_override
        logger.error("Cannot override value of time without 'allow_time_override'. Using event timestamp")
        sprintf_points['time'] = event.timestamp.to_i
      end
    else
      sprintf_points['time'] = event.timestamp.to_i
    end

    #not check code below
    @coerce_values.each do |column, value_type|
      if sprintf_points.has_key?(column)
        begin
          case value_type
            when "integer"
            @logger.debug? and @logger.debug("Converting column #{column} to type #{value_type}: Current value: #{sprintf_points[column]}")
              sprintf_points[column] = sprintf_points[column].to_i
            when "float"
            @logger.debug? and @logger.debug("Converting column #{column} to type #{value_type}: Current value: #{sprintf_points[column]}")
              sprintf_points[column] = sprintf_points[column].to_f
            else
              @logger.error("Don't know how to convert to #{value_type}")
          end
        rescue => e
          @logger.error("Unhandled exception", :error => e.message)
        end
      end
    end

    line = ''
    line.concat(event.sprintf(@series))
    line.concat(",")

    @tags.each do |tag_field, tag_field_value|
      tag_value=event.sprintf(tag_field_value)

      if tag_value.nil?
        @logger.debug? and @logger.debug("empty")
      else
        @logger.debug? and @logger.debug("tag value class == #{tag_value.class}")
        if tag_value.class == String
          @logger.debug? and @logger.debug(" #{tag_field} and value #{tag_field_value}: Current value: #{tag_value}")
          line.concat(event.sprintf(tag_field))
          line.concat("=\"")
          line.concat(tag_value)
          line.concat("\",")

        elsif tag_value.class ==LogStash::Timestamp
          line.concat("#{event.sprintf(tag_field)}=\"#{tag_value}\",")
        else
          line.concat("#{event.sprintf(tag_field)}=#{tag_value},")
        end
      end
    end
    line=line.chop()
    line.concat(" ")
    @data_points.each do |field, field_value|
      value=event.sprintf(field_value)
      if value.nil?
        @logger.debug? and @logger.debug("empty")
      else
        if value.class == String
          value=value.gsub('=', '\=')
          line.concat(event.sprintf(field))
          line.concat("=\"")
          line.concat(value)
          line.concat("\",")
        elsif value.class ==LogStash::Timestamp
          line.concat("#{event.sprintf(field)}=\"#{value}\",")
        else
          line.concat("#{event.sprintf(field)}=#{value},")
        end
      end
    end
    line = line.chop()
    buffer_receive(line)
  end

  # def receive

  def flush(events, teardown = false)
    # seen_series stores a list of series and associated columns
    # we've seen for each event
    # so that we can attempt to batch up points for a given series.
    #
    # Columns *MUST* be exactly the same
    events.each do |ev|
      post(ev)
    end
  end

  def post(body)
    begin
      @logger.debug? and @logger.error("Post body: #{body}")
      response = @agent.post!(@url, :body => body)
    rescue EOFError
      @logger.warn("EOF while writing request or reading response header from InfluxDB",
                   :host => @host, :port => @port)
      return # abort this flush
    end
    # Consume the body for error checking
    # This will also free up the connection for reuse.
    if response.status != 204
      @logger.error("Error writing to InfluxDB",
                    :response => response, :response_body => body,
                    :request_body => @queue.join("\n"))
      body = ""
      begin
        response.read_body { |chunk| body += chunk }
      rescue EOFError
        @logger.warn("EOF while reading response body from InfluxDB",
                     :host => @host, :port => @port)
        return # abort this flush
      end
      return
    end
  end # def post

  def teardown
    buffer_flush(:final => true)
  end # def teardown
end # class LogStash::Outputs::InfluxDB
