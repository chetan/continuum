
require 'multi_json'
require 'socket'
require 'curb'

module Continuum

  # Create an instance of the client to interface with the OpenTSDB API (http://opentsdb.net/http-api.html)
  class Client

    attr_reader :write_connection

    # Create an connection to a specific OpenTSDB instance
    #
    # *Params:*
    #
    # * host is the host IP (defaults to localhost)
    # * port is the host's port (defaults to 4242)
    #
    # *Returns:*
    #
    # A client to play with
    def initialize host = '127.0.0.1', port = 4242
      @host = host
      @port = port
    end

    # Lists the supported aggregators by this instance
    #
    # *Returns:*
    #
    # An array of aggregators.
    def aggregators
      response = get_http '/aggregators?json=true'
      MultiJson.load response
    end

    # Lists an array of log lines. By default, OpenTSDB returns 1024 lines.
    # You can't modify that number via the API.
    #
    # *Returns:*
    #
    # An array of log lines.
    def logs
      response = get_http '/logs?json=true'
      MultiJson.load response
    end

    # Queries the instance for a graph. 3 (useful) formats are supported:
    #
    # * ASCII returns data that is suitable for graphing or otherwise interpreting on the client
    # * JSON returns meta data for the query
    # * PNG returns a PNG image you can render on the client
    #
    #
    # Params (See http://opentsdb.net/http-api.html#/q_Parameters for more information):
    #
    #
    # options a hash which may include the following keys:
    #
    # * format (one of json, ascii, png), defaults to json.
    # * start	The query's start date. (required)
    # * end	The query's end date.
    # * m	The query itself. (required, may be an array)
    # * o	Rendering options.
    # * wxh	The dimensions of the graph.
    # * yrange	The range of the left Y axis.
    # * y2range	The range of the right Y axis.
    # * ylabel	Label for the left Y axis.
    # * y2label	Label for the right Y axis.
    # * yformat	Format string for the left Y axis.
    # * y2format	Format string for the right Y axis.
    # * ylog	Enables log scale for the left Y axis.
    # * y2log	Enables log scale for the right Y axis.
    # * key	Options for the key (legend) of the graph.
    # * nokey	Removes the key (legend) from the graph.
    # * nocache	Forces TSD to ignore cache and fetch results from HBase.
    #
    # The syntax for metrics (m) (square brackets indicate an optional part):
    #
    # AGG:[interval-AGG:][rate:]metric[{tag1=value1[,tag2=value2...]}]
    def query options = {}
      format = options.delete(:format) || options.delete('format') || 'json'
      options[format.to_sym] = true
      params   = query_params(options, [:start, :m])
      response = get_http "/q?#{params}"

      if format.to_sym == :json
        MultiJson.load response
      else
        response
      end
    end

    # Run multiple parallel queries
    #
    # Takes an array of option hashes in the same format as the query method.
    #
    # * threads - maximum number of parallel connections
    def multi_query opts, threads=4
      if opts.nil? or opts.empty?
        return opts
      end

      reqs = []
      opts.each do |options|
        format = options.delete(:format) || options.delete('format') || 'json'
        options[format.to_sym] = true
        params   = query_params(options, [:start, :m])
        reqs << "/q?#{params}"
      end

      responses = multi_get_http(reqs)
      ret = []
      responses.each_with_index do |response, i|
        if reqs[i] =~ /json/ then
          ret << MultiJson.load(response)
        else
          ret << response
        end
      end

      return ret
    end

    # Stats about the OpenTSDB server itself.
    #
    # Returns:
    # An array of stats.
    def stats
      response = get_http '/stats?json'
      MultiJson.load response
    end

    # Returns suggestions for metric or tag names.
    #
    # Params:
    # * query: the string to search for
    # * type: the type of item to search for (defaults to metrics)
    # Type can be one of the following:
    # * metrics: Provide suggestions for metric names.
    # * tagk: Provide suggestions for tag names.
    # * tagv: Provide suggestions for tag values.
    #
    # Returns:
    # An array of suggestions
    def suggest query, type = 'metrics'
      response = get_http "/suggest?q=#{query}&type=#{type}"
      MultiJson.load response
    end

    # Format
    # put <metric> <tisse> <value> host=<hostname>
    # put proc.loadavg.5m 1305308654 0.01 host=i-00000106
    def metric name, value, ts = Time.now, tags = {}
      tags ||= {}
      tag_str = tags.collect { |k, v| "%s=%s" % [k, v] }.join(" ")
      if !tag_str.empty?
        tag_str = " #{tag_str}"
      end
      message = "put #{name} #{ts.to_i} #{value}#{tag_str}\n"
      socket_write(message)
      true
    end

    # Returns the version of OpenTSDB
    #
    # Returns
    # An array with the version information
    def version
      response = get_http '/version?json'
      MultiJson.load response
    end

    # Parses a query param hash into a query string as expected by OpenTSDB
    # *Params:*
    # * params the parameters to parse into a query string
    # * requirements: any required parameters
    # *Returns:*
    # A query string
    # Raises:
    # ArgumentError if a required parameter is missing
    def query_params params = {}, requirements = []
      query = []

      requirements.each do |req|
        unless params.keys.include?(req.to_sym) || params.keys.include?(req.to_s)
          raise ArgumentError.new("#{req} is a required parameter.")
        end
      end

      params.each_pair do |k,v|
        if v.respond_to? :each
          v.each do |subv|
            query << "#{k}=#{subv}"
          end
        else
          v = v.strftime('%Y/%m/%d-%H:%M:%S') if v.respond_to? :strftime
          query << "#{k}=#{v}"
        end
      end
      query.join '&'
    end


    private

    def get_http(path)
      return thread_get_http(path_to_uri(path)).first
    end

    def multi_get_http(paths)
      paths = [ paths ] if not paths.kind_of? Array
      uris = []
      paths.each do |path|
        uris << path_to_uri(path)
      end
      return thread_get_http(uris)
    end

    def thread_get_http(uris, num_threads=4)
      uris = [ uris ] if not uris.kind_of? Array
      results = []
      ret = {}

      Batch.new(uris).each_with_index(num_threads) do |batch, batch_pad|

        threads = []

        batch.each_with_index do |uri, i|
          threads << Thread.new do
            num = i+batch_pad
            ret[num] = Curl::Easy.http_get(uri).body_str
          end
        end

        threads.each do |t|
          t.join
        end

      end

      (0..uris.size-1).each do |i|
        results << ret[i]
      end

      return results
    end

    def path_to_uri(path)
      path = path[1..-1] if path[0..0] == "/"
      return "http://%s:%i/%s" % [@host, @port, path]
    end

    def client
      @client ||= TCPSocket.new(@host, @port)
    end

    def socket_write(msg)
      c = 0
      loop do

        c += 1
        begin
          client.sendmsg(msg)
          return
        rescue Exception => ex
          @client = nil
          raise ex if c > 3
        end

      end # loop
    end # write

  end # Client

  # Helper for multi_query
  class Batch < Array
    def each(batch_size, &block)
      loops = (size().to_f / batch_size).ceil
      (0..loops-1).each do |l|
        batch = slice(l*batch_size, batch_size)
        yield(batch)
      end
    end

    def each_with_index(batch_size, &block)
      c = 0
      each(batch_size) do |batch|
        yield(batch.to_a, c)
        c += batch_size
      end
    end
  end

end
