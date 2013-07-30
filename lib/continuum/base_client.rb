
module Continuum
  class BaseClient

    attr_accessor :thread_key

    # Create a connection to the server
    #
    # *Params:*
    #
    # * host is the host IP (defaults to localhost)
    # * port is the host's port (defaults to 4242)
    #
    # *Returns:*
    #
    # A client to play with
    def initialize(host = '127.0.0.1', port = 4242)
      @host = host
      @port = port
      @thread_key = :continuum
    end

    # Put metric
    #
    # put <metric> <tisse> <value> host=<hostname>
    # put proc.loadavg.5m 1305308654 0.01 host=i-00000106
    def put(name, value, ts = Time.now, tags = {})
      tags ||= {}
      tag_str = tags.collect { |k, v| "%s=%s" % [k, v] }.join(" ")
      if !tag_str.empty?
        tag_str = " #{tag_str}"
      end
      message = "put #{name} #{ts.to_i} #{value}#{tag_str}\n"
      socket_write(message)
      true
    end
    alias_method :metric, :put



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

    def thread_get_http(uris)
      http_thread_pool.get(uris)
    end

    def http_thread_pool(num_threads=4)
      Thread.current[@thread_key] ||= Curl::ThreadPool.new(num_threads)
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

    def path_to_uri(path)
      path = path[1..-1] if path[0..0] == "/"
      return "http://%s:%i/%s" % [@host, @port, path]
    end

  end # ClientUtil
end
