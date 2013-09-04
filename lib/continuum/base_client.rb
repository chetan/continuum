
module Continuum
  class BaseClient

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
      @port = @http_port = port.to_i
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
      return multi_get_http(path).first
    end

    def multi_get_http(paths)
      paths = [ paths ] if not paths.kind_of? Array
      uris = paths.map { |path| path_to_uri(path) }
      return do_multi_get_http(uris)
    end

    def path_to_uri(path)
      path = path[1..-1] if path[0..0] == "/"
      return "http://%s:%i/%s" % [@host, @http_port, path]
    end


    # Socket client (for puts)

    def socket
      @socket ||= TCPSocket.new(@host, @port)
    end

    def socket_write(msg)
      c = 0
      loop do

        c += 1
        begin
          socket.sendmsg(msg)
          return
        rescue Exception => ex
          @socket = nil
          raise ex if c > 3
        end

      end # loop
    end # write

  end # BaseClient
end
