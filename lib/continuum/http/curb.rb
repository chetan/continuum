
module Continuum

  class BaseClient


    private

    # Fetch a list of URLs
    #
    # @param [Array<String>]
    def do_multi_get_http(uris)
      http_thread_pool.get(uris)
    end

    # POST the given requests
    #
    # @param [Array<Array<String, String>>] reqs      Array of reqs, where req is [uri, body]
    def do_multi_post_http(reqs)
      http_thread_pool.post(reqs)
    end

    def http_thread_pool(num_threads=4)
      Thread.current[@thread_key] ||= Curl::ThreadPool.new(num_threads)
    end

  end # BaseClient
end # Continuum
