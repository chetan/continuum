
module Continuum

  class BaseClient


    private

    def do_multi_get_http(uris)
      http_thread_pool.get(uris)
    end

    def do_multi_post_http(reqs)
      http_thread_pool.post(reqs)
    end

    def http_thread_pool(num_threads=4)
      Thread.current[@thread_key] ||= Curl::ThreadPool.new(num_threads)
    end

  end # BaseClient
end # Continuum
