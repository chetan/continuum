
require 'bixby/api_pool'

module Continuum

  class BaseClient


    private

    # Fetch a list of URLs
    #
    # @param [Array<String>]
    def do_multi_get_http(uris)
      Bixby::APIPool.get(uris, key(uris.first))
    end

    # POST the given requests
    #
    # @param [Array<Array<String, String>>] reqs      Array of reqs, where req is [uri, body]
    def do_multi_post_http(reqs)
      reqs = reqs.map{ |r| Bixby::APIPool::Request.new(r.shift, :post, r.shift)}
      Bixby::APIPool.fetch(reqs, key(reqs.first.url))
    end

    def key(uri)
      if uri.kind_of? Array then
        uri = uri.first
      elsif uri.kind_of? URI then
        return uri.host
      end
      URI(uri).host # uri should be a string here
    end

  end # BaseClient
end # Continuum
