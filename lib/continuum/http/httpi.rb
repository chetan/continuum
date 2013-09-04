
module Continuum

  class BaseClient


    private

    # Fetch a list of URLs. HTTPI adapter fetches each serialily.
    #
    # @param [Array<String>]
    def do_multi_get_http(uris)
      uris = [uris] if not uris.kind_of? Array
      uris.map do |uri|
        puts "httpi.getting #{uri.to_s}"
        HTTPI.get(uri).body
      end
    end

    # POST the given requests. HTTPI adapter will post each serially.
    #
    # @param [Array<Array<String, String>>] reqs      Array of reqs, where req is [uri, body]
    def do_multi_post_http(reqs)
      reqs.map do |req|
        url = req.shift
        body = req.shift
        puts "httpi.posting #{url}"
        p body
        ret = HTTPI.post(HTTPI::Request.new(:url => url, :body => body))
        p ret
        p ret.body
        puts
        puts
        ret.body
      end
    end

  end # BaseClient
end # Continuum
