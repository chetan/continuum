
module Continuum
  class KairosDB < BaseClient

    def initialize(host="localhost", telnet_port=4242, http_port=8080)
      super(host, telnet_port)
      @http_port = http_port
      @thread_key = :continuum_kairosdb
    end

    # Get metrics for the given options
    #
    # @param [Hash] opts
    # @option opts [String] :key          Metric key name
    # @option opts [Time] :start_time     Accepts either Time object or Fixnum (epoch in millisec)
    # @option opts [Time] :end_time       Accepts either Time object or Fixnum (epoch in millisec)
    # @option opts [Hash] :tags           Additional tags to filter by
    # @option opts [String] :agg          Aggregate function; one of: sum, max, min, avg (default="sum")
    # @option opts [String] :downsample   Whether or not to downsample values (default=nil)
    #
    # @example
    #   Downsample is a combination of a time period and an aggregation function.
    #   It takes the following form: "time-agg", where
    #     time = 1s, 1m, 1h, 1d
    #     agg = min, max, sum, avg
    #   ex: 10m-avg
    #
    # @return [Hash] metrics
    #
    # @example result
    #
    #   {"name"=>"foo.bar.df", "tags"=>{"foo"=>["bar"]}, "values"=>[[1375189490000, 3.200000047683716], [1375189940000, 3.200000047683716]]}
    #
    def get(opts)
      multi_get([opts])
    end

    def multi_get(opts, threads=4)
      queries = []
      opts.each do |opt|

        query = {}

        query[:start_absolute] = opt[:start_time]
        query[:end_absolute]   = opt[:end_time]

        metric = {
          :name => opt[:key]
        }
        query[:metrics] = [ metric ]

        if opt[:tags] and not(opt[:tags].empty?) then
          metric[:tags] = opt[:tags]
        end

        if opt[:downsample].kind_of? Hash then
          metric[:aggregators] = [ opt[:downsample] ]

        elsif opt[:downsample] =~ /^(\d+)(.*?)-(.*?)$/ then
          unit = case $2
          when "s"
            "seconds"
          when "m"
            "minutes"
          when "h"
            "hours"
          when "d"
            "days"
          when "w"
            "weeks"
          end

          metric[:aggregators] = [
            {
              :name => $3,
              :sampling => {
                :value => $1.to_i,
                :unit => unit
              }
            }
          ]
        end

        queries << query
      end

      multi_post_http(queries)
    end


    private

    def multi_post_http(queries)
      uri = path_to_uri("/api/v1/datapoints/query")
      reqs = queries.map { |q| [uri, MultiJson.dump(q)] }
      http_thread_pool.post(reqs).map do |r|
        obj = MultiJson.load(r)
        results = obj["queries"].first["results"].first
      end
    end

  end # KairosDB
end # Continuum
