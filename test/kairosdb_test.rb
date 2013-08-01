
require "test_helper"

class KairosDBTest < MiniTest::Unit::TestCase

  include WebMock::API


  def test_get
    stub = stub_request(:post, "http://localhost:8081/api/v1/datapoints/query").with{ |req|
      obj = MultiJson.load(req.body)
      ret = true
      %w{start_absolute end_absolute metrics}.each{ |k| next if !ret; ret = obj.include? k }
      ret && req.body =~ /foo.bar.df/
      }.to_return {
        { :status => 200, :body => '{"queries":[{"results":[{"name":"foo.bar.df","tags":{"foo":["bar"]},"values":[[1375189490000,3.200000047683716],[1375189940000,3.200000047683716]]}]}]}' }
      }

    client = Continuum::KairosDB.new("localhost", 4343, 8081)
    ret = client.get({ :key => "foo.bar.df", :start_time => (Time.new.to_i-86400*7)*1000, :end_time => Time.new.to_i*1000 })

    assert_kind_of Hash, ret
    assert_equal "foo.bar.df", ret["name"]
    assert_equal "bar", ret["tags"]["foo"].first
    assert_equal 2, ret["values"].size
    assert_equal 1375189490000, ret["values"].first.first
  end

  def test_get_empty
    stub = stub_request(:post, "http://localhost:8081/api/v1/datapoints/query").with{ |req|
      obj = MultiJson.load(req.body)
      ret = true
      %w{start_absolute end_absolute metrics}.each{ |k| next if !ret; ret = obj.include? k }
      ret && req.body =~ /foo.bar.gg/
      }.to_return {
        { :status => 200, :body => %q/{"errors":["org.kairosdb.core.exception.DatastoreException: net.opentsdb.uid.NoSuchUniqueName: No such name for 'metrics': 'foo.bar.gg'"]}/ }
      }

    client = Continuum::KairosDB.new("localhost", 4343, 8081)
    ret = client.get({ :key => "foo.bar.gg", :start_time => (Time.new.to_i-86400*7)*1000, :end_time => Time.new.to_i*1000 })

    assert_kind_of Hash, ret
    assert_empty ret
  end

end
