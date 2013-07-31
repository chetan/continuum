Bundler.setup :default, :test

require "continuum"
require "webmock"
require "minitest/unit"
require "minitest/spec"
require "vcr"

require 'turn'
require 'turn/reporter'
require 'turn/reporters/outline_reporter'

Turn.config.framework = :minitest
Turn.config.format = :outline

VCR.config do |c|
  c.stub_with :webmock
  c.cassette_library_dir     = "test/cassettes"
  c.default_cassette_options = { :record => :none }
end

MiniTest::Unit.autorun
