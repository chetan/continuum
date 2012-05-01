Bundler.setup :default, :test

require "continuum"
require "fakeweb"
require "minitest/spec"
require "nokogiri"
require "vcr"

require 'turn'
require 'turn/reporter'
require 'turn/reporters/outline_reporter'

Turn.config.framework = :minitest
Turn.config.format = :outline

VCR.config do |c|
  c.stub_with :fakeweb
  c.cassette_library_dir     = "test/cassettes"
  c.default_cassette_options = { :record => :none }
end

MiniTest::Unit.autorun
