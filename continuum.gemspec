# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "continuum/version"

Gem::Specification.new do |s|
  s.name        = "continuum"
  s.version     = Continuum::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Josh Kleinpeter", "Chetan Sarva"]
  s.email       = ["josh@kleinpeter.org", "chetan@pixelcop.net"]
  s.homepage    = ""
  s.summary     = %q{A Ruby gem that interfaces with OpenTSDB.}
  s.description = %q{A Ruby gem that interfaces with OpenTSDB.}

  s.rubyforge_project = "continuum"
  s.add_dependency 'rake'
  s.add_dependency 'eventmachine'
  s.add_dependency 'em-http-request', '~>1.0.2'
  s.add_development_dependency 'minitest'
  s.add_development_dependency 'webmock'
  s.add_development_dependency 'vcr', '1.5.0'
  s.add_development_dependency 'autotest-standalone'
  s.add_development_dependency 'bundler'
  s.add_development_dependency 'nokogiri'
  s.add_development_dependency 'turn'


  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  s.require_paths = ["lib"]
end
