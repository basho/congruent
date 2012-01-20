#!/usr/bin/env ruby

require 'rubygems'
require 'bundler/setup'
require 'optparse'

require 'riak'
require 'base64'

Riak.disable_list_keys_warnings = true

module Congruent
  module Commands
    def get(options)
      bucket, key = extract_bkey(options)
      begin
        object = client[bucket].get(key, options.symbolize_keys)
        {:siblings => object.siblings.map {|o| o.raw_data } }
      rescue Riak::FailedRequest => fr
        if fr.not_found?
          :not_found
        else
          raise
        end
      end
    end

    def put(options)
      bucket, key = extract_bkey(options)
      value = decode options.delete('value')
      object = client[bucket].get_or_new(key, options.symbolize_keys)
      object.content_type = options.delete('content_type') || 'application/octet-stream'
      object.raw_data = value
      result = object.store(options.symbolize_keys)
      if options['returnbody']
        {:siblings => result.siblings.map {|o| o.raw_data } }
      else
        true
      end
    end

    def delete(options)
      bucket, key = extract_bkey(options)
      client[bucket].delete(key, options.symbolize_keys)
      true
    end

    def keys(options)
      bucket = decode options.delete('bucket')
      { :keys => client[bucket].keys }
    end

    def ping(options)
      client.ping
      true
    end

    private
    def extract_bkey(options)
      [ decode(options.delete('bucket')),
        decode(options.delete('key')) ]
    end

    def decode(v)
      Base64.decode64(v)
    end
  end

  class CommandRunner
    include Commands
    attr_accessor :client, :input_file, :output_file

    def initialize(options = {})
      @input_file, @output_file = options.delete(:input_file), options.delete(:output_file)
      @client = Riak::Client.new(options)
    end

    def run
      commands = JSON.parse(File.read(input_file))
      results = []
      commands.each_with_index do |cmd, i|
        command = cmd.delete('command')
        begin
          if respond_to?(command)
            results[i] = send(command, cmd)
          else
            results[i] = :unimplemented
          end
        rescue => e
          results[i] = {:error => e.message}
        end
      end
      report(results)
      errors = results.select {|r| r == :unimplemented || (Hash === r && r[:error]) }
      errors.length
    end

    def report(results)
      File.open(output_file, "w") do |f|
        results.each do |result|
          report_result(result, f)
        end
      end
    end

    def report_result(result, f)
      case result
      when :unimplemented
        f.puts "{error, #{result}}."
      when TrueClass
        f.puts "ok."
      when Symbol
        f.puts "{ok, #{result}}."
      when Hash
        if result[:error]
          f.puts "{error, #{result[:error]}."
        elsif result[:keys]
          f.puts "{ok, [#{ result[:keys].map {|k| k.inspect } }]}."
        elsif result[:siblings]
          f.puts "{ok ,[#{ result[:siblings].map {|s| to_binary(s) }.join(',') }]}."
        else
          $stderr.puts "Didn't know how to encode #{result.inspect}"
          f.puts "{error, #{to_binary(result.inspect)}}."
        end
      else
        $stderr.puts "Didn't know how to encode #{result.inspect}"
        f.puts "{error, #{to_binary(result.inspect)}}."
      end
    end

    def to_binary(s)
      if s
        "<<" << s.each_byte.map {|b| b.to_s }.join(",") << ">>"
      else
        "<<>>"
      end
    end
  end
end

# -f InputFile
# -o OutputFile
# -h 127.0.0.1:8098:8087

options = {}
getopt = OptionParser.new do |o|
  o.on("-f FILE") do |file|
    options[:input_file] = file
    options[:output_file] ||= file + ".out"
  end

  o.on("-o OUTFILE") do |outfile|
    options[:output_file] = outfile
  end

  o.on("-h HOST:HTTP:PBC") do |h|
    host, http, pbc = h.split(/:/)
    options[:host] = host
    options[:http_port] = http
    options[:pb_port] = pbc
  end
end

begin
  getopt.parse ARGV
rescue OptionParser::ParseError => e
  puts getopt
  puts
  puts e.message
  exit 127
end

if [:input_file, :output_file, :host, :http_port, :pb_port].all? {|o| options[o] }
  exit Congruent::CommandRunner.new(options).run
else
  puts getopt
  puts
  puts "Missing mandatory arguments!"
  exit 127
end
