require 'benchmark'
require 'eventmachine'
require 'net/http'
require 'yajl' 

module StatsdServer
  class LibratoMetricsStore
    class << self
      def flush!(counters, timers)
        StatsdServer.logger "Flushing #{counters.count} counters and #{timers.count} timers to Librato Metrics\n"
        @now = Time.now.to_i
        
        out = {}

        out[:counters] = counters.map do |key, value|
          next if value.zero?
          name, source = key.split("#")
          metric = {name: name, value: value}
          metric[:source] = source if source
          metric
        end.compact

        out[:gauges] = timers.map do |key, values|
          next if values.length == 0
          name, source = key.split("#")
          metric = {name: name, count: values.count, sum: values.sum}
          metric[:source] = source if source
          metric
        end.compact

        post(out) if out[:counters].any? || out[:gauges].any?
      end

      def post(data)
        puts data.inspect if $options[:debug]
        puts "auth: '#{ENV["LIBRATO_USER"]}', '#{ENV["LIBRATO_API_KEY"]}'" if $options[:debug]
        attempts = 0
        begin
          attempts += 1
          uri = URI('https://metrics-api.librato.com/v1/metrics')
          request = Net::HTTP::Post.new(uri.path)
          request.body = Yajl::Encoder.encode(data)
          request["Content-Type"] = "application/json"
          request.basic_auth ENV["LIBRATO_USER"], ENV["LIBRATO_API_KEY"]
          response = Net::HTTP.start(uri.host, uri.port, use_ssl: true) do |http|
            http.request(request)
          end
          if response.code.to_i >= 500
            raise "Unexpected response code #{response.code}: #{response.body}"
          elsif response.code.to_i == 200
            puts "successfully posted"
          else
            puts "post error (#{response.code}}): #{response.body}"
          end
        rescue Exception => e
          puts "Failed (attempt #{attempts}) #{e}"
          retry if attempts < 3
        end
      end
    end
  end
end
