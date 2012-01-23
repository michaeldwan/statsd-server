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
          {name: key, value: value}
        end.compact

        out[:gauges] = timers.map do |key, values|
          next if values.length == 0
          {name: key, count: values.count, sum: values.sum}
        end.compact

        post(out) if out[:counters].any? || out[:gauges].any?
      end

      def post(data)
        puts data.inspect if $options[:debug]
        attempts = 0
        begin
          attempts += 1
          uri = URI('https://metrics-api.librato.com/v1/metrics')
          request = Net::HTTP::Post.new(uri.path)
          request.body = Yajl::Encoder.encode(data)
          request["Content-Type"] = "application/json"
          # puts request.body
          request.basic_auth 'michael@snapjoy.com', '19dd9f08b29f0017de0845e704617429c34dc5c3310b1704d3e80845f24460a4'
          response = Net::HTTP.start(uri.host, uri.port, use_ssl: true) do |http|
            http.request(request)
          end
          if response.code.to_i >= 500
            raise "Unexpected response code #{response.code}: #{response.body}"
          end
        rescue Exception => e
          puts "Failed (attempt #{attempts}) #{e}"
          retry if attempts < 3
        end
      end
    end
  end
end
