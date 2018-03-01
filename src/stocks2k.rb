#!/usr/bin/env ruby

# Only calculate one per day

require 'json'
require 'csv'
require 'open-uri'
require 'forex_quotes'
require 'quandl'
# from ruby-kafka
require 'kafka'

iforge_apikey = ENV['IFORGE_APIKEY']
kafka_broker = ENV['KAFKA_BROKER'].nil? ? "127.0.0.1" : ENV['KAFKA_BROKER']
kafka_port = ENV['KAFKA_PORT'].nil? ? "9092" : ENV['KAFKA_PORT']
kafka_topic = ENV['KAFKA_TOPIC'].nil? ? "itrading" : ENV['KAFKA_TOPIC']
kclient = Kafka.new(seed_brokers: ["#{kafka_broker}:#{kafka_port}"], client_id: "quandl2k")
iforge_client = ForexDataClient.new(iforge_apikey)

while true
	# Petroleo, barril de brent
	# https://www.quandl.com/data/EIA/PET_RBRTE_D-Europe-Brent-Spot-Price-FOB-Daily
	data = Quandl::Dataset.get('EIA/PET_RBRTE_D').data(params: {limit: 1})
	# {:meta=>{"limit"=>1, "transform"=>nil, "column_index"=>nil, "column_names"=>["Date", "Value"], "start_date"=>Wed, 20 May 1987, "end_date"=>Mon, 12 Feb 2018, "frequency"=>"daily", "collapse"=>nil, "order"=>nil}, :values=>[{"date"=>Mon, 12 Feb 2018, "value"=>62.2}]}
	itrading =	{
			"url" => "https://www.quandl.com/data/EIA/PET_RBRTE_D-Europe-Brent-Spot-Price-FOB-Daily",
			"sensor" => "quandl",
			"timestamp" => data.values.first.date.to_time.to_i,
			"metic" => "PET_RBRTE_D",
			"value" => data.values.first.value
			}
	kclient.deliver_message("#{itrading.to_json}",topic: kafka_topic)
	
	# Oro, LBMA/GOLD
	# https://www.quandl.com/data/LBMA/GOLD-Gold-Price-London-Fixing
	data = Quandl::Dataset.get('LBMA/GOLD').data(params: {limit: 1})
	# {:meta=>{"limit"=>1, "transform"=>nil, "column_index"=>nil, "column_names"=>["Date", "USD (AM)", "USD (PM)", "GBP (AM)", "GBP (PM)", "EURO (AM)", "EURO (PM)"], "start_date"=>Tue, 02 Jan 1968, "end_date"=>Thu, 22 Feb 2018, "frequency"=>"daily", "collapse"=>nil, "order"=>nil}, :values=>[{"date"=>Thu, 22 Feb 2018, "usd_am"=>1323.5, "usd_pm"=>nil, "gbp_am"=>952.66, "gbp_pm"=>nil, "euro_am"=>1076.4, "euro_pm"=>nil}]} 
	itrading =	{
			"url" => "https://www.quandl.com/data/LBMA/GOLD-Gold-Price-London-Fixing",
			"sensor" => "quandl",
			"timestamp" => data.values.first.date.to_time.to_i,
			"metric" => "GOLD_AM",
			"value" => data.values.first.euro_am
			}
	kclient.deliver_message("#{itrading.to_json}",topic: kafka_topic)
	itrading =	{
			"url" => "https://www.quandl.com/data/LBMA/GOLD-Gold-Price-London-Fixing",
			"sensor" => "quandl",
			"timestamp" => data.values.first.date.to_time.to_i,
			"metric" => "GOLD_PM",
			"value" => data.values.first.euro_pm
			}
	kclient.deliver_message("#{itrading.to_json}",topic: kafka_topic)

	
	# Plata, LBMA/SILVER
	# https://www.quandl.com/data/LBMA/SILVER-Silver-Price-London-Fixing
	data = Quandl::Dataset.get('LBMA/SILVER').data(params: {limit: 1})
	# {:meta=>{"limit"=>1, "transform"=>nil, "column_index"=>nil, "column_names"=>["Date", "USD", "GBP", "EURO"], "start_date"=>Tue, 02 Jan 1968, "end_date"=>Wed, 21 Feb 2018, "frequency"=>"daily", "collapse"=>nil, "order"=>nil}, :values=>[{"date"=>Wed, 21 Feb 2018, "usd"=>16.435, "gbp"=>11.8, "euro"=>13.35}]}
	itrading =	{
	                "url" => "https://www.quandl.com/data/LBMA/SILVER-Silver-Price-London-Fixing",
	                "sensor" => "quandl",
	                "timestamp" => data.values.first.date.to_time.to_i,
	                "metric" => "SILVER",
	                "value" => data.values.first.euro
	                }
	kclient.deliver_message("#{itrading.to_json}",topic: kafka_topic)



	# SymbolList = ["EURUSD", "USDJPY", "GBPUSD", "USDCHF", "EURCHF", "AUDUSD", "USDCAD", "NZDUSD", "EURGBP", "EURJPY", "GBPJPY", "CHFJPY", "GBPCHF", "EURAUD", "EURCAD", "AUDCAD", "AUDJPY", "CADJPY", "NZDJPY", "GBPCAD", "GBPNZD", "GBPAUD", "AUDNZD", "USDSEK", "EURSEK", "EURNOK", "USDNOK", "USDMXN", "AUDCHF", "EURNZD", "USDZAR", "ZARJPY", "USDTRY", "EURTRY", "NZDCHF", "CADCHF", "NZDCAD", "TRYJPY", "USDCNH", "XAUUSD", "XAGUSD", "USDEUR", "USDGBP", "USDAUD", "USDNZD", "USDXAU", "USDXAG", "EURMXN", "EURZAR", "EURCNH", "EURXAU", "EURXAG", "JPYUSD", "JPYEUR", "JPYGBP", "JPYCHF", "JPYAUD", "JPYCAD", "JPYNZD", "JPYSEK", "JPYNOK", "JPYMXN", "JPYZAR", "JPYTRY", "JPYCNH", "JPYXAU", "JPYXAG", "GBPEUR", "GBPSEK", "GBPNOK", "GBPMXN", "GBPZAR", "GBPTRY", "GBPCNH", "GBPXAU", "GBPXAG", "CHFUSD", "CHFEUR", "CHFGBP", "CHFAUD", "CHFCAD", "CHFNZD", "CHFSEK", "CHFNOK", "CHFMXN", "CHFZAR", "CHFTRY", "CHFCNH", "CHFXAU", "CHFXAG", "AUDEUR", "AUDGBP", "AUDSEK", "AUDNOK", "AUDMXN", "AUDZAR", "AUDTRY", "AUDCNH", "AUDXAU", "AUDXAG", "CADUSD", "CADEUR", "CADGBP", "CADAUD", "CADNZD", "CADSEK", "CADNOK", "CADMXN", "CADZAR", "CADTRY", "CADCNH", "CADXAU", "CADXAG", "NZDEUR", "NZDGBP", "NZDAUD", "NZDSEK", "NZDNOK", "NZDMXN", "NZDZAR", "NZDTRY", "NZDCNH", "NZDXAU", "NZDXAG", "SEKUSD", "SEKEUR", "SEKJPY", "SEKGBP", "SEKCHF", "SEKAUD", "SEKCAD", "SEKNZD", "SEKNOK", "SEKMXN", "SEKZAR", "SEKTRY", "SEKCNH", "SEKXAU", "SEKXAG", "NOKUSD", "NOKEUR", "NOKJPY", "NOKGBP", "NOKCHF", "NOKAUD", "NOKCAD", "NOKNZD", "NOKSEK", "NOKMXN", "NOKZAR", "NOKTRY", "NOKCNH", "NOKXAU", "NOKXAG", "MXNUSD", "MXNEUR", "MXNJPY", "MXNGBP", "MXNCHF", "MXNAUD", "MXNCAD", "MXNNZD", "MXNSEK", "MXNNOK", "MXNZAR", "MXNTRY", "MXNCNH", "MXNXAU", "MXNXAG", "ZARUSD", "ZAREUR", "ZARGBP", "ZARCHF", "ZARAUD", "ZARCAD", "ZARNZD", "ZARSEK", "ZARNOK", "ZARMXN", "ZARTRY", "ZARCNH", "ZARXAU", "ZARXAG", "TRYUSD", "TRYEUR", "TRYGBP", "TRYCHF", "TRYAUD", "TRYCAD", "TRYNZD", "TRYSEK", "TRYNOK", "TRYMXN", "TRYZAR", "TRYCNH", "TRYXAU", "TRYXAG", "CNHUSD", "CNHEUR", "CNHJPY", "CNHGBP", "CNHCHF", "CNHAUD", "CNHCAD", "CNHNZD", "CNHSEK", "CNHNOK", "CNHMXN", "CNHZAR", "CNHTRY", "CNHXAU", "CNHXAG", "XAUEUR", "XAUJPY", "XAUGBP", "XAUCHF", "XAUAUD", "XAUCAD", "XAUNZD", "XAUSEK", "XAUNOK", "XAUMXN", "XAUZAR", "XAUTRY", "XAUCNH", "XAUXAG", "XAGEUR", "XAGJPY", "XAGGBP", "XAGCHF", "XAGAUD", "XAGCAD", "XAGNZD", "XAGSEK", "XAGNOK", "XAGMXN", "XAGZAR", "XAGTRY", "XAGCNH", "XAGXAU"]
	# [{"symbol"=>"EURUSD", "price"=>1.23176, "bid"=>1.23176, "ask"=>1.23176, "timestamp"=>1519735980}, {"symbol"=>"USDJPY", "price"=>107.0125, "bid"=>107.012, "ask"=>107.013, "timestamp"=>1519735980},
	
	# Walk through all symbols and get quotes
	
	symbols = iforge_client.getSymbols
	quotes = iforge_client.getQuotes(symbols)
	# [{"symbol"=>"EURUSD", "price"=>1.23176, "bid"=>1.23176, "ask"=>1.23176, "timestamp"=>1519735980}, {"symbol"=>"USDJPY", "price"=>107.0125, "bid"=>107.012, "ask"=>107.013, "timestamp"=>1519735980}, ...]
	
	quotes.each do |quote|
		# {"symbol"=>"EURUSD", "price"=>1.23176, "bid"=>1.23176, "ask"=>1.23176, "timestamp"=>1519735980}
		itrading = {
			"url" => "https://forex.1forge.com/1.0.3/quotes?pairs=#{quote["symbol"]}&api_key=APIKEY",
			"sensor" => "1forge",
			"timestamp" => quote["timestamp"],
			"metric" => quote["symbol"],
			"value" => quote["price"]
			}
		kclient.deliver_message("#{itrading.to_json}",topic: kafka_topic)
	end

	# wait for next day
	stime = 86400 - (Time.now.to_i % 86400)
	sleep stime
end
