package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class KafkaProcessor {
    private static HashMap<String, Integer> descrptionCnt = new HashMap<>();
    public static JSONObject fetchWeatherData(String cityName, String apiKey) {
        String apiUrl = "https://api.openweathermap.org/data/2.5/weather?q=" + cityName + "&appid=" + apiKey;
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(apiUrl))
                .build();
        CompletableFuture<HttpResponse<String>> futureResponse = client.sendAsync(request, HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> response;
        JSONObject weatherData = new JSONObject();
        try {
            response = futureResponse.get(); // This blocks until the response is available
            JSONObject json = new JSONObject(response.body());
            weatherData.put("location",json.get("name").toString());
            String time = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX")
                    .withZone(ZoneId.systemDefault())
                    .format(Instant.ofEpochSecond(Long.parseLong(json.get("dt").toString())));
            weatherData.put("dateTime",time);
            JSONObject ob = (JSONObject) json.get("main");
            weatherData.put("temp", Double.parseDouble(String.valueOf(ob.getBigDecimal("temp"))));
            JSONArray weatherList = (JSONArray) json.get("weather");
            JSONObject weatherDesc = (JSONObject) weatherList.get(0);
            descrptionCnt.putIfAbsent(weatherDesc.getString("description"), descrptionCnt.getOrDefault(weatherDesc.getString("description"), 0) + 1);
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }
        return weatherData;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties kafkaProps = new Properties();
        kafkaProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

        Properties prop = new Properties();
        try (InputStream fis = KafkaProcessor.class.getClassLoader().getResourceAsStream("env.properties")) {
            prop.load(fis);
        } catch (FileNotFoundException ex) {
            throw ex;
        } catch (IOException ex) {
            throw new IOException();
        }
        String[] cityName= prop.getProperty("locations").split(",");
        String apiKey = prop.getProperty("app_key");
        long timeout = Long.parseLong(prop.getProperty("frequency_sec"));
        long aggregate_period_sec = Long.parseLong(prop.getProperty("aggregate_period_sec"));
        long nextComparisonTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(aggregate_period_sec);
        double initiTemp = 0.0;
        HashMap<String, Double> temp = new HashMap<>();
        double initGlobalTemp = 0.0;
        while (true) {

            if (System.currentTimeMillis() >= nextComparisonTime) {
                Properties prop2 = new Properties();
                double total = 0;
                for(String city : cityName){
                    JSONObject data = fetchWeatherData(city, apiKey);
                    data.put("avg_temperature", temp.get(city) / ((double) aggregate_period_sec / timeout));
                    data.put("temperature_diff", data.getDouble("temp") - initiTemp);
                    total += data.getDouble("avg_temperature");
                    int max = 0;
                    String ans = "";
                    for(String desc : descrptionCnt.keySet()){
                        if(max < descrptionCnt.get(desc)){
                            ans = desc;
                            max = descrptionCnt.get(desc);
                        }
                    }
                    data.put("description", ans);
                    producer.send(new ProducerRecord<>("Weather-data", data.toString()));
                    temp.put(city,0.0);
                }
                prop2.put("dateTime", DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssXXX")
                        .withZone(ZoneId.systemDefault())
                        .format(Instant.ofEpochSecond(Instant.now().getEpochSecond())));
                prop2.put("avg_global_temperature", total / cityName.length);
                prop2.put("temperture_change", (Double) prop2.get("avg_global_temperature") - initGlobalTemp);
                initGlobalTemp = (Double) prop2.get("avg_global_temperature");
                producer.send(new ProducerRecord<>("Weather-data-2", prop2.toString()));
                initiTemp = 0.0;
                descrptionCnt = new HashMap<>();
                nextComparisonTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(aggregate_period_sec);
            }
            for(String city : cityName){
                JSONObject data = fetchWeatherData(city, apiKey);
                if(initiTemp == 0){
                    initiTemp = data.getDouble("temp");
                }
                temp.putIfAbsent(city, temp.getOrDefault(city, 0.0) + data.getDouble("temp"));
            }
            TimeUnit.SECONDS.sleep(timeout);
        }
    }
}
