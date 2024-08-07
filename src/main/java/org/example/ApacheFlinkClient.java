package org.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

import com.google.gson.Gson;

public class ApacheFlinkClient {
    public static final int WINDOW_SIZE = 40;
    public static final int SLIDE_SIZE = 20;
    public static final double MIN_SUP = 0.1;

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // ソケットからのストリームを受け取る
        DataStream<String> socketStream = env.socketTextStream("localhost", 8080);

        // クライアントに送るためにWebSocketServerを起動
        FlinkWebSocketServer server = new FlinkWebSocketServer(8887);
        server.start();

        // データストリームにタイムスタンプを付与
        DataStream<String> purchasingStream = socketStream.map(new MapFunction<String, String>(){
            private final SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss");
            @Override
            public String map(String text) {
                String timestamp = dateFormat.format(new Date());
                return text + "," + timestamp;
            }
        });

        purchasingStream
                .countWindowAll(WINDOW_SIZE, SLIDE_SIZE)
                .process(new WindowFunction())
                .addSink(new WebSocketSink(server));

        env.execute("Flink Window Job");

    }

    // データストリームの処理のためのクラス
    // ProcessAllWindowFunction<入力の型, 出力の型, Windowの形式>
    public static class WindowFunction extends ProcessAllWindowFunction<String, String, GlobalWindow> {
        @Override
        public void process(Context context, Iterable<String> elements, Collector<String> out) {
            // 受け取ったwindowを処理しやすい形に整形
            List<Map<String, Object>> transactions = new ArrayList<>();
            for (String element : elements) {
                String[] parts = element.split(",");
                String timestamp = parts[parts.length - 1];
                String[] items = Arrays.copyOf(parts, parts.length - 1);
                Set<String> transaction = new HashSet<>(Arrays.asList(items));
                Map<String, Object> transactionMap = new HashMap<>();
                transactionMap.put("items", transaction);
                transactionMap.put("timestamp", timestamp);
                transactions.add(transactionMap);
            }

            // アプリオリアルゴリズムを用いて、頻出パターンをマイニングする
            double minSupport = WINDOW_SIZE * MIN_SUP; // 最小サポート値
            Map<Set<String>, Integer> frequentPatterns = apriori(transactions, minSupport);

            // JSON形式で結果を出力
            Map<String, Object> jsonData = new HashMap<>();
            jsonData.put("transactions", transactions);
            jsonData.put("frequent_patterns", frequentPatterns);
            jsonData.put("window_size", WINDOW_SIZE);
            jsonData.put("slide_size", SLIDE_SIZE);
            jsonData.put("min_support", minSupport);

            Gson gson = new Gson();
            String jsonOutput = gson.toJson(jsonData);
            out.collect(jsonOutput);
        }
        //  アプリオリアルゴリズムの実装
        //　トランザクションと最小サポート値を受け取って、頻出パターンとその頻度を返す
        private Map<Set<String>, Integer> apriori(List<Map<String, Object>> transactions, double minSupport) {
            //　結果を格納するハッシュマップ
            Map<Set<String>, Integer> frequentPatterns = new HashMap<>();
            //　頻度を数える際に使用するハッシュマップ
            Map<Set<String>, Integer> itemCount = new HashMap<>();

            // トランザクション内のすべてのアイテムの頻度を数えてitemCountに格納する
            for (Map<String, Object> transactionMap : transactions) {
                Set<String> transaction = (Set<String>) transactionMap.get("items");
                for (String item : transaction) {
                    Set<String> itemSet = new HashSet<>(Collections.singletonList(item));
                    itemCount.put(itemSet, itemCount.getOrDefault(itemSet, 0) + 1);
                }
            }

            // 現時点での頻出なアイテムセットを格納するリスト
            List<Set<String>> currentL = new ArrayList<>();

            //　頻度が最小サポート値以上のアイテムセットを抽出する
            for (Map.Entry<Set<String>, Integer> entry : itemCount.entrySet()) {
                if (entry.getValue() >= minSupport) {
                    frequentPatterns.put(entry.getKey(), entry.getValue());
                    currentL.add(entry.getKey());
                }
            }

            // 生成されなくなるまで要素数k個の頻繁なアイテムセットから、要素数k+1の候補アイテムセットの生成を繰り返す
            while (!currentL.isEmpty()) {
                List<Set<String>> nextL = new ArrayList<>();

                for (int i = 0; i < currentL.size(); i++) {
                    for (int j = i + 1; j < currentL.size(); j++) {
                        Set<String> unionSet = new HashSet<>(currentL.get(i));
                        unionSet.addAll(currentL.get(j));
                        if (unionSet.size() == currentL.get(i).size() + 1) {
                            itemCount.put(unionSet, 0);
                            nextL.add(unionSet);
                        }
                    }
                }

                // 要素数k+1のアイテムセットの頻度を数えて、itemCountに格納する
                for (Map<String, Object> transactionMap : transactions) {
                    Set<String> transaction = (Set<String>) transactionMap.get("items");
                    for (Set<String> itemSet : nextL) {
                        if (transaction.containsAll(itemSet)) {
                            itemCount.put(itemSet, itemCount.get(itemSet) + 1);
                        }
                    }
                }

                // サポート値が閾値を超えるアイテムセットを抽出
                currentL.clear();
                for (Set<String> itemSet : nextL) {
                    if (itemCount.get(itemSet) >= minSupport) {
                        frequentPatterns.put(itemSet, itemCount.get(itemSet));
                        currentL.add(itemSet);
                    }
                }
            }

            return frequentPatterns;
        }
    }
    //　結果を送信するためのクラス
    public static class WebSocketSink implements SinkFunction<String> {
        private final FlinkWebSocketServer server;

        public WebSocketSink(FlinkWebSocketServer server) {
            this.server = server;
        }

        @Override
        public void invoke(String value, Context context) {
            server.broadcast(value);
        }
    }
}
