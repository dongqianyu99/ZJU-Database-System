import utils.ConnectConfig;
import utils.DatabaseConnector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.logging.Logger;
import java.io.OutputStream;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import queries.ApiResult;
import queries.CardList;
import entities.*;

import com.sun.net.httpserver.Headers;
import com.google.gson.*;

public class Main {

    private static final Logger log = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws IOException {
        try {
            // parse connection config from "resources/application.yaml"
            ConnectConfig conf = new ConnectConfig();
            log.info("Success to parse connect config. " + conf.toString());
            // connect to database
            DatabaseConnector connector = new DatabaseConnector(conf);
            boolean connStatus = connector.connect();
            if (!connStatus) {
                log.severe("Failed to connect database.");
                System.exit(1);
            }

            /* do somethings */
            LibraryManagementSystemImpl system = new LibraryManagementSystemImpl(connector);
            log.info("Successfully connected to database.");

            // 创建HTTP服务器，监听指定端口
            // 这里是8000，建议不要80端口，容易和其他的撞
            HttpServer server = HttpServer.create(new InetSocketAddress(8000), 0);

            // 添加handler，这里就绑定到/card路由
            // 所以localhost:8000/card是会有handler来处理
            server.createContext("/card", new CardHandler(system));
            // server.createContext("/book", new BookHandler(system));
            // server.createContext("/borrow", new BorrowHandler(system));

            // 启动服务器
            server.start();

            // 标识一下，这样才知道我的后端启动了（确信
            System.out.println("Server is listening on port 8000");

            // release database connection handler
            // if (connector.release()) {
            // log.info("Success to release connection.");
            // } else {
            // log.warning("Failed to release connection.");
            // }
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                if (connector.release()) {
                    log.info("Success to release connection.");
                } else {
                    log.warning("Failed to release connection on shutdown.");
                }
            }));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class CardHandler implements HttpHandler {
        private final LibraryManagementSystem system;

        public CardHandler(LibraryManagementSystem system) {
            this.system = system;
        }

        // 关键重写handle方法
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            // 允许所有域的请求，cors处理
            Headers headers = exchange.getResponseHeaders();
            headers.add("Access-Control-Allow-Origin", "*");
            headers.add("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
            headers.add("Access-Control-Allow-Headers", "Content-Type");
            // 解析请求的方法，看GET还是POST
            String requestMethod = exchange.getRequestMethod();
            // 注意判断要用equals方法而不是==啊，java的小坑（
            if (requestMethod.equals("GET")) {
                // 处理GET
                handleGetRequest(exchange);
            } else if (requestMethod.equals("POST")) {
                // 处理POST
                handlePostRequest(exchange);
            } else if (requestMethod.equals("OPTIONS")) {
                // 处理OPTIONS
                handleOptionsRequest(exchange);
            } else {
                // 其他请求返回405 Method Not Allowed
                exchange.sendResponseHeaders(405, -1);
            }
        }

        private static class CardRequest {
            int id; // 借书证id
            String name; // 姓名
            String department; // 部门
            String type; // 类型
            String op; // 操作
        }

        private void handleGetRequest(HttpExchange exchange) throws IOException {
            // 响应头，因为是JSON通信
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            // 状态码为200，也就是status ok
            exchange.sendResponseHeaders(200, 0);
            // 获取输出流，java用流对象来进行io操作
            OutputStream outputStream = exchange.getResponseBody();

            ApiResult result = system.showCards();

            String response;
            if (result.ok) {
                CardList cardList = (CardList) result.payload;
                response = new Gson().toJson(cardList.getCards()); // JSON 序列化
                // 写
                outputStream.write(response.getBytes());
            } else {
                response = "{\"error\": \"" + result.message + "\"}";
            }

            // 流一定要close！！！小心泄漏
            outputStream.close();
        }

        private void handlePostRequest(HttpExchange exchange) throws IOException {
            // 读取POST请求体
            String requestBody;
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))) {
                requestBody = reader.lines().collect(java.util.stream.Collectors.joining("\n"));
            }

            // 看看读到了啥
            // 实际处理可能会更复杂点
            System.out.println("Received POST request to create card with data: " + requestBody);

            CardRequest request = new Gson().fromJson(requestBody, CardRequest.class);
            // 响应头
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            // 响应状态码200
            exchange.sendResponseHeaders(200, 0);

            if ("教师".equals(request.type)) {
                request.type = "T";
            } else if ("学生".equals(request.type)) {
                request.type = "S";
            }

            ApiResult result = null;
            System.out.println("Request operation: " + request.op);
            if ("new".equals(request.op)) { // create a new card
                Card newCard = new Card();
                newCard.setName(request.name);
                newCard.setDepartment(request.department);
                newCard.setType(Card.CardType.valueOf(request.type));
                result = system.registerCard(newCard);
                if (result.ok) {
                    OutputStream outputStream = exchange.getResponseBody();
                    outputStream.write("Successfully created a card.".getBytes());
                    outputStream.close();
                } else {
                    OutputStream outputStream = exchange.getResponseBody();
                    outputStream.write("The card already exists!".getBytes());
                    outputStream.close();
                }
            } else if ("remove".equals(request.op)) { // remove a card
                System.out.println("Request id: " + request.id);
                result = system.removeCard(request.id);

                if (result.ok) {
                    System.out.println("Successfully removed the card.");
                    OutputStream outputStream = exchange.getResponseBody();
                    outputStream.write("Successfully removed the card.".getBytes());
                    outputStream.close();
                } else {
                    System.out.println("Failed to remove the card.");
                    OutputStream outputStream = exchange.getResponseBody();
                    outputStream.write(result.message.getBytes()); // try to get the message
                    outputStream.close();
                }
            } else { // invalid operation
                OutputStream outputStream = exchange.getResponseBody();
                outputStream.write("Invalid operation.".getBytes());
                outputStream.close();
            }
        }

        private void handleOptionsRequest(HttpExchange exchange) throws IOException {
            exchange.sendResponseHeaders(204, -1); // No Content
        }
    }
}
