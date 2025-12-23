#include "crow.h"
#include <pqxx/pqxx>
#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <vector>

const std::string DB_CONN_STR = "postgresql://user:password@orders-db:5432/orders_db";
const std::string AMPQ_HOST = "rabbitmq";
const std::string QUEUE_NAME = "orders_queue";
const std::string RESULTS_QUEUE = "results_queue";

std::string ExtractJsonString(const std::string &json, const std::string &key) {
    std::string search = "\"" + key + "\"";
    size_t start = json.find(search);
    if (start == std::string::npos) return "";
    size_t col = json.find(":", start);
    if (col == std::string::npos) return "";
    size_t val_start = json.find("\"", col);
    if (val_start == std::string::npos) return "";
    val_start++;
    size_t val_end = json.find("\"", val_start);
    if (val_end == std::string::npos) return "";
    return json.substr(val_start, val_end - val_start);
}

long ExtractJsonValue(const std::string &json, const std::string &key) {
    std::string search = "\"" + key + "\"";
    size_t start_pos = json.find(search);
    if (start_pos == std::string::npos) return -1;
    size_t col_pos = json.find(":", start_pos);
    if (col_pos == std::string::npos) return -1;
    size_t val_start = col_pos + 1;
    while (val_start < json.length() && !isdigit(json[val_start]) && json[val_start] != '-') val_start++;
    if (val_start >= json.length()) return -1;
    size_t val_end = val_start;
    while (val_end < json.length() && isdigit(json[val_end])) val_end++;
    std::string val = json.substr(val_start, val_end - val_start);
    try { return std::stol(val); } catch (...) { return -1; }
}

class OrdersRepository {
    std::string connection_string_;

 public:
    OrdersRepository(std::string conn_str) : connection_string_(std::move(conn_str)) {}

    void InitializeSchema(pqxx::connection &c) {
        pqxx::work w(c);
        w.exec0(
            "CREATE TABLE IF NOT EXISTS orders (id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL, amount BIGINT NOT NULL, status VARCHAR(20) NOT NULL DEFAULT 'NEW', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);");
        w.exec0(
            "CREATE TABLE IF NOT EXISTS outbox (id SERIAL PRIMARY KEY, event_type VARCHAR(50) NOT NULL, payload JSONB NOT NULL, status VARCHAR(20) NOT NULL DEFAULT 'PENDING', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);");
        w.commit();
    }

    void WaitForDatabase() {
        int retries = 0;
        while (retries < 10) {
            try {
                pqxx::connection c(connection_string_);
                if (c.is_open()) {
                    InitializeSchema(c);
                    return;
                }
            } catch (...) {}
            std::this_thread::sleep_for(std::chrono::seconds(2));
            retries++;
        }
        throw std::runtime_error("DB connection failed");
    }

    long CreateOrder(long user_id, long amount) {
        try {
            pqxx::connection c(connection_string_);
            pqxx::work w(c);
            auto result = w.exec_params(
                "INSERT INTO orders (user_id, amount, status) VALUES ($1, $2, 'NEW') RETURNING id",
                user_id,
                amount);
            long order_id = result[0][0].as<long>();
            std::string payload =
                "{\"order_id\": " + std::to_string(order_id) + ", \"user_id\": " + std::to_string(user_id)
                    + ", \"amount\": " + std::to_string(amount) + "}";
            w.exec_params("INSERT INTO outbox (event_type, payload) VALUES ('OrderCreated', $1)", payload);
            w.commit();
            return order_id;
        } catch (...) { return -1; }
    }

    std::string GetOrderStatus(long order_id) {
        try {
            pqxx::connection c(connection_string_);
            pqxx::work w(c);
            auto row = w.exec_params1("SELECT status FROM orders WHERE id = $1", order_id);
            return row[0].as<std::string>();
        } catch (...) { return "NOT_FOUND"; }
    }

    crow::json::wvalue GetUserOrders(long user_id) {
        crow::json::wvalue orders_json;
        try {
            pqxx::connection c(connection_string_);
            pqxx::work w(c);
            auto rows =
                w.exec_params("SELECT id, amount, status FROM orders WHERE user_id = $1 ORDER BY id DESC", user_id);

            int i = 0;
            for (auto row : rows) {
                orders_json[i]["id"] = row["id"].as<long>();
                orders_json[i]["amount"] = row["amount"].as<long>();
                orders_json[i]["status"] = row["status"].as<std::string>();
                i++;
            }
        } catch (...) {}
        return orders_json;
    }

    void UpdateOrderStatus(long order_id, std::string status) {
        try {
            pqxx::connection c(connection_string_);
            pqxx::work w(c);
            w.exec_params("UPDATE orders SET status = $2 WHERE id = $1", order_id, status);
            w.commit();
            std::cout << "[Order DB] Status updated for " << order_id << " to " << status << std::endl;
        } catch (const std::exception &e) {
            std::cerr << "[Order DB Error] " << e.what() << std::endl;
        }
    }

    void ProcessOutbox() {
        AmqpClient::Channel::ptr_t channel;

        while (true) {
            try {
                if (!channel) {
                    AmqpClient::Channel::OpenOpts opts;
                    opts.host = AMPQ_HOST;
                    opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth("guest", "guest");
                    channel = AmqpClient::Channel::Open(opts);
                    channel->DeclareQueue(QUEUE_NAME, false, true, false, false);
                    std::cout << "[Order Outbox] Connected/Reconnected to RabbitMQ!" << std::endl;
                }

                pqxx::connection c(connection_string_);
                pqxx::work w(c);
                auto rows = w.exec(
                    "SELECT id, payload FROM outbox WHERE status = 'PENDING' ORDER BY id ASC LIMIT 10 FOR UPDATE SKIP LOCKED");

                for (auto row : rows) {
                    long id = row["id"].as<long>();
                    std::string payload = row["payload"].as<std::string>();

                    channel->BasicPublish("", QUEUE_NAME, AmqpClient::BasicMessage::Create(payload));

                    w.exec_params("UPDATE outbox SET status = 'SENT' WHERE id = $1", id);
                    std::cout << "[Order Outbox] Sent event " << id << std::endl;
                }
                w.commit();

                std::this_thread::sleep_for(std::chrono::seconds(2));

            } catch (const std::exception &e) {
                std::cerr << "[Order Outbox Error] " << e.what() << ". Reconnecting..." << std::endl;
                channel.reset();
                std::this_thread::sleep_for(std::chrono::seconds(3));
            }
        }
    }

    void StartResultConsumer() {
        AmqpClient::Channel::ptr_t channel;

        while (true) {
            try {
                if (!channel) {
                    AmqpClient::Channel::OpenOpts opts;
                    opts.host = AMPQ_HOST;
                    opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth("guest", "guest");
                    channel = AmqpClient::Channel::Open(opts);
                    channel->DeclareQueue(RESULTS_QUEUE, false, true, false, false);
                    channel->BasicConsume(RESULTS_QUEUE, "");
                    std::cout << "[Order Consumer] Listening..." << std::endl;
                }

                auto envelope = channel->BasicConsumeMessage();
                std::string payload = envelope->Message()->Body();
                std::cout << "[Order Consumer] Received: " << payload << std::endl;

                long order_id = ExtractJsonValue(payload, "order_id");
                std::string status = ExtractJsonString(payload, "status");

                if (order_id != -1 && !status.empty()) {
                    UpdateOrderStatus(order_id, status);
                }
                channel->BasicAck(envelope);

            } catch (const std::exception &e) {
                std::cerr << "[Order Consumer Error] " << e.what() << ". Reconnecting..." << std::endl;
                channel.reset();
                std::this_thread::sleep_for(std::chrono::seconds(3));
            }
        }
    }
};

int main() {
    setbuf(stdout, nullptr);
    setbuf(stderr, nullptr);

    crow::SimpleApp app;
    OrdersRepository repo(DB_CONN_STR);

    std::cout << "Starting Orders Service..." << std::endl;
    try { repo.WaitForDatabase(); } catch (...) { return 1; }

    std::thread outbox_thread([&repo]() { repo.ProcessOutbox(); });
    outbox_thread.detach();

    std::thread consumer_thread([&repo]() { repo.StartResultConsumer(); });
    consumer_thread.detach();

    CROW_ROUTE(app, "/create_order").methods(crow::HTTPMethod::POST)
        ([&repo](const crow::request& req) {
            char* u = req.url_params.get("user_id");
            char* a = req.url_params.get("amount");

            if (!u || !a) return crow::response(400, "Missing params");

            long user_id = std::atol(u);
            long amount = std::atol(a);

            if (amount <= 0) {
                return crow::response(400, "Amount must be positive");
            }

            long id = repo.CreateOrder(user_id, amount);
            if (id != -1) {
                crow::json::wvalue x; x["order_id"] = id; x["status"] = "NEW";
                return crow::response(201, x);
            }
            return crow::response(500);
        });

    CROW_ROUTE(app, "/order").methods(crow::HTTPMethod::GET)
        ([&repo](const crow::request &req) {
            char *id = req.url_params.get("id");
            if (!id) return crow::response(400);
            std::string s = repo.GetOrderStatus(std::atol(id));
            if (s == "NOT_FOUND") return crow::response(404);
            crow::json::wvalue x;
            x["status"] = s;
            return crow::response(x);
        });

    CROW_ROUTE(app, "/orders").methods(crow::HTTPMethod::GET)
        ([&repo](const crow::request &req) {
            char *u = req.url_params.get("user_id");
            if (!u) return crow::response(400);
            crow::json::wvalue orders = repo.GetUserOrders(std::atol(u));
            crow::json::wvalue x;
            x["orders"] = std::move(orders);
            return crow::response(x);
        });

    app.port(8080).multithreaded().run();
}