#include "crow.h"
#include <pqxx/pqxx>
#include <SimpleAmqpClient/SimpleAmqpClient.h>
#include <iostream>
#include <string>
#include <thread>
#include <chrono>
#include <vector>

const std::string DB_CONN_STR = "postgresql://user:password@payments-db:5432/payments_db";
const std::string AMPQ_HOST = "rabbitmq";
const std::string ORDER_QUEUE = "orders_queue";
const std::string RESULTS_QUEUE = "results_queue";

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

class PaymentsRepository {
    std::string connection_string_;

 public:
    PaymentsRepository(std::string conn_str) : connection_string_(std::move(conn_str)) {}

    void InitializeSchema(pqxx::connection &c) {
        pqxx::work w(c);
        w.exec0("CREATE TABLE IF NOT EXISTS accounts (user_id BIGINT PRIMARY KEY, balance BIGINT NOT NULL DEFAULT 0);");
        w.exec0(
            "CREATE TABLE IF NOT EXISTS processed_orders (order_id BIGINT PRIMARY KEY, status VARCHAR(20), created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);");
        w.exec0(
            "CREATE TABLE IF NOT EXISTS outbox (id SERIAL PRIMARY KEY, event_type VARCHAR(50) NOT NULL, payload JSONB NOT NULL, status VARCHAR(20) NOT NULL DEFAULT 'PENDING', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);");
        w.commit();
        std::cout << "Payments DB schema initialized." << std::endl;
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

    bool CreateAccount(long user_id) {
        try {
            pqxx::connection c(connection_string_);
            pqxx::work w(c);
            w.exec_params("INSERT INTO accounts (user_id, balance) VALUES ($1, 0) ON CONFLICT (user_id) DO NOTHING",
                          user_id);
            w.commit();
            return true;
        } catch (...) { return false; }
    }
    bool AddFunds(long user_id, long amount) {
        try {
            pqxx::connection c(connection_string_);
            pqxx::work w(c);
            auto r = w.exec_params("UPDATE accounts SET balance = balance + $2 WHERE user_id = $1", user_id, amount);
            w.commit();
            return r.affected_rows() > 0;
        } catch (...) { return false; }
    }
    long GetBalance(long user_id) {
        try {
            pqxx::connection c(connection_string_);
            pqxx::work w(c);
            auto r = w.exec_params1("SELECT balance FROM accounts WHERE user_id = $1", user_id);
            return r[0].as<long>();
        } catch (...) { return -1; }
    }

    void ProcessPayment(long order_id, long user_id, long amount) {
        try {
            if (amount <= 0) {
                std::cerr << "[Payment Security] Attempt to process negative amount for Order " << order_id << std::endl;
                return;
            }

            pqxx::connection c(connection_string_);
            pqxx::work w(c);

            auto check = w.exec_params("SELECT status FROM processed_orders WHERE order_id = $1", order_id);
            if (!check.empty()) {
                std::cout << "[Payment] Order " << order_id << " duplicate. Skipping." << std::endl;
                return;
            }

            auto acc = w.exec_params("SELECT balance FROM accounts WHERE user_id = $1 FOR UPDATE", user_id);
            std::string status = "FAILED";

            if (!acc.empty()) {
                long balance = acc[0][0].as<long>();
                if (balance >= amount) {
                    w.exec_params("UPDATE accounts SET balance = balance - $2 WHERE user_id = $1", user_id, amount);
                    status = "FINISHED";
                    std::cout << "[Payment] Order " << order_id << " PAID." << std::endl;
                } else {
                    status = "CANCELLED";
                    std::cout << "[Payment] Order " << order_id << " NO FUNDS." << std::endl;
                }
            } else {
                status = "CANCELLED";
                std::cout << "[Payment] Order " << order_id << " NO USER." << std::endl;
            }

            w.exec_params("INSERT INTO processed_orders (order_id, status) VALUES ($1, $2)", order_id, status);
            std::string
                response_payload = "{\"order_id\": " + std::to_string(order_id) + ", \"status\": \"" + status + "\"}";
            w.exec_params("INSERT INTO outbox (event_type, payload) VALUES ('PaymentProcessed', $1)", response_payload);
            w.commit();
        } catch (const std::exception &e) {
            std::cerr << "[Payment Error] " << e.what() << std::endl;
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
                    channel->DeclareQueue(RESULTS_QUEUE, false, true, false, false);
                    std::cout << "[Payment Outbox] Connected/Reconnected!" << std::endl;
                }

                pqxx::connection c(connection_string_);
                pqxx::work w(c);
                auto rows = w.exec(
                    "SELECT id, payload FROM outbox WHERE status = 'PENDING' ORDER BY id ASC LIMIT 10 FOR UPDATE SKIP LOCKED");

                for (auto row : rows) {
                    long id = row["id"].as<long>();
                    std::string payload = row["payload"].as<std::string>();

                    channel->BasicPublish("", RESULTS_QUEUE, AmqpClient::BasicMessage::Create(payload));
                    w.exec_params("UPDATE outbox SET status = 'SENT' WHERE id = $1", id);
                    std::cout << "[Payment Outbox] Sent result for event " << id << std::endl;
                }
                w.commit();

                std::this_thread::sleep_for(std::chrono::seconds(2));

            } catch (const std::exception &e) {
                std::cerr << "[Payment Outbox Error] " << e.what() << ". Reconnecting..." << std::endl;
                channel.reset();
                std::this_thread::sleep_for(std::chrono::seconds(3));
            }
        }
    }

    void StartConsumer() {
        AmqpClient::Channel::ptr_t channel;

        while (true) {
            try {
                if (!channel) {
                    AmqpClient::Channel::OpenOpts opts;
                    opts.host = AMPQ_HOST;
                    opts.auth = AmqpClient::Channel::OpenOpts::BasicAuth("guest", "guest");
                    channel = AmqpClient::Channel::Open(opts);
                    channel->DeclareQueue(ORDER_QUEUE, false, true, false, false);
                    channel->BasicConsume(ORDER_QUEUE, "");
                    std::cout << "[Payment Consumer] Listening..." << std::endl;
                }

                auto envelope = channel->BasicConsumeMessage();
                std::string payload = envelope->Message()->Body();
                std::cout << "[Payment Consumer] Received: " << payload << std::endl;

                long order_id = ExtractJsonValue(payload, "order_id");
                long user_id = ExtractJsonValue(payload, "user_id");
                long amount = ExtractJsonValue(payload, "amount");

                if (order_id != -1 && user_id != -1) {
                    ProcessPayment(order_id, user_id, amount);
                }
                channel->BasicAck(envelope);

            } catch (const std::exception &e) {
                std::cerr << "[Payment Consumer Error] " << e.what() << ". Reconnecting..." << std::endl;
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
    PaymentsRepository repo(DB_CONN_STR);

    std::cout << "Starting Payments Service..." << std::endl;
    try { repo.WaitForDatabase(); } catch (...) { return 1; }

    std::thread consumer_thread([&repo]() { repo.StartConsumer(); });
    consumer_thread.detach();

    std::thread outbox_thread([&repo]() { repo.ProcessOutbox(); });
    outbox_thread.detach();

    CROW_ROUTE(app, "/create_account").methods(crow::HTTPMethod::POST)
        ([&repo](const crow::request &req) {
            char *uid = req.url_params.get("user_id");
            if (uid && repo.CreateAccount(std::atol(uid))) return crow::response(201);
            return crow::response(400);
        });

    CROW_ROUTE(app, "/add_funds").methods(crow::HTTPMethod::POST)
        ([&repo](const crow::request& req) {
            char* uid = req.url_params.get("user_id");
            char* amt = req.url_params.get("amount");

            if (!uid || !amt) return crow::response(400);

            long amount_val = std::atol(amt);
            if (amount_val <= 0) return crow::response(400, "Amount must be positive");

            if(repo.AddFunds(std::atol(uid), amount_val)) return crow::response(200);
            return crow::response(400);
        });

    CROW_ROUTE(app, "/balance").methods(crow::HTTPMethod::GET)
        ([&repo](const crow::request &req) {
            char *uid = req.url_params.get("user_id");
            if (!uid) return crow::response(400);
            long b = repo.GetBalance(std::atol(uid));
            if (b == -1) return crow::response(404);
            crow::json::wvalue x;
            x["balance"] = b;
            return crow::response(x);
        });

    app.port(8080).multithreaded().run();
}