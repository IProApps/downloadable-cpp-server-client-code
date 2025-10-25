#include <boost/asio.hpp>
#include <iostream>
#include <fstream>
#include <thread>
#include <atomic>
#include <chrono>
#include <sstream>
#include <vector>
#include <ctime>

using boost::asio::ip::tcp;

enum OpType { INSERT, DELETE, RETAIN };

struct Operation {
    OpType type;
    size_t position;
    std::string content;
    size_t length;
};

std::atomic<bool> running(true);
std::atomic<uint64_t> local_version(0);
std::atomic<std::time_t> local_timestamp(0);
std::mutex file_mtx;

// Compute diff between two strings
std::vector<Operation> compute_diff(const std::string& old_text, const std::string& new_text) {
    std::vector<Operation> ops;
    
    size_t i = 0;
    size_t old_len = old_text.length();
    size_t new_len = new_text.length();
    
    // Find common prefix
    while (i < old_len && i < new_len && old_text[i] == new_text[i]) {
        i++;
    }
    
    if (i > 0) {
        ops.push_back({RETAIN, 0, "", i});
    }
    
    size_t old_end = old_len;
    size_t new_end = new_len;
    
    // Find common suffix
    while (old_end > i && new_end > i && 
           old_text[old_end - 1] == new_text[new_end - 1]) {
        old_end--;
        new_end--;
    }
    
    // Delete middle
    if (old_end > i) {
        ops.push_back({DELETE, i, "", old_end - i});
    }
    
    // Insert middle
    if (new_end > i) {
        ops.push_back({INSERT, i, new_text.substr(i, new_end - i), new_end - i});
    }
    
    // Retain suffix
    if (new_end < new_len) {
        ops.push_back({RETAIN, new_end, "", new_len - new_end});
    }
    
    return ops;
}

// Apply operations to text
std::string apply_operations(const std::string& text, const std::vector<Operation>& ops) {
    std::string result;
    size_t pos = 0;
    
    for (const auto& op : ops) {
        switch (op.type) {
            case RETAIN:
                result += text.substr(pos, op.length);
                pos += op.length;
                break;
            case INSERT:
                result += op.content;
                break;
            case DELETE:
                pos += op.length;
                break;
        }
    }
    
    return result;
}

// Serialize operations
std::string serialize_ops(uint64_t version, std::time_t timestamp, const std::vector<Operation>& ops) {
    std::ostringstream oss;
    oss << version << "|" << timestamp << "|" << ops.size();
    
    for (const auto& op : ops) {
        oss << "|";
        switch (op.type) {
            case RETAIN:
                oss << "R," << op.length;
                break;
            case INSERT:
                oss << "I," << op.position << "," << op.content.length() << "," << op.content;
                break;
            case DELETE:
                oss << "D," << op.position << "," << op.length;
                break;
        }
    }
    
    return oss.str();
}

// Parse operations
struct ParsedMessage {
    uint64_t version;
    std::time_t timestamp;
    std::vector<Operation> ops;
};

ParsedMessage parse_message(const std::string& data) {
    ParsedMessage msg;
    std::istringstream iss(data);
    std::string token;
    
    std::getline(iss, token, '|');
    msg.version = std::stoull(token);
    
    std::getline(iss, token, '|');
    msg.timestamp = std::stoll(token);
    
    std::getline(iss, token, '|');
    size_t op_count = std::stoull(token);
    
    for (size_t i = 0; i < op_count; i++) {
        std::getline(iss, token, '|');
        std::istringstream op_stream(token);
        
        char type;
        op_stream >> type;
        op_stream.ignore(1);
        
        Operation op;
        if (type == 'R') {
            op.type = RETAIN;
            op_stream >> op.length;
        } else if (type == 'I') {
            op.type = INSERT;
            op_stream >> op.position;
            op_stream.ignore(1);
            size_t content_len;
            op_stream >> content_len;
            op_stream.ignore(1);
            std::string remaining;
            std::getline(op_stream, remaining);
            op.content = remaining;
            op.length = content_len;
        } else if (type == 'D') {
            op.type = DELETE;
            op_stream >> op.position;
            op_stream.ignore(1);
            op_stream >> op.length;
        }
        
        msg.ops.push_back(op);
    }
    
    return msg;
}

void write_file_safe(const std::string& filename, const std::string& content) {
    std::lock_guard<std::mutex> lock(file_mtx);
    std::ofstream file(filename, std::ios::trunc);
    file << content;
    file.close();
}

std::string read_file_safe(const std::string& filename) {
    std::lock_guard<std::mutex> lock(file_mtx);
    std::ifstream file(filename);
    if (!file.is_open()) return "";
    return std::string((std::istreambuf_iterator<char>(file)),
                       std::istreambuf_iterator<char>());
}

void listen_for_updates(tcp::socket& socket, const std::string& filename) {
    boost::asio::streambuf buf;
    std::string current_text = read_file_safe(filename);
    
    while (running) {
        boost::system::error_code ec;
        boost::asio::read_until(socket, buf, "\n", ec);
        if (ec) {
            std::cerr << "[Error] Connection lost: " << ec.message() << "\n";
            running = false;
            break;
        }
        
        std::istream is(&buf);
        std::string message;
        std::getline(is, message);
        
        ParsedMessage server_msg = parse_message(message);
        
        // Apply operations to current text
        std::string new_text = apply_operations(current_text, server_msg.ops);
        
        // Update file
        write_file_safe(filename, new_text);
        current_text = new_text;
        
        // Update version tracking
        local_version = server_msg.version;
        local_timestamp = server_msg.timestamp;
        
        std::cout << "[Update] Applied " << server_msg.ops.size() 
                  << " ops from server (v" << server_msg.version << ")\n";
        
        // Print operation details
        for (const auto& op : server_msg.ops) {
            switch (op.type) {
                case RETAIN:
                    std::cout << "  RETAIN " << op.length << " chars\n";
                    break;
                case INSERT:
                    std::cout << "  INSERT at " << op.position << ": \"" 
                              << op.content.substr(0, std::min(size_t(20), op.content.length())) 
                              << (op.content.length() > 20 ? "..." : "") << "\"\n";
                    break;
                case DELETE:
                    std::cout << "  DELETE at " << op.position << ": " << op.length << " chars\n";
                    break;
            }
        }
    }
}

void watch_file(tcp::socket& socket, const std::string& filename) {
    std::string last_content = read_file_safe(filename);
    
    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        
        std::string current_content = read_file_safe(filename);
        
        if (current_content != last_content) {
            // Compute diff operations
            std::vector<Operation> ops = compute_diff(last_content, current_content);
            
            uint64_t version = local_version.load();
            std::time_t timestamp = std::time(nullptr);
            
            std::string msg = serialize_ops(version, timestamp, ops);
            
            try {
                boost::asio::write(socket, boost::asio::buffer(msg + "\n"));
                last_content = current_content;
                local_timestamp = timestamp;
                
                std::cout << "[Sent] " << ops.size() << " ops to server (based on v" 
                          << version << ")\n";
                
                // Print operation details
                for (const auto& op : ops) {
                    switch (op.type) {
                        case RETAIN:
                            std::cout << "  RETAIN " << op.length << " chars\n";
                            break;
                        case INSERT:
                            std::cout << "  INSERT at " << op.position << ": \"" 
                                      << op.content.substr(0, std::min(size_t(20), op.content.length())) 
                                      << (op.content.length() > 20 ? "..." : "") << "\"\n";
                            break;
                        case DELETE:
                            std::cout << "  DELETE at " << op.position << ": " << op.length << " chars\n";
                            break;
                    }
                }
            } catch (const std::exception& e) {
                std::cerr << "[Error] Failed to send: " << e.what() << "\n";
                running = false;
                break;
            }
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::cerr << "Usage: ./sync_client <server_ip> <filename>\n";
        return 1;
    }
    
    std::string server_ip = argv[1];
    std::string filename = argv[2];
    
    // Create file if it doesn't exist
    std::ofstream init_file(filename, std::ios::app);
    init_file.close();
    
    boost::asio::io_context io;
    tcp::socket socket(io);
    
    try {
        socket.connect({boost::asio::ip::make_address(server_ip), 8080});
        std::cout << "Connected to diff-based sync server\n";
        std::cout << "File: " << filename << "\n";
        std::cout << "Conflict resolution: Last-write-wins\n";
        std::cout << "Syncing mode: Diff-based (only changes sent)\n\n";
    } catch (const std::exception& e) {
        std::cerr << "Connection failed: " << e.what() << "\n";
        return 1;
    }
    
    std::thread listener(listen_for_updates, std::ref(socket), filename);
    std::thread watcher(watch_file, std::ref(socket), filename);
    
    listener.join();
    watcher.join();
    
    return 0;
}
