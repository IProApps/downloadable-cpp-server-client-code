#include <boost/asio.hpp>
#include <iostream>
#include <thread>
#include <mutex>
#include <set>
#include <memory>
#include <string>
#include <sstream>
#include <ctime>

using boost::asio::ip::tcp;

struct Document {
    std::string text;
    uint64_t version;
    std::time_t timestamp;
};

// Global shared state
std::set<std::shared_ptr<tcp::socket>> clients;
Document current_doc = {"", 0, std::time(nullptr)};
std::mutex mtx;

// Serialize document into a single string with '|' separators and a null terminator
std::string serialize_doc(const Document& doc) {
    std::ostringstream oss;
    oss << doc.version << "|" << doc.timestamp << "|" << doc.text;
    return oss.str();
}

// Parse serialized document back into struct
Document parse_doc(const std::string& data) {
    Document doc;
    size_t first_pipe = data.find('|');
    size_t second_pipe = data.find('|', first_pipe + 1);

    doc.version = std::stoull(data.substr(0, first_pipe));
    doc.timestamp = std::stoll(data.substr(first_pipe + 1, second_pipe - first_pipe - 1));
    doc.text = data.substr(second_pipe + 1);
    return doc;
}

// Send message to all connected clients except the sender
void broadcast_myfunc(const std::string& message, std::shared_ptr<tcp::socket> sender) {
     std::cout << "starting broadcast\n";
     
     int iterations = 1;
     for (auto& client : clients) {
            iterations++;
            std::cout << iterations + "\n";
        if (client != sender || 1) {
            boost::system::error_code ec;
            boost::asio::write(*client, boost::asio::buffer(message + '\0'), ec);
        }
    }
     
    
}

// Handle one connected client
void handle_client(std::shared_ptr<tcp::socket> socket) {
    {
        std::lock_guard<std::mutex> lock(mtx);
        clients.insert(socket);
    }

    // Send current document to new client
    {
        std::lock_guard<std::mutex> lock(mtx);
        std::string init_msg = serialize_doc(current_doc);
        boost::asio::write(*socket, boost::asio::buffer(init_msg + '\0'));
        std::cout << "[Server] Sent initial doc (v" << current_doc.version << ") to new client\n";
    }

    try {
        for (;;) {
            boost::asio::streambuf buf;
            std::cout << "waiting for message";
            boost::asio::read_until(*socket, buf, '\0');  // use null terminator

            std::cout << "\nwe found a freaking message yo\n";
            std::istream is(&buf);
            std::string message;
            std::getline(is, message, '\0');  // read until null char

            Document incoming_doc = parse_doc(message);
                std::cout << "doc = parsed\n";
            std::cout << "[Message] v" << incoming_doc.version << ": " << incoming_doc.text << "\n";

            std::lock_guard<std::mutex> lock(mtx);

            // Conflict resolution
            if (incoming_doc.version != current_doc.version) {
                std::cout << "[Conflict] Client v" << incoming_doc.version
                          << " vs Server v" << current_doc.version << "\n";

                if (incoming_doc.timestamp > current_doc.timestamp) {
                    std::cout << "[Resolution] Accepted newer client update\n";
                    current_doc.text = incoming_doc.text;
                    current_doc.version = incoming_doc.version;
                    current_doc.timestamp = std::time(nullptr);
                } else {
                    std::cout << "[Resolution] Rejected older client update\n";
                    std::string reject_msg = serialize_doc(current_doc);
                    boost::asio::write(*socket, boost::asio::buffer(reject_msg + '\0'));
                    continue;
                }
            } else {
                // No conflict
                std::cout << "no conflict detected";
                current_doc.text = incoming_doc.text;
                current_doc.version = incoming_doc.version;
                current_doc.timestamp = std::time(nullptr);
                std::cout << "[Accept] Updated to version " << current_doc.version << "\n";
            }

            // Notify others
            std::string broadcast_msg = serialize_doc(current_doc);
            std::cout << "doc = serialized \n";
            std::cout << "Broadcase msg: " << broadcast_msg << '\n';
            std::cout << "here\n";
            std::cout << "PTR VALUE: " << static_cast<const void*>(&(*socket)) << std::endl;
            broadcast_myfunc(broadcast_msg, socket);
            std::cout << "message = broadcasted"; 
        }
    } catch (std::exception& e) {
        std::cout << "error ! error !";
        std::lock_guard<std::mutex> lock(mtx);
        std::cout << "Error! Trying to disconnect (or something)";
        clients.erase(socket);
        std::cout << "[Disconnect] " << e.what() << "\n";
    }
}

int main() {
    try {
        boost::asio::io_context io;
        tcp::acceptor acceptor(io, tcp::endpoint(tcp::v4(), 8080));
        std::cout << "Conflict-aware server running on port 8080...\n";

        for (;;) {
                std::shared_ptr<tcp::socket> socket = std::make_shared<tcp::socket>(io);
            acceptor.accept(*socket);
            std::cout << "[Connect] New client connected\n";
            std::thread(handle_client, socket).detach();
        }
    } catch (std::exception& e) {
        std::cerr << "Server error: " << e.what() << "\n";
    }
}

