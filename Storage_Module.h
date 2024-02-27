/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#pragma once
#include <asdp_api.h>
#include <map>
#include <mutex>
#include <list>

namespace asdp {

class Storage_Module;

/// @brief Per-serial-number server module that advertises on a specific port.
class Storage_Module_Server : public CoreServerBase {
public:

  /// @brief Constructor
  /// @param parent The parent object that created this server. @todo WARNING: This is an unprotected pointer.
  /// We must be careful to ensure that the parent object outlives this object.
  /// @param serialNumber The serial number of the server.
  /// @param NicName The name of the network interface to listen on for incoming connections.
  /// @param sendPort The port to send outgoing connections on.
  /// @param listenPort The port to listen on for incoming connections.
  /// @param maxPayloadSize The maximum size of a payload that can be sent or received.
  /// @param verbosity The verbosity level of the server, 0 for no verbosity, higher for more verbosity.
  Storage_Module_Server(Storage_Module *parent, uint32_t serialNumber, const std::string &NicName,
    uint16_t sendPort, uint16_t listenPort, uint32_t maxPayloadSize, int verbosity);

protected:

  /// The parent object that created this server.
  Storage_Module *m_parent;

  /// @brief Handle operations that must be done every loop, like checking if our thread should stop.
  void doEveryLoop() override;

  //=============================================================================
  // Override methods to implement the commands as needed.

  /// @brief Implement the specified command.
  /// @param command The command packet to implement.
  /// @param client The client that the command is coming from.
  void doReset(const CommandPacketReset&, ClientState& client) override;

  /// @brief Implement the specified command.
  /// @param command The command packet to implement.
  /// @param client The client that the command is coming from.
  void doConfigureTrigger(const CommandPacketConfigureTrigger&, ClientState& client) override;

  /// @brief Implement the specified command.
  /// @param command The command packet to implement.
  /// @param client The client that the command is coming from.
  void doSoftwareTrigger(const CommandPacketSoftwareTrigger&, ClientState& client) override;

  /// @brief Implement the specified command.
  /// @param command The command packet to implement.
  /// @param client The client that the command is coming from.
  void doStreamSubregion(const CommandPacketStreamSubregion&, ClientState& client) override;

  /// @brief Implement the specified command.
  /// @param command The command packet to implement.
  /// @param client The client that the command is coming from.
  void doCancelSubregion(const CommandPacketCancelSubregion&, ClientState& client) override;

  /// @todo Override the other methods as needed.
};

/// @brief Storage module that acts as an intermediary between Core Modules and other clients.
class Storage_Module : public CoreClient {
public:
  /// @brief Constructor
  /// 
  /// This constructor will create a server for each serial number found in the storage root directory,
  /// along with a thread to run each server.  It also creates a client to connect to the Core Module,
  /// also in its own thread.  It then accepts any connections on the servers and forwards them to the
  /// Core Module, and accepts any responses from the Core Module and forwards them to the clients if
  /// they are using the same serial number.  It updates the CurrentStatus() to indicate whether the
  /// object is doing okay.
  /// @param NicNameIn The name of the network interface to listen on for incoming connections.
  /// @param NicNameOut The name of the network interface to send outgoing connections on.
  /// @param StorageRoot The root directory for storage.
  /// @param verbosity The verbosity level of the server, 0 for no verbosity, higher for more verbosity.
  Storage_Module(const std::string &NicNameIn, const std::string &NicNameOut,
    const std::string StorageRoot, int verbosity = 0);

  /// @brief Destructor
  ///
  /// Tears down all threads, closing all connections.
  ~Storage_Module();

  /// @brief Get the current status of the object.
  Status GetCurrentStatus() const;

  /// @brief Test the class.
  /// @return Empty string on success, message describing the problem on failure.
  static std::string Test();

protected:

  /// The current status of the object.
  Status m_status;

  /// The verbosity level of the server, 0 for no verbosity, higher for more verbosity.
  int m_verbosity;

  /// The name of the network interface to listen on for incoming connections.
  std::string m_nicNameIn;

  /// The name of the network interface to listen on for outgoing connections.
  std::string m_nicNameOut;

  /// The root directory for storage.
  std::string m_storageRoot;

  //=============================================================================
  // Helper functions

  /// @brief Wait for a message of the specified type to arrive.
  /// @param type The type of message to wait for.
  /// @param seconds The number of seconds to wait for the message.
  /// @return The message that arrived, or nullptr if none arrived.
  std::shared_ptr<Message> WaitForMessageType(MessageID type, float seconds);

  //=============================================================================
  // Thread management.

  /// Set to true to stop all threads.
  std::atomic<bool> m_stop;

  /// Mutex used by threads to avoid race conditions.
  /// @todo Consider whether the things done on the client-side only need to be guarded.
  std::recursive_mutex m_mutex;

  /// @brief Information about a single server.
  struct ServerInfo {
    /// @brief Constructor
    ServerInfo(uint32_t SerialNumber, std::shared_ptr<Storage_Module_Server> Server) :
      m_serialNumber(SerialNumber), m_server(Server) { }

    uint32_t m_serialNumber;  ///< The serial number of the server.
    std::shared_ptr<Storage_Module_Server> m_server;  ///< The server.
  };

  /// One server per serial number found in the storage root directory.
  /// When we connect a client, we will also ensure that the server for that serial number is running.
  std::vector< std::shared_ptr<ServerInfo> > m_servers;

  /// Server threads
  std::vector< std::thread > m_server_threads;

  /// @brief Body of a thread that handles a single server.
  /// @param server The server to handle.
  void ServerThread(std::shared_ptr<ServerInfo> server);

  /// Thread for the client to the Core Module.
  std::thread m_client_thread;

  /// @brief Body of the thread that handles the client connected to a Core Module.
  void ClientThread();

  /// Receivers for the UDP streams from each camera on the connected server.
  std::vector< std::shared_ptr<ReceiverUDP> > m_receivers;

  /// @brief Information about a single receiver.
  struct ReceiverInfo {
    /// @brief Constructor
    ReceiverInfo(std::shared_ptr<ReceiverUDP> Receiver) :
      m_receiver(Receiver) { }

    std::shared_ptr<ReceiverUDP> m_receiver;
  };

  std::vector<std::thread> m_receiver_threads;

  /// @brief Body of a thread that handles a single receiver.
  /// @param receiver The receiver to handle.
  void StreamReceiverThread(std::shared_ptr<ReceiverInfo> receiver);

  /// The server associated with the current client.
  std::shared_ptr<Storage_Module_Server> m_server;

  /// @brief Construct a new server thread based on the one we are connected to.
  /// 
  /// This not only makes a server object, but also starts a thread to run it.
  /// Before doing so, it creates a new directory under the storage root directory for the serial number
  /// and initializes the information in that directory.
  /// @param [out] server The server that was constructed as part of this.
  /// @return Status indicating success or failure.
  Status ConstructNewServer(std::shared_ptr<Storage_Module_Server> &server);

  /// @brief Configure the client connection based on the response from the Core Module.
  /// 
  /// This will request streaming of all optional streams that are supported by the server.
  /// It will also open a UDP receiver for each camera, at full size and rate.
  /// @param response The response from the Core Module.
  /// @return Status indicating success or failure.
  Status ConfigureClientConnection(const MessageState &response);

  /// Used to determine the port to listen on so that each is different.  Decrement to get the next port.
  std::atomic<uint16_t> m_nextPort;

  friend class Storage_Module_Server;
};

} // namespace asdp
