/*
 * Copyright (C) 2024: Arizona Board of Regents on Behalf of the University of Arizona
 */

#pragma once
#include <ASDP_Core_API.h>
#include <map>
#include <mutex>
#include <list>

namespace asdp {

class Storage_Module;

/// @brief Per-serial-number server module that advertises on a specific port.
class Storage_Module_Server : public CoreServerBase {
public:

  /// @brief Constructor
  /// @param parent The parent object that created this server. Note: This is an unprotected pointer.
  /// We must be careful to ensure that the parent object outlives this object. This is being done by
  /// waiting for all threads to stop before deleting the parent object.
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
  void doStartRecording(const CommandPacketStartRecording& command, ClientState& client) override;
  void doStopRecording(const CommandPacketStopRecording& command, ClientState& client) override;
  void doSetStartUpRecordingState(const CommandPacketSetStartUpRecordingState& command, ClientState& client) override;
  void doListStoredStreams(const CommandPacketListStoredStreams& command, ClientState& client) override;
  void doEraseStoredStream(const CommandPacketEraseStoredStream& command, ClientState& client);
  void doEraseAllStoredStreams(const CommandPacketEraseAllStoredStreams& command, ClientState& client);

  friend class Storage_Module;
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
  /// A negative verbosity will cause the server not to report error messages to the console.
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

  //=============================================================================
  // Persistent state that is stored to disk and loaded from disk.

  class PersistentState {
  public:
    /// @brief Constructor
    /// @param fileName The name of a file to read the state from.  If the file does not
    /// exist, the state will be initialized to default values and then written to the file.
    /// If the file cannot be created, the state will still be initialized to default values.
    PersistentState(std::string const &fileName);

    /// @brief Load the state from disk.
    /// @return Status indicating success or failure.
    bool LoadFromFile();

    /// @brief Save the state to disk.
    /// @return Status indicating success or failure.
    bool SaveToFile() const;

    /// @brief Find out whether we are storing to disk at restart.
    bool StoringAtRestart() const { return m_storingAtRestart; }

    /// @brief Set whether we are storing to disk at restart.
    void SetStoringAtRestart(bool storingAtRestart) { m_storingAtRestart = storingAtRestart; }

    /// @brief Get the number of bytes in a disk block.
    uint32_t DiskBlockSize() const { return m_diskBlockSize; }

    /// @brief Set the number of bytes in a disk block.
    /// @param diskBlockSize The number of bytes in a disk block. Ensures that we write to disk in multiples of this size.
    void SetDiskBlockSize(uint32_t diskBlockSize) { m_diskBlockSize = diskBlockSize; }

    /// @brief Get the total buffer size for each UDP ingest stream.
    uint32_t TotalBufferSize() const { return m_totalBufferSize; }

    /// @brief Set the total buffer size for each UDP ingest stream.
    /// @param totalBufferSize The total buffer size for each UDP ingest stream.
    void SetTotalBufferSize(uint32_t totalBufferSize) { m_totalBufferSize = totalBufferSize; }

    /// @brief Get the high-water mark for each UDP ingest stream where it writes to disk if it reaches this size.
    uint32_t HighWaterMark() const { return m_highWaterMark; }

    /// @brief Set the high-water mark for each UDP ingest stream where it writes to disk if it reaches this size.
    /// @param highWaterMark The high-water mark for each UDP ingest stream where it writes to disk if it reaches this size.
    void SetHighWaterMark(uint32_t highWaterMark) { m_highWaterMark = highWaterMark; }

  protected:
    /// @brief Default constructor
    PersistentState() : m_storingAtRestart(false), m_diskBlockSize(1024)
      , m_totalBufferSize(512*1024), m_highWaterMark(512*1024 - 9000) {};

    /// File that we are associated with.
    std::string m_fileName;

    /// Are we storing to disk at restart?
    bool m_storingAtRestart;

    /// Number of bytes in a disk block.
    uint32_t m_diskBlockSize;

    /// Total buffer size for each UDP ingest stream
    uint32_t m_totalBufferSize;

    /// High-water mark for each UDP ingest stream where it writes to disk if it reaches this size.
    uint32_t m_highWaterMark;
  };
  PersistentState m_persistentState;

  //=============================================================================
  // Storage management.

  /// The root directory for storage.
  std::string m_storageRoot;

  /// The number of cameras on the server we last connected to.
  size_t m_numCameras;

  std::mutex m_storageMutex;
  std::vector< std::shared_ptr<SenderFile> > m_storageSenders;
  uint16_t m_writingToID;   ///< The ID of the session we are writing, 0 for none

  /// @brief Start storing to disk.
  /// @return Status indicating success or failure.
  Status StartStoring();

  /// @brief Stop storing to disk.
  /// @return Status indicating success or failure.
  Status StopStoring();

  //=============================================================================
  // Helper functions

  /// @brief Wait for a message of the specified type to arrive.
  /// @param type The type of message to wait for.
  /// @param seconds The number of seconds to wait for the message.
  /// @return The message that arrived, or nullptr if none arrived.
  std::shared_ptr<Message> WaitForMessageType(MessageID type, float seconds);

  //=============================================================================
  // Thread management.

  /// @todo Consider the need for mutexes to protect the data structures when we
  /// view or modify them in the m_servers or in the StreamReceiverThreads.

  /// Set to true to stop all threads.
  std::atomic<bool> m_stop;

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
    ReceiverInfo(std::shared_ptr<ReceiverUDP> Receiver, uint32_t ID) :
      m_receiver(Receiver), m_ID(ID) { }

    std::shared_ptr<ReceiverUDP> m_receiver;  ///< The receiver.
    uint32_t m_ID;  ///< The ID of the receiver.
  };

  std::vector<std::thread> m_receiver_threads;

  /// @brief Body of a thread that handles a single receiver.
  /// 
  /// This reads Messages from the receiver. If we are storing, it
  /// stores to the appropriate file. If we are streaming, it forwards
  /// the message to the appropriate m_server.
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
